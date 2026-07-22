//go:build integration

// This file is excluded from a normal `go test` run by the build tag above, and
// included only via `go test -tags=integration` (see `make test-integration`).
//
// Testcontainers means these tests need no running compose stack and work in CI
// — but they still start real Kafka containers, costing tens of seconds each.
// The tag keeps `make check` fast enough to run constantly while writing code.
package kafkax_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/ipko1996/huweathersim/pkg/events"
	"github.com/ipko1996/huweathersim/pkg/kafkax"
)

// kafkaImage is deliberately a Confluent image, not the apache/kafka:3.9.1 used
// by docker-compose.
//
// The testcontainers Kafka module copies a startup script into Confluent-specific
// paths (/etc/confluent/docker/launch) and sets Confluent env conventions, so it
// simply does not work with the Apache image. The Kafka wire protocol is
// identical either way, so kafka-go cannot tell the difference — but the version
// skew is worth knowing about if a test ever behaves differently from compose.
const kafkaImage = "confluentinc/confluent-local:7.5.0"

// startKafka boots a throwaway broker for one test and returns its addresses.
//
// This is why testcontainers was chosen over pointing tests at the compose
// stack: the test brings its own Kafka, so it passes on a laptop with nothing
// running and in GitHub Actions (Phase 8) with no extra setup. The cost is
// ~10-20s of container startup, which is why these live behind `make
// test-integration` rather than in the fast `make test` loop.
func startKafka(t *testing.T) []string {
	t.Helper()

	ctx := context.Background()

	ctr, err := tckafka.Run(ctx, kafkaImage)
	// CleanupContainer registers teardown even if Run failed partway, so a
	// broken test can't leak a container. It must come before the error check.
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}

	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		t.Fatalf("get broker addresses: %v", err)
	}
	return brokers
}

func testReading(id string, temp float64) events.SensorReading {
	return events.SensorReading{
		SensorID: id,
		Lat:      47.4979,
		Lon:      19.0402,
		TempC:    temp,
		// Truncated to milliseconds: JSON round-trips RFC3339 nanoseconds fine,
		// but trimming avoids any doubt when comparing times below.
		Time: time.Now().UTC().Truncate(time.Millisecond),
	}
}

// TestProduceConsumeRoundTrip is the Phase 1 acceptance test: a reading survives
// the trip producer -> Kafka -> consumer with every field intact.
func TestProduceConsumeRoundTrip(t *testing.T) {
	brokers := startKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const topic = "test.readings.roundtrip"
	if err := kafkax.EnsureTopic(ctx, brokers, topic, kafkax.ReadingsPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	// --- produce -----------------------------------------------------------
	producer := kafkax.NewProducer(brokers, topic)
	defer producer.Close()

	const wantCount = 5
	sent := make([]events.SensorReading, 0, wantCount)
	for i := range wantCount {
		r := testReading("sensor-0001", 20.0+float64(i))
		if err := producer.Publish(ctx, r); err != nil {
			t.Fatalf("publish reading %d: %v", i, err)
		}
		sent = append(sent, r)
	}

	// --- consume -----------------------------------------------------------
	consumer := kafkax.NewConsumer(brokers, topic, "test-group-roundtrip")
	defer consumer.Close()

	// The handler feeds a channel so the test can stop the consumer as soon as
	// it has everything, instead of waiting for a fixed timeout.
	received := make(chan events.SensorReading, wantCount)
	runCtx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(runCtx, func(_ context.Context, r events.SensorReading) error {
			received <- r
			return nil
		})
	}()

	got := make([]events.SensorReading, 0, wantCount)
	for len(got) < wantCount {
		select {
		case r := <-received:
			got = append(got, r)
		case err := <-done:
			t.Fatalf("consumer stopped early after %d readings: %v", len(got), err)
		case <-ctx.Done():
			t.Fatalf("timed out with %d/%d readings", len(got), wantCount)
		}
	}

	stopConsumer()
	if err := <-done; err != nil {
		t.Errorf("consumer returned error on shutdown: %v", err)
	}

	// --- assert ------------------------------------------------------------
	// All 5 readings share a sensor ID, so they share a partition key and arrive
	// in the order they were sent. Across partitions that would not hold.
	if len(got) != wantCount {
		t.Fatalf("received %d readings, want %d", len(got), wantCount)
	}
	for i := range sent {
		if got[i].SensorID != sent[i].SensorID {
			t.Errorf("reading %d: sensor id got %q, want %q", i, got[i].SensorID, sent[i].SensorID)
		}
		if got[i].TempC != sent[i].TempC {
			t.Errorf("reading %d: temp got %.1f, want %.1f", i, got[i].TempC, sent[i].TempC)
		}
		if !got[i].Time.Equal(sent[i].Time) {
			t.Errorf("reading %d: time got %v, want %v", i, got[i].Time, sent[i].Time)
		}
	}
}

// TestEnsureTopicPartitionsAndIdempotency covers the reason EnsureTopic exists:
// the topic must have 6 partitions (auto-creation would give it 1 and silently
// cap Phase 6 autoscaling), and calling it repeatedly must be harmless because
// every service calls it at startup.
func TestEnsureTopicPartitionsAndIdempotency(t *testing.T) {
	brokers := startKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const topic = "test.readings.partitions"

	// Calling twice must succeed both times.
	for i := range 2 {
		if err := kafkax.EnsureTopic(ctx, brokers, topic, kafkax.ReadingsPartitions); err != nil {
			t.Fatalf("EnsureTopic call %d: %v", i+1, err)
		}
	}

	client := &kafkago.Client{Addr: kafkago.TCP(brokers...), Timeout: 10 * time.Second}
	resp, err := client.Metadata(ctx, &kafkago.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		t.Fatalf("fetch metadata: %v", err)
	}

	var found bool
	for _, tp := range resp.Topics {
		if tp.Name != topic {
			continue
		}
		found = true
		if len(tp.Partitions) != kafkax.ReadingsPartitions {
			t.Errorf("partitions: got %d, want %d — a 1-partition topic would cap consumer parallelism at one pod",
				len(tp.Partitions), kafkax.ReadingsPartitions)
		}
	}
	if !found {
		t.Fatalf("topic %s not present in metadata", topic)
	}
}

// TestConsumerSkipsPoisonMessages proves a malformed message cannot wedge the
// pipeline. Bad JSON will never become good, so retrying it forever would block
// its partition permanently — the classic "poison pill". The consumer must log
// it, commit past it, and keep going.
func TestConsumerSkipsPoisonMessages(t *testing.T) {
	brokers := startKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const topic = "test.readings.poison"
	if err := kafkax.EnsureTopic(ctx, brokers, topic, 1); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	// Write directly with a raw kafka-go writer: Producer.Publish validates, so
	// it could not send this garbage even if we wanted it to.
	good := testReading("sensor-0001", 21.5)
	goodJSON, err := json.Marshal(good)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Structurally valid JSON, but a location in Berlin — rejected by Validate.
	outOfBounds, err := json.Marshal(events.SensorReading{
		SensorID: "sensor-0002",
		Lat:      52.52,
		Lon:      13.40,
		TempC:    21.5,
		Time:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	w := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
	}
	defer w.Close()

	// Order matters: both bad messages come first, so the good one is only
	// reachable if the consumer actually skipped past them.
	err = w.WriteMessages(ctx,
		kafkago.Message{Key: []byte("sensor-0003"), Value: []byte("this is not json at all")},
		kafkago.Message{Key: []byte("sensor-0002"), Value: outOfBounds},
		kafkago.Message{Key: []byte("sensor-0001"), Value: goodJSON},
	)
	if err != nil {
		t.Fatalf("write raw messages: %v", err)
	}

	consumer := kafkax.NewConsumer(brokers, topic, "test-group-poison")
	defer consumer.Close()

	received := make(chan events.SensorReading, 1)
	runCtx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(runCtx, func(_ context.Context, r events.SensorReading) error {
			received <- r
			return nil
		})
	}()

	select {
	case r := <-received:
		// Only the valid reading should ever reach the handler.
		if r.SensorID != "sensor-0001" {
			t.Errorf("handler got %q, want sensor-0001 — invalid readings must be skipped", r.SensorID)
		}
	case err := <-done:
		t.Fatalf("consumer stopped before delivering the valid reading: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for the valid reading; a poison message wedged the consumer")
	}

	stopConsumer()
	if err := <-done; err != nil {
		t.Errorf("consumer returned error on shutdown: %v", err)
	}
}
