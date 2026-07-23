// Package kafkax holds thin wrappers around segmentio/kafka-go.
//
// The goal is that no service ever configures Kafka by hand. Settings that must
// agree across the whole system (partition key strategy, acknowledgement mode,
// commit semantics) live here once, so a new service can't accidentally opt out
// of them. "x" is a common Go suffix for "extensions to <library>".
package kafkax

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ipko1996/huweathersim/pkg/events"
)

// Producer publishes events to one Kafka topic.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer builds a producer for one topic.
//
// Two settings here are load-bearing and deliberately not left at their defaults:
//
//   - Balancer: &kafka.Hash{} routes by hashing the message key, so a given
//     sensor's readings always land on the same partition and stay in order.
//     Without it kafka-go round-robins, scattering one sensor across partitions.
//
//   - RequiredAcks: RequireAll waits for every in-sync replica to confirm the
//     write. The zero value on a struct literal is RequireNone — fire-and-forget,
//     which drops messages silently if a broker dies. (kafka-go only substitutes
//     RequireAll inside the deprecated NewWriter constructor, not here.) Readings
//     are the product; losing them quietly is the one failure we can't tolerate.
//
//   - BatchTimeout: see batchTimeout below — the default cripples low-rate
//     producers.
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.Hash{},
			// Wait for the full in-sync replica set before calling a write done.
			RequiredAcks: kafka.RequireAll,
			// Async: false means WriteMessages blocks until the broker answers,
			// so a failed publish surfaces as an error instead of vanishing.
			Async:        false,
			BatchTimeout: batchTimeout,
		},
	}
}

// batchTimeout is how long the writer waits for a batch to fill before sending
// it anyway.
//
// kafka-go batches writes for throughput: it flushes when BatchSize (default
// 100) messages are buffered, OR when BatchTimeout elapses. Its default timeout
// is a full SECOND, which is disastrous for a per-sensor producer — one sensor
// emits one message at a time, so the batch never reaches 100 and every publish
// blocks for the whole second. A sensor configured to emit every 100ms would
// still only manage one reading per second.
//
// 10ms keeps latency negligible at low rates while preserving batching where it
// actually matters: at the stress-test rate of 2,000 msg/s, 100 messages
// accumulate in well under 10ms, so full batches still form and throughput is
// unaffected. This also protects the PROJECT.md §3 SLO of <2s p95 ingestion
// latency — a 1s producer-side stall would burn half that budget doing nothing.
const batchTimeout = 10 * time.Millisecond

// Publish sends one event to Kafka.
//
// It takes the events.Event interface rather than a concrete type, because
// everything the producer does — validate, key, marshal — is exactly the
// Event contract; the payload's shape is irrelevant here. One producer type
// therefore serves readings, dead letters and (soon) window aggregates.
//
// The event is validated first: a producer that emits garbage forces every
// downstream consumer to defend against it, so the cheapest place to stop bad
// data is before it enters the log at all.
func (p *Producer) Publish(ctx context.Context, e events.Event) error {
	if err := e.Validate(); err != nil {
		return fmt.Errorf("invalid event: %w", err)
	}

	value, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// The ctx here is a real cancellation signal: if the service is shutting down
	// mid-write, this call unwinds instead of hanging.
	//
	// No Time field is set on the message: the broker stamps arrival time, and
	// the payload carries its own event time (e.g. a reading's ts) — the only
	// timestamp downstream logic is allowed to care about.
	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   e.Key(), // decides the partition — see events.SensorReading.Key
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("write to %s: %w", p.writer.Topic, err)
	}
	return nil
}

// Close flushes any buffered writes and releases the connection. Always defer
// this — without it, a shutdown can drop messages the writer hadn't sent yet.
func (p *Producer) Close() error {
	return p.writer.Close()
}
