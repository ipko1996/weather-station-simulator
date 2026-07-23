// Command aggregator is the entrypoint for the aggregation-service.
//
// It consumes validated readings from sensor.readings.clean, folds them into
// 10-second tumbling windows, and publishes one WindowAggregate per closed
// window to weather.aggregates — the stream the notification-gateway fans out
// to browsers.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ipko1996/huweathersim/pkg/events"
	"github.com/ipko1996/huweathersim/pkg/kafkax"
	"github.com/ipko1996/huweathersim/services/aggregation-service/internal/window"
)

// main stays a two-liner on purpose — see the simulator's main for the full
// reasoning (log.Fatalf skips defers; run() owns all work and cleanup).
func main() {
	if err := run(); err != nil {
		log.Fatalf("aggregation-service: %v", err)
	}
}

func run() error {
	var (
		brokers = strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
		// Sharing this group ID across replicas splits the clean topic's
		// partitions between them — each reading is windowed exactly once
		// within the group. This is the SECOND consumer group on the shared
		// pipeline: ingestion-consumer reads sensor.readings with its own
		// group, we read sensor.readings.clean with ours, and neither group's
		// offsets affect the other.
		groupID = getenv("KAFKA_GROUP_ID", "aggregation-service")
	)

	windowSize, err := getduration("WINDOW_SIZE", 10*time.Second) // PROJECT.md §4
	if err != nil {
		return err
	}
	// 2s grace: late readings within it still count toward their window. See
	// the window package for the event-time-vs-wall-clock reasoning.
	lateness, err := getduration("ALLOWED_LATENESS", 2*time.Second)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	topicCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := kafkax.EnsureTopic(topicCtx, brokers, kafkax.CleanTopic); err != nil {
		return fmt.Errorf("ensure topic %s: %w", kafkax.CleanTopic.Name, err)
	}
	if err := kafkax.EnsureTopic(topicCtx, brokers, kafkax.AggregatesTopic); err != nil {
		return fmt.Errorf("ensure topic %s: %w", kafkax.AggregatesTopic.Name, err)
	}

	producer := kafkax.NewProducer(brokers, kafkax.AggregatesTopic.Name)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("closing producer: %v", err)
		}
	}()

	consumer := kafkax.NewConsumer(brokers, kafkax.CleanTopic.Name, groupID)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("closing consumer: %v", err)
		}
	}()

	tumbler := window.New(windowSize, lateness)

	// The flusher: once a second, harvest closed windows and publish them.
	// This is the Tumbler's second goroutine — the mutex inside it exists
	// precisely because this loop and the consumer below run concurrently.
	flusherDone := make(chan struct{})
	go func() {
		defer close(flusherDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, agg := range tumbler.Closed(time.Now().UTC()) {
					if err := producer.Publish(ctx, agg); err != nil {
						// An aggregate that fails to publish is gone — see
						// the delivery trade-off note below. Log and move on;
						// the next window regenerates the picture in 10s.
						log.Printf("publish aggregate [%s, %s): %v",
							agg.WindowStart.Format("15:04:05"), agg.WindowEnd.Format("15:04:05"), err)
						continue
					}
					log.Printf("window [%s, %s): avg=%.2f°C min=%.1f max=%.1f n=%d (late so far: %d)",
						agg.WindowStart.Format("15:04:05"), agg.WindowEnd.Format("15:04:05"),
						agg.AvgTempC, agg.MinTempC, agg.MaxTempC, agg.Count, tumbler.LateCount())
				}
			}
		}
	}()

	log.Printf("aggregating %s -> %s (window %s, grace %s) as group %q",
		kafkax.CleanTopic.Name, kafkax.AggregatesTopic.Name, windowSize, lateness, groupID)

	// # Delivery trade-off, stated plainly
	//
	// The reading's offset commits when Add returns — BEFORE its window is
	// flushed. A crash between Add and the flush loses that partial window:
	// aggregates are effectively AT-MOST-ONCE, while the database path is
	// at-least-once. That asymmetry is deliberate and correct for this data:
	// an aggregate is a live display value that regenerates every 10 seconds,
	// and TimescaleDB — not this topic — is the durable record. Buying
	// exactly-once here (transactions, or committing only after flush) would
	// cost real complexity to protect a number nobody would miss.
	//
	// Late readings return nil (not an error): late is a permanent fact about
	// a reading, not a transient failure — redelivering it would only make it
	// later. It was counted, and that count becomes a Phase 5 metric.
	err = kafkax.Run(ctx, consumer, func(_ context.Context, r events.SensorReading) error {
		tumbler.Add(r, time.Now().UTC())
		return nil
	}, nil) // nil dlq: the clean topic is validated upstream, poison here means an ingestion bug — log loudly is enough

	// Wait for the flusher before closing the producer underneath it.
	<-flusherDone

	if err != nil {
		return fmt.Errorf("consumer stopped: %w", err)
	}
	log.Println("bye")
	return nil
}

// getenv returns the env var value, or a fallback if it's unset/empty.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getduration reads a Go duration string such as "5s" or "500ms".
func getduration(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("env %s: %q is not a duration (try 5s): %w", key, raw, err)
	}
	return v, nil
}
