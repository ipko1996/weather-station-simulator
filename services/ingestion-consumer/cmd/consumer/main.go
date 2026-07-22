// Command consumer is the entrypoint for the ingestion-consumer service.
//
// Phase 1 consumes sensor.readings and logs each one. Phase 2 adds the real
// pipeline work: validate, write to TimescaleDB, re-publish to the clean topic.
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
	"github.com/ipko1996/huweathersim/services/ingestion-consumer/internal/consumer"
)

// main stays a two-liner on purpose: log.Fatalf calls os.Exit, which skips
// deferred functions. All real work — and every defer — lives in run(), so the
// consumer always leaves its group cleanly, even on an error exit.
func main() {
	if err := run(); err != nil {
		log.Fatalf("ingestion-consumer: %v", err)
	}
}

func run() error {
	var (
		brokers = strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
		topic   = getenv("KAFKA_TOPIC", events.TopicSensorReadings)
		// The consumer group ID is what ties replicas together. Every pod of
		// this service shares it, so Kafka splits the 6 partitions between them
		// and each reading is processed once across the group. Keep it stable:
		// changing it creates a brand-new group that re-reads the whole topic
		// from the beginning.
		groupID = getenv("KAFKA_GROUP_ID", "ingestion-consumer")
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Every service that touches the topic ensures it exists — not just the
	// producer. Otherwise, starting this consumer first on a fresh broker would
	// auto-create sensor.readings with the broker default of ONE partition
	// (capping consumer parallelism at a single pod), and nothing would ever
	// complain. EnsureTopic is idempotent, so producer and consumer both calling
	// it is safe — whoever starts first creates it correctly.
	topicCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := kafkax.EnsureTopic(topicCtx, brokers, topic, kafkax.ReadingsPartitions); err != nil {
		return fmt.Errorf("ensure topic %s: %w", topic, err)
	}

	c := kafkax.NewConsumer(brokers, topic, groupID)
	// Closing leaves the consumer group cleanly so Kafka rebalances immediately
	// rather than waiting for the session to time out. In run() (not main) so
	// it executes on every exit path, including errors.
	defer func() {
		if err := c.Close(); err != nil {
			log.Printf("closing consumer: %v", err)
		}
	}()

	var stats consumer.Stats

	log.Printf("consuming %s as group %q from %s", topic, groupID, strings.Join(brokers, ","))

	// Run blocks until ctx is cancelled or a handler fails permanently.
	if err := c.Run(ctx, stats.Handler); err != nil {
		return fmt.Errorf("consumer stopped: %w", err)
	}

	log.Printf("processed %d readings, bye", stats.Processed())
	return nil
}

// getenv returns the env var value, or a fallback if it's unset/empty.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
