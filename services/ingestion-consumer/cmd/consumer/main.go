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
	"github.com/ipko1996/huweathersim/services/ingestion-consumer/internal/store"
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
		// Matches the compose stack's TimescaleDB (user/password/db all
		// "weather"); compose overrides the host to timescaledb:5432.
		dbURL = getenv("DATABASE_URL", "postgres://weather:weather@localhost:5432/weather")
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Database first, Kafka second: joining the consumer group is the moment
	// Kafka starts counting on us, so every other dependency must already be
	// proven healthy by then.
	dbCtx, cancelDB := context.WithTimeout(ctx, 30*time.Second)
	defer cancelDB()
	pool, err := store.NewPool(dbCtx, dbURL)
	if err != nil {
		return err
	}
	// Close returns connections and shuts the pool down; in run() so it fires
	// on every exit path.
	defer pool.Close()

	if err := store.EnsureSchema(dbCtx, pool); err != nil {
		return err
	}
	log.Printf("timescaledb ready (schema ensured)")

	// Every service that touches a topic ensures it exists — not just the
	// producer. Otherwise, starting this consumer first on a fresh broker
	// would fail outright (auto-creation is off in compose). EnsureTopic is
	// idempotent, so producer and consumer both calling it is safe — whoever
	// starts first creates it correctly. This service touches two topics: it
	// consumes raw readings and produces to the clean topic.
	spec := kafkax.ReadingsTopic
	spec.Name = topic
	topicCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := kafkax.EnsureTopic(topicCtx, brokers, spec); err != nil {
		return fmt.Errorf("ensure topic %s: %w", spec.Name, err)
	}
	if err := kafkax.EnsureTopic(topicCtx, brokers, kafkax.CleanTopic); err != nil {
		return fmt.Errorf("ensure topic %s: %w", kafkax.CleanTopic.Name, err)
	}

	// The producer half of this service: validated readings go back out on
	// the clean topic for downstream consumers (aggregation, Phase 2+).
	cleanProducer := kafkax.NewProducer(brokers, kafkax.CleanTopic.Name)
	defer func() {
		if err := cleanProducer.Close(); err != nil {
			log.Printf("closing clean producer: %v", err)
		}
	}()

	c := kafkax.NewConsumer(brokers, topic, groupID)
	// Closing leaves the consumer group cleanly so Kafka rebalances immediately
	// rather than waiting for the session to time out. In run() (not main) so
	// it executes on every exit path, including errors.
	defer func() {
		if err := c.Close(); err != nil {
			log.Printf("closing consumer: %v", err)
		}
	}()

	pipeline := consumer.NewPipeline(store.New(pool), cleanProducer)

	log.Printf("consuming %s as group %q from %s", topic, groupID, strings.Join(brokers, ","))

	// Run blocks until ctx is cancelled or a handler error stops it — which
	// for this service means "database trouble": exiting IS the retry
	// mechanism (restart → redeliver → ON CONFLICT dedupes), see
	// Pipeline.Handle.
	if err := c.Run(ctx, pipeline.Handle); err != nil {
		return fmt.Errorf("consumer stopped: %w", err)
	}

	log.Printf("processed %d readings, bye", pipeline.Processed())
	return nil
}

// getenv returns the env var value, or a fallback if it's unset/empty.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
