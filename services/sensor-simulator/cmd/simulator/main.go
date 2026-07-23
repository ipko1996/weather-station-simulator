// Command simulator is the entrypoint for the sensor-simulator service.
//
// Phase 1 ran exactly one hardcoded sensor from env vars. Phase 2 replaces
// that with a registry-driven fleet: the Manager polls Redis for the desired
// sensor list and runs one goroutine per sensor, so adding a sensor through
// the gateway's API is all it takes for readings to start flowing.
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

	goredis "github.com/redis/go-redis/v9"

	"github.com/ipko1996/huweathersim/pkg/events"
	"github.com/ipko1996/huweathersim/pkg/kafkax"
	"github.com/ipko1996/huweathersim/pkg/registry"
	"github.com/ipko1996/huweathersim/services/sensor-simulator/internal/simulator"
)

// main stays a two-liner on purpose. log.Fatalf calls os.Exit, which SKIPS
// deferred functions — so any `defer producer.Close()` in the same function
// would silently never run on the error path, dropping buffered messages.
// Putting all real work (and all defers) in run() means the defers always
// execute, and main only decides the process exit code.
func main() {
	if err := run(); err != nil {
		log.Fatalf("sensor-simulator: %v", err)
	}
}

func run() error {
	// Twelve-factor config: everything comes from the environment, which is how
	// it works in compose (Phase 2) and Kubernetes ConfigMaps (Phase 4). The
	// Phase 1 SENSOR_* variables are gone — sensor definitions now live in the
	// registry, put there by the gateway's API.
	var (
		brokers   = strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
		topic     = getenv("KAFKA_TOPIC", events.TopicSensorReadings)
		redisAddr = getenv("REDIS_ADDR", "localhost:6379")
	)

	// 15s balances registry load against worst-case latency — and it's the
	// SAFETY NET, not the primary path: the Pub/Sub fast path (next step)
	// makes reaction near-instant, and the poll guarantees convergence even
	// when notifications are lost.
	reconcileInterval, err := getduration("RECONCILE_INTERVAL", 15*time.Second)
	if err != nil {
		return err
	}

	// signal.NotifyContext returns a context that cancels on SIGINT/SIGTERM.
	// Everything downstream — manager, every sensor worker, in-flight Kafka
	// writes — hangs off this one ctx, so a single Ctrl-C unwinds it all.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Printf("closing redis client: %v", err)
		}
	}()

	// Fail fast if Redis is missing (go-redis dials lazily, so PING is the
	// first real connection) — same boot philosophy as EnsureTopic below.
	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPing()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("redis unreachable at %s: %w", redisAddr, err)
	}
	log.Printf("connected to redis at %s", redisAddr)

	// Create the topic explicitly before producing. Auto-creation would give us
	// a 1-partition topic and silently cap Phase 6 autoscaling — see EnsureTopic.
	topicCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := kafkax.EnsureTopic(topicCtx, brokers, topic, kafkax.ReadingsPartitions); err != nil {
		return fmt.Errorf("ensure topic %s: %w", topic, err)
	}
	log.Printf("topic %s ready (%d partitions) on %s",
		topic, kafkax.ReadingsPartitions, strings.Join(brokers, ","))

	producer := kafkax.NewProducer(brokers, topic)
	// Closing the producer flushes anything still buffered. Manager.Run waits
	// for every sensor worker before returning, so by the time this defer
	// fires, nothing is publishing anymore — that ordering is the whole
	// reason the manager owns a WaitGroup.
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("closing producer: %v", err)
		}
	}()

	// One shared producer for the whole fleet: kafka-go's Writer is
	// goroutine-safe and batches across sensors, so 2,000 workers do NOT need
	// 2,000 connections.
	manager := simulator.NewManager(registry.New(rdb), producer, reconcileInterval)

	// The Pub/Sub fast path (registry.Watch → manager.Kick) is wired here in
	// the next step; until then the poll interval alone drives convergence.

	log.Printf("manager starting: reconciling every %s", reconcileInterval)
	return manager.Run(ctx)
}

// getenv returns the env var value, or a fallback if it's unset/empty.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getduration reads a Go duration string such as "5s" or "500ms". A typo
// should stop the service at boot with a clear error, not be silently
// defaulted away.
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
