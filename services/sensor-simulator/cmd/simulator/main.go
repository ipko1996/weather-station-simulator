// Command simulator is the entrypoint for the sensor-simulator service.
//
// Phase 1 runs exactly one hardcoded sensor, which is enough to prove the Kafka
// mechanics end to end. Phase 2 turns this into N sensors driven by the Redis
// registry — the loop below becomes a loop over registered sensors.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ipko1996/huweathersim/pkg/events"
	"github.com/ipko1996/huweathersim/pkg/kafkax"
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
	// it will work in compose (Phase 2) and Kubernetes ConfigMaps (Phase 4).
	var (
		brokers  = strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
		topic    = getenv("KAFKA_TOPIC", events.TopicSensorReadings)
		sensorID = getenv("SENSOR_ID", "sensor-0001")
		pattern  = simulator.Pattern(getenv("DRIFT_PATTERN", string(simulator.PatternNoisy)))
	)

	lat, err := getfloat("SENSOR_LAT", 47.4979) // Budapest
	if err != nil {
		return err
	}
	lon, err := getfloat("SENSOR_LON", 19.0402)
	if err != nil {
		return err
	}
	startTemp, err := getfloat("START_TEMP_C", 20.0)
	if err != nil {
		return err
	}
	interval, err := getduration("EMIT_INTERVAL", 5*time.Second) // PROJECT.md §3 baseline
	if err != nil {
		return err
	}

	if !pattern.Valid() {
		return fmt.Errorf("invalid DRIFT_PATTERN %q (want steady, rising, falling or noisy)", pattern)
	}

	// signal.NotifyContext is the idiomatic shorthand for the manual signal
	// channel in sensor-gateway's main.go: it returns a context that cancels on
	// SIGINT/SIGTERM. Everything downstream takes that ctx, so one Ctrl-C (or
	// one `kubectl delete pod`) unwinds the entire service cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
	// Closing the producer flushes anything still buffered. Because this defer
	// lives in run() (not main), it runs on EVERY exit path, including errors.
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("closing producer: %v", err)
		}
	}()

	sensor := simulator.NewSensor(sensorID, lat, lon, startTemp, interval, pattern)
	if err := sensor.Validate(); err != nil {
		return fmt.Errorf("sensor configuration: %w", err)
	}

	// Run blocks until ctx is cancelled. Phase 2 launches one goroutine per
	// sensor here instead of running a single one on the main goroutine.
	if err := sensor.Run(ctx, producer); err != nil {
		return fmt.Errorf("sensor stopped: %w", err)
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

// getfloat reads a float env var. A typo in a coordinate should stop the
// service at boot with a clear error, not produce readings that get rejected
// downstream for the rest of the deployment's life.
func getfloat(key string, fallback float64) (float64, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("env %s: %q is not a number: %w", key, raw, err)
	}
	return v, nil
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
