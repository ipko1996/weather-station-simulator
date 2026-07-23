// Command notifier is the entrypoint for the notification-gateway service:
// it consumes weather.aggregates and fans every aggregate out to all
// connected WebSocket clients. Kept separate from the aggregation-service so
// each can scale on its own driver — aggregation on message volume, this on
// connected-client count (PROJECT.md §4).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ipko1996/huweathersim/pkg/events"
	"github.com/ipko1996/huweathersim/pkg/kafkax"
	"github.com/ipko1996/huweathersim/services/notification-gateway/internal/hub"
	"github.com/ipko1996/huweathersim/services/notification-gateway/internal/httpapi"
)

// main stays a two-liner on purpose — see the simulator's main for the full
// reasoning (log.Fatalf skips defers; run() owns all work and cleanup).
func main() {
	if err := run(); err != nil {
		log.Fatalf("notification-gateway: %v", err)
	}
}

func run() error {
	var (
		addr    = ":" + getenv("PORT", "8082")
		brokers = strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	)

	// PER-INSTANCE group ID — the deliberate inversion of every other
	// consumer in this system. Ingestion replicas SHARE a group to split the
	// work; notification replicas each need the WHOLE aggregate stream,
	// because each serves a different set of browsers. Same group here would
	// mean each browser sees only a fraction of the updates. The hostname
	// suffix gives every instance (container hostnames are unique) its own
	// group, i.e. its own full copy of the stream.
	host, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("hostname for group id: %w", err)
	}
	groupID := "notification-gateway-" + host

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	topicCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := kafkax.EnsureTopic(topicCtx, brokers, kafkax.AggregatesTopic); err != nil {
		return fmt.Errorf("ensure topic %s: %w", kafkax.AggregatesTopic.Name, err)
	}

	// WithLatestStartOffset: a fresh instance must not replay an hour of
	// stale aggregates at newly connected clients — live data only.
	consumer := kafkax.NewConsumer(brokers, kafkax.AggregatesTopic.Name, groupID,
		kafkax.WithLatestStartOffset())
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("closing consumer: %v", err)
		}
	}()

	h := hub.New()

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewRouter(h),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Two blocking loops must coexist: the HTTP server (WebSocket clients)
	// and the Kafka consumer (aggregates in). Each runs in a goroutine and
	// reports into its own buffered channel; the select below takes whichever
	// finishes first — same shape as the sensor-gateway's main, one loop more.
	httpErr := make(chan error, 1)
	go func() {
		log.Printf("notification-gateway listening on %s (ws at /ws)", addr)
		httpErr <- srv.ListenAndServe()
	}()

	consumerErr := make(chan error, 1)
	go func() {
		log.Printf("fanning out %s as group %q", kafkax.AggregatesTopic.Name, groupID)
		consumerErr <- kafkax.Run(ctx, consumer, func(_ context.Context, a events.WindowAggregate) error {
			// Re-marshal the typed aggregate rather than forwarding raw
			// bytes: what goes to browsers is exactly what passed Validate,
			// and the wire shape stays pinned to events.WindowAggregate.
			msg, err := json.Marshal(a)
			if err != nil {
				return fmt.Errorf("marshal aggregate: %w", err)
			}
			h.Broadcast(msg)
			return nil
		}, nil) // nil dlq: aggregates come from our own producer; poison means our bug
	}()

	select {
	case err := <-httpErr:
		return fmt.Errorf("http server: %w", err)
	case err := <-consumerErr:
		if err != nil {
			return fmt.Errorf("consumer stopped: %w", err)
		}
		// nil means ctx was cancelled — clean shutdown, fall through.
	case <-ctx.Done():
	}

	log.Println("shutting down...")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-httpErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
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
