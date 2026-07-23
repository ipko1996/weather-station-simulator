// Command telemetry is the entrypoint for the telemetry-api service — a
// Phase 2 stub that completes the six-service topology; see internal/httpapi
// for what it becomes in Phase 5.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ipko1996/huweathersim/services/telemetry-api/internal/httpapi"
)

// main stays a two-liner on purpose — see the simulator's main for the full
// reasoning (log.Fatalf skips defers; run() owns all work and cleanup).
func main() {
	if err := run(); err != nil {
		log.Fatalf("telemetry-api: %v", err)
	}
}

// run mirrors the sensor-gateway's shape exactly: error channel from the
// server goroutine, select against the signal context, drain on shutdown.
func run() error {
	addr := ":" + getenv("PORT", "8083")

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewRouter(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("telemetry-api listening on %s (stub until Phase 5)", addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Println("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
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
