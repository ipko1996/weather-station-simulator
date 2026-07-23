// Command gateway is the entrypoint binary for the sensor-gateway service.
// It stays deliberately thin: read config, build the router, start the server,
// and shut down cleanly. All real logic lives in internal/ packages.
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

	"github.com/ipko1996/huweathersim/services/sensor-gateway/internal/httpapi"
)

// main stays a two-liner on purpose (same reasoning as the simulator's main):
// log.Fatalf calls os.Exit, which SKIPS deferred functions. The Phase 0 version
// of this file even called log.Fatalf from inside the server goroutine — an
// os.Exit from a side goroutine that would have skipped every defer in the
// process. Putting all work and all defers in run() closes that hole.
func main() {
	if err := run(); err != nil {
		log.Fatalf("sensor-gateway: %v", err)
	}
}

func run() error {
	// Twelve-factor config: everything comes from the environment, which is how
	// it works in compose (Phase 2) and Kubernetes ConfigMaps (Phase 4).
	addr := ":" + getenv("PORT", "8080")

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewRouter(),
		ReadHeaderTimeout: 5 * time.Second, // basic protection against slow-loris
	}

	// signal.NotifyContext replaces Phase 0's manual signal channel: it returns
	// a context that cancels on SIGINT (Ctrl-C) or SIGTERM (what Kubernetes
	// sends to stop a pod). Graceful shutdown starts the moment ctx is done.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ListenAndServe blocks forever, so it runs in a goroutine and hands its
	// error back over a channel. The buffer of 1 matters: if the server fails
	// while nobody is receiving yet, an unbuffered send would block and leak
	// the goroutine — buffered, it deposits the error and exits.
	errCh := make(chan error, 1)
	go func() {
		log.Printf("sensor-gateway listening on %s", addr)
		errCh <- srv.ListenAndServe()
	}()

	// Two things can end this service: the server dying (port in use, etc.) or
	// a shutdown signal. select waits on both channels at once and takes
	// whichever fires first — Go's Promise.race.
	select {
	case err := <-errCh:
		// http.ErrServerClosed is what ListenAndServe returns after a clean
		// Shutdown call, but reaching this branch means we did NOT call
		// Shutdown — the server failed on its own, so any error is real.
		return err
	case <-ctx.Done():
	}

	log.Println("shutting down...")
	// A fresh context for the drain deadline — ctx is already cancelled, so
	// reusing it would make Shutdown bail out immediately instead of giving
	// in-flight requests up to 10s to finish.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}

	// Shutdown made ListenAndServe return ErrServerClosed into errCh; receive
	// it so the goroutine's send is complete, and ignore it — here it just
	// means "shut down cleanly, as asked".
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
