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

func main() {
	// Read the port from an env var, defaulting to 8080. Twelve-factor style:
	// config comes from the environment, which is exactly how it'll work in
	// docker-compose and later Kubernetes.
	addr := ":" + getenv("PORT", "8080")

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewRouter(),
		ReadHeaderTimeout: 5 * time.Second, // basic protection against slow-loris
	}

	// Start the server in a goroutine so main can go on to wait for a shutdown
	// signal. `go func() { ... }()` launches a concurrent lightweight thread —
	// this is your first goroutine. We'll lean on these heavily from Phase 1.
	go func() {
		log.Printf("sensor-gateway listening on %s", addr)
		// ListenAndServe blocks until the server stops. http.ErrServerClosed is
		// the *expected* error on a clean shutdown, so we only log real failures.
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Block until we receive SIGINT (Ctrl-C) or SIGTERM (what Kubernetes sends to
	// stop a pod). This is graceful-shutdown 101 and matters a lot in K8s later.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop // receive from the channel: blocks here until a signal arrives

	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
	log.Println("bye")
}

// getenv returns the env var value, or a fallback if it's unset/empty.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
