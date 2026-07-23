// Package httpapi is the telemetry-api's HTTP surface.
//
// Phase 2 ships this service as a deliberate STUB: it exists so the compose
// (and later Kubernetes) topology is final — six services, ports, health
// checks — while its real job arrives in Phase 5, when it starts querying
// Prometheus for pod counts, consumer lag and RED metrics to feed the
// Telemetry page (PROJECT.md §2).
package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func NewRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/healthz", handleHealthz)
	r.Get("/api/telemetry", handleTelemetry)

	return r
}

type healthResponse struct {
	Status  string    `json:"status"`
	Service string    `json:"service"`
	Time    time.Time `json:"time"`
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, healthResponse{Status: "ok", Service: "telemetry-api", Time: time.Now().UTC()})
}

// handleTelemetry returns a static placeholder until Phase 5 wires Prometheus.
func handleTelemetry(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{
		"status": "stub",
		"note":   "real data in Phase 5 (Prometheus-backed health grid, lag, RED metrics)",
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
