// Package httpapi holds the HTTP surface of the sensor-gateway service.
// Keeping it separate from package main means the routing/handler logic can be
// unit-tested without starting a real server.
package httpapi

import (
	"encoding/json"
	"net/http"
	"time"
)

// NewRouter builds the HTTP handler for the gateway. Returning an *http.ServeMux
// (Go's standard-library router) keeps us dependency-free for now; we can swap in
// chi later without changing main.go, because both satisfy http.Handler.
func NewRouter() http.Handler {
	mux := http.NewServeMux()

	// "GET /healthz" is Go 1.22+ method-aware routing: this only matches GET.
	mux.HandleFunc("GET /healthz", handleHealthz)

	return mux
}

// healthResponse is what /healthz returns. Struct tags (`json:"..."`) control the
// JSON field names on the wire — the Go convention is CapitalizedFields (exported)
// mapped to snake_case JSON.
type healthResponse struct {
	Status  string    `json:"status"`
	Service string    `json:"service"`
	Time    time.Time `json:"time"`
}

// handleHealthz is an http.HandlerFunc: (w, r). We write JSON back to the client.
func handleHealthz(w http.ResponseWriter, r *http.Request) {
	resp := healthResponse{
		Status:  "ok",
		Service: "sensor-gateway",
		Time:    time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	// json.NewEncoder(w).Encode(...) streams the struct straight to the response.
	// It returns an error, but for a tiny health check we can safely ignore it.
	_ = json.NewEncoder(w).Encode(resp)
}
