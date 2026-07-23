// Package httpapi holds the HTTP surface of the sensor-gateway service.
// Keeping it separate from package main means the routing/handler logic can be
// unit-tested without starting a real server.
package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// NewRouter builds the HTTP handler for the gateway. Phase 0 returned a stdlib
// *http.ServeMux here; Phase 2 swaps in chi — and main.go doesn't change,
// because chi.Router satisfies the same one-method http.Handler interface the
// stdlib mux does. That interface boundary is the entire reason the swap is
// this cheap.
//
// Why chi at all? Two things the stdlib mux lacks:
//   - middleware: cross-cutting wrappers (logging, panic recovery, timeouts)
//     applied to every route in one place instead of inside each handler
//   - route grouping + URL params ({id}) that stay readable as the sensor
//     CRUD API grows
//
// The store parameter is this service's entire dependency injection: main
// passes the Redis-backed registry, tests pass an in-memory fake. No
// container, no decorators — a function argument.
func NewRouter(store SensorStore) http.Handler {
	r := chi.NewRouter()

	// Middleware in chi is nothing magic: each one is a plain function taking
	// an http.Handler and returning a new http.Handler that wraps it. r.Use
	// stacks them, so a request flows Logger → Recoverer → Timeout → route
	// handler, and the response unwinds back out the same way.
	r.Use(middleware.Logger) // one line per request: method, path, status, duration

	// Recoverer turns a panicking handler into a 500 response + stack trace in
	// the log. Without it, one panic on one request kills the whole process —
	// every in-flight request from every other client dies with it.
	r.Use(middleware.Recoverer)

	// Timeout puts a deadline on the request *context*. Handlers doing I/O
	// (Redis calls, from Step 3 on) pass r.Context() down, so a hung
	// dependency cancels the request instead of leaking a goroutine forever.
	r.Use(middleware.Timeout(30 * time.Second))

	// "GET /healthz" in the Phase 0 mux becomes method + path as separate
	// arguments. Same behavior: other methods on this path get a 405.
	r.Get("/healthz", handleHealthz)

	// Route grouping: everything under /api/sensors in one block. {id} is a
	// URL parameter — the handler reads it with chi.URLParam.
	h := &sensorHandlers{store: store}
	r.Route("/api/sensors", func(r chi.Router) {
		r.Post("/", h.create)
		r.Get("/", h.list)
		r.Get("/{id}", h.get)
		r.Delete("/{id}", h.remove)
	})

	return r
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
