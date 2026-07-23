package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/ipko1996/huweathersim/pkg/registry"
)

// SensorStore is what the HTTP handlers need from a sensor registry — nothing
// more. It is declared HERE, by the consumer, not next to registry.Registry:
// that inversion is idiomatic Go. registry.Registry satisfies it without ever
// knowing it exists (interface satisfaction is implicit, checked structurally
// by the compiler), and tests satisfy it with an in-memory map — which is why
// the handler tests below need no Redis, no network, no containers.
type SensorStore interface {
	Add(ctx context.Context, s registry.Sensor) error
	Remove(ctx context.Context, id string) (bool, error)
	Get(ctx context.Context, id string) (registry.Sensor, error)
	List(ctx context.Context) ([]registry.Sensor, error)
}

// sensorHandlers groups the CRUD handlers around their one dependency.
type sensorHandlers struct {
	store SensorStore
}

// createSensorRequest is the POST body. It is deliberately NOT
// registry.Sensor: the wire shape and the domain shape evolve separately
// (interval is a human "5s" string here, a time.Duration there; id/created_at
// are server-assigned and must not be client-settable).
type createSensorRequest struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
	// A pointer distinguishes "field absent" (nil → apply the default) from
	// an explicit 0 — with a plain float64 a client asking for 0°C would be
	// indistinguishable from one who never mentioned temperature.
	StartTempC *float64 `json:"start_temp_c"`
	Pattern    string   `json:"pattern"`
	Interval   string   `json:"interval"`
}

// Defaults for omitted fields, per the plan: a bare {"lat":…,"lon":…} click
// on the Phase 3 map should just work.
const (
	defaultStartTempC = 15.0
	defaultPattern    = registry.PatternNoisy
	defaultInterval   = 5 * time.Second // PROJECT.md §3 baseline emit interval
)

// sensorResponse is the wire shape of a sensor in every response.
type sensorResponse struct {
	ID         string    `json:"id"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	StartTempC float64   `json:"start_temp_c"`
	Pattern    string    `json:"pattern"`
	Interval   string    `json:"interval"` // "5s" — same format the request accepts
	CreatedAt  time.Time `json:"created_at"`
}

func toResponse(s registry.Sensor) sensorResponse {
	return sensorResponse{
		ID:         s.ID,
		Lat:        s.Lat,
		Lon:        s.Lon,
		StartTempC: s.StartTempC,
		Pattern:    string(s.Pattern),
		Interval:   s.Interval.String(),
		CreatedAt:  s.CreatedAt,
	}
}

// create handles POST /api/sensors.
func (h *sensorHandlers) create(w http.ResponseWriter, r *http.Request) {
	var req createSensorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON body: %w", err))
		return
	}

	startTemp := defaultStartTempC
	if req.StartTempC != nil {
		startTemp = *req.StartTempC
	}
	pattern := defaultPattern
	if req.Pattern != "" {
		pattern = registry.Pattern(req.Pattern)
	}
	interval := defaultInterval
	if req.Interval != "" {
		var err error
		if interval, err = time.ParseDuration(req.Interval); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("interval: %q is not a duration (try \"5s\")", req.Interval))
			return
		}
	}

	s := registry.Sensor{
		ID:         newSensorID(),
		Lat:        req.Lat,
		Lon:        req.Lon,
		StartTempC: startTemp,
		Pattern:    pattern,
		Interval:   interval,
		CreatedAt:  time.Now().UTC(),
	}

	// Add validates before storing, so every rule (Hungary bounding box,
	// pattern names, interval range) lives in pkg/registry — the handler adds
	// no rules of its own, it only translates errors to HTTP. Omitted lat/lon
	// need no special casing: they decode as 0, and (0,0) is in the Atlantic,
	// which the bounding-box check rejects with a clear message.
	if err := h.store.Add(r.Context(), s); err != nil {
		// The status split matters: 400 says "your request is wrong, do not
		// retry it" and is only true for validation failures. Everything
		// else (Redis down, timeout) is OUR failure — 500, so clients and
		// Phase 5 error-rate metrics see it as such.
		if errors.Is(err, registry.ErrInvalidSensor) {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusCreated, toResponse(s))
}

// list handles GET /api/sensors.
func (h *sensorHandlers) list(w http.ResponseWriter, r *http.Request) {
	sensors, err := h.store.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// Start from an allocated empty slice: a nil slice marshals to JSON
	// `null`, and no frontend wants to .map() over null.
	resp := make([]sensorResponse, 0, len(sensors))
	for _, s := range sensors {
		resp = append(resp, toResponse(s))
	}
	writeJSON(w, http.StatusOK, resp)
}

// get handles GET /api/sensors/{id}.
func (h *sensorHandlers) get(w http.ResponseWriter, r *http.Request) {
	// chi.URLParam pulls the {id} segment the router matched — chi's answer
	// to req.params.id.
	id := chi.URLParam(r, "id")

	s, err := h.store.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(s))
}

// remove handles DELETE /api/sensors/{id}.
func (h *sensorHandlers) remove(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	existed, err := h.store.Remove(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if !existed {
		writeError(w, http.StatusNotFound, fmt.Errorf("sensor %s: %w", id, registry.ErrNotFound))
		return
	}
	// 204: the request succeeded and there is deliberately nothing to say.
	w.WriteHeader(http.StatusNoContent)
}

// newSensorID returns an id like "sensor-3f9c2a1b". crypto/rand instead of
// math/rand because ids must not collide across gateway replicas, and 4
// random bytes (2^32) is plenty of space for a 2,000-sensor ceiling without
// pulling in a uuid dependency.
func newSensorID() string {
	b := make([]byte, 4)
	// rand.Read only fails if the OS entropy source is broken, in which case
	// the process has bigger problems — panicking is the standard treatment.
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand unavailable: %v", err))
	}
	return "sensor-" + hex.EncodeToString(b)
}

// errorResponse is the JSON shape of every non-2xx answer.
type errorResponse struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorResponse{Error: err.Error()})
}

// writeJSON is the single place a response body gets written, so the header
// order rule lives once: headers, then status, then body — WriteHeader locks
// the headers, and anything Set after it is silently dropped.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Too late for an error status — the header is gone. Log and move on.
		log.Printf("write response: %v", err)
	}
}
