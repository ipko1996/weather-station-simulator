package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ipko1996/huweathersim/pkg/registry"
)

// fakeStore is an in-memory SensorStore. This is the payoff of the
// consumer-side interface: handler tests run against a map instead of Redis,
// so they need no containers and finish in microseconds. It mirrors the real
// registry's contract — including validate-on-Add — because the handlers rely
// on that behavior for their 400s.
type fakeStore struct {
	sensors map[string]registry.Sensor
}

func newFakeStore() *fakeStore {
	return &fakeStore{sensors: make(map[string]registry.Sensor)}
}

func (f *fakeStore) Add(_ context.Context, s registry.Sensor) error {
	// Mirrors registry.Registry.Add's wrapping exactly: the handler's
	// 400-vs-500 split hinges on ErrInvalidSensor being in the chain, so the
	// fake must reproduce that part of the contract faithfully.
	if err := s.Validate(); err != nil {
		return fmt.Errorf("%w: %w", registry.ErrInvalidSensor, err)
	}
	f.sensors[s.ID] = s
	return nil
}

func (f *fakeStore) Remove(_ context.Context, id string) (bool, error) {
	_, ok := f.sensors[id]
	delete(f.sensors, id)
	return ok, nil
}

func (f *fakeStore) Get(_ context.Context, id string) (registry.Sensor, error) {
	s, ok := f.sensors[id]
	if !ok {
		return registry.Sensor{}, fmt.Errorf("sensor %s: %w", id, registry.ErrNotFound)
	}
	return s, nil
}

func (f *fakeStore) List(_ context.Context) ([]registry.Sensor, error) {
	out := make([]registry.Sensor, 0, len(f.sensors))
	for _, s := range f.sensors {
		out = append(out, s)
	}
	return out, nil
}

// do drives one request through the full router (middleware included) and
// returns the recorded response.
func do(t *testing.T, router http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestCreateSensor(t *testing.T) {
	t.Run("minimal body gets defaults and a generated id", func(t *testing.T) {
		store := newFakeStore()
		router := NewRouter(store)

		rec := do(t, router, http.MethodPost, "/api/sensors", `{"lat":47.4979,"lon":19.0402}`)

		if rec.Code != http.StatusCreated {
			t.Fatalf("status: got %d, want %d (body: %s)", rec.Code, http.StatusCreated, rec.Body)
		}
		var got sensorResponse
		if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if !strings.HasPrefix(got.ID, "sensor-") || len(got.ID) != len("sensor-")+8 {
			t.Errorf("id: got %q, want sensor-<8 hex chars>", got.ID)
		}
		if got.StartTempC != defaultStartTempC {
			t.Errorf("start_temp_c: got %v, want default %v", got.StartTempC, defaultStartTempC)
		}
		if got.Pattern != string(defaultPattern) {
			t.Errorf("pattern: got %q, want default %q", got.Pattern, defaultPattern)
		}
		if got.Interval != defaultInterval.String() {
			t.Errorf("interval: got %q, want default %q", got.Interval, defaultInterval)
		}
		if _, ok := store.sensors[got.ID]; !ok {
			t.Errorf("sensor %s not in the store after 201", got.ID)
		}
	})

	t.Run("explicit zero temperature is not overwritten by the default", func(t *testing.T) {
		// The reason StartTempC is a *float64 in the request type.
		router := NewRouter(newFakeStore())

		rec := do(t, router, http.MethodPost, "/api/sensors",
			`{"lat":47.4979,"lon":19.0402,"start_temp_c":0}`)

		if rec.Code != http.StatusCreated {
			t.Fatalf("status: got %d, want %d (body: %s)", rec.Code, http.StatusCreated, rec.Body)
		}
		var got sensorResponse
		if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if got.StartTempC != 0 {
			t.Errorf("start_temp_c: got %v, want explicit 0", got.StartTempC)
		}
	})

	// Table of rejects: every 400 must carry the offending rule's message so
	// the map UI (Phase 3) can show it to the user verbatim.
	rejects := []struct {
		name     string
		body     string
		wantErrs string // substring expected in the error text
	}{
		{"not json", `{lat: nope}`, "invalid JSON"},
		{"outside Hungary", `{"lat":52.52,"lon":13.405}`, "outside Hungary"}, // Berlin
		{"missing coordinates", `{}`, "outside Hungary"},                    // (0,0) is in the Atlantic
		{"unknown pattern", `{"lat":47.5,"lon":19.05,"pattern":"sideways"}`, "pattern"},
		{"malformed interval", `{"lat":47.5,"lon":19.05,"interval":"fast"}`, "duration"},
		{"interval out of range", `{"lat":47.5,"lon":19.05,"interval":"45s"}`, "interval"},
	}
	for _, tc := range rejects {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			store := newFakeStore()
			router := NewRouter(store)

			rec := do(t, router, http.MethodPost, "/api/sensors", tc.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status: got %d, want %d (body: %s)", rec.Code, http.StatusBadRequest, rec.Body)
			}
			var got errorResponse
			if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
				t.Fatalf("decode error body: %v", err)
			}
			if !strings.Contains(got.Error, tc.wantErrs) {
				t.Errorf("error text %q does not mention %q", got.Error, tc.wantErrs)
			}
			if len(store.sensors) != 0 {
				t.Errorf("store not empty after a rejected create")
			}
		})
	}
}

// downStore simulates the registry when Redis is unreachable: every call
// fails with an infrastructure error that is NOT ErrInvalidSensor.
type downStore struct{}

func (downStore) Add(context.Context, registry.Sensor) error {
	return fmt.Errorf("store sensor: connection refused")
}
func (downStore) Remove(context.Context, string) (bool, error) {
	return false, fmt.Errorf("remove sensor: connection refused")
}
func (downStore) Get(context.Context, string) (registry.Sensor, error) {
	return registry.Sensor{}, fmt.Errorf("get sensor: connection refused")
}
func (downStore) List(context.Context) ([]registry.Sensor, error) {
	return nil, fmt.Errorf("list sensors: connection refused")
}

// TestStoreDownIs500 pins the 400-vs-500 contract: an unreachable store is OUR
// failure, and answering 400 would tell clients "your request is bad, don't
// retry" about requests that would succeed once Redis is back.
func TestStoreDownIs500(t *testing.T) {
	router := NewRouter(downStore{})

	cases := []struct {
		method, path, body string
	}{
		{http.MethodPost, "/api/sensors", `{"lat":47.4979,"lon":19.0402}`}, // valid body!
		{http.MethodGet, "/api/sensors", ""},
		{http.MethodGet, "/api/sensors/sensor-x", ""},
		{http.MethodDelete, "/api/sensors/sensor-x", ""},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rec := do(t, router, tc.method, tc.path, tc.body)

			if rec.Code != http.StatusInternalServerError {
				t.Errorf("status: got %d, want %d (body: %s)", rec.Code, http.StatusInternalServerError, rec.Body)
			}
		})
	}
}

func TestGetSensor(t *testing.T) {
	store := newFakeStore()
	router := NewRouter(store)

	rec := do(t, router, http.MethodPost, "/api/sensors", `{"lat":47.4979,"lon":19.0402}`)
	var created sensorResponse
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}

	t.Run("existing sensor round-trips", func(t *testing.T) {
		rec := do(t, router, http.MethodGet, "/api/sensors/"+created.ID, "")

		if rec.Code != http.StatusOK {
			t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
		}
		var got sensorResponse
		if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if got != created {
			t.Errorf("GET differs from POST response:\ngot  %+v\nwant %+v", got, created)
		}
	})

	t.Run("missing sensor is 404", func(t *testing.T) {
		rec := do(t, router, http.MethodGet, "/api/sensors/sensor-ghost", "")

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status: got %d, want %d", rec.Code, http.StatusNotFound)
		}
	})
}

func TestListSensors(t *testing.T) {
	store := newFakeStore()
	router := NewRouter(store)

	t.Run("empty registry lists as [] not null", func(t *testing.T) {
		rec := do(t, router, http.MethodGet, "/api/sensors", "")

		if rec.Code != http.StatusOK {
			t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
		}
		if body := strings.TrimSpace(rec.Body.String()); body != "[]" {
			t.Errorf("body: got %s, want []", body)
		}
	})

	t.Run("lists what was created", func(t *testing.T) {
		do(t, router, http.MethodPost, "/api/sensors", `{"lat":47.4979,"lon":19.0402}`)
		do(t, router, http.MethodPost, "/api/sensors", `{"lat":46.253,"lon":20.1414}`) // Szeged

		rec := do(t, router, http.MethodGet, "/api/sensors", "")

		var got []sensorResponse
		if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if len(got) != 2 {
			t.Errorf("listed %d sensors, want 2", len(got))
		}
	})
}

func TestDeleteSensor(t *testing.T) {
	store := newFakeStore()
	router := NewRouter(store)

	rec := do(t, router, http.MethodPost, "/api/sensors", `{"lat":47.4979,"lon":19.0402}`)
	var created sensorResponse
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}

	t.Run("existing sensor deletes with 204", func(t *testing.T) {
		rec := do(t, router, http.MethodDelete, "/api/sensors/"+created.ID, "")

		if rec.Code != http.StatusNoContent {
			t.Fatalf("status: got %d, want %d", rec.Code, http.StatusNoContent)
		}
		if len(store.sensors) != 0 {
			t.Errorf("sensor still in store after 204")
		}
	})

	t.Run("deleting it again is 404", func(t *testing.T) {
		rec := do(t, router, http.MethodDelete, "/api/sensors/"+created.ID, "")

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status: got %d, want %d", rec.Code, http.StatusNotFound)
		}
	})
}
