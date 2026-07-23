package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHealthz drives the /healthz endpoint through the real router, entirely in
// memory — no server, no port, no network. httptest gives us a fake request and a
// ResponseRecorder that captures whatever the handler writes.
func TestHealthz(t *testing.T) {
	router := NewRouter()

	t.Run("GET returns 200 and ok status", func(t *testing.T) {
		// Build a fake GET request and a recorder to capture the response.
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()

		// Call the handler directly. This runs the exact routing + handler code.
		router.ServeHTTP(rec, req)

		// 1) status code
		if rec.Code != http.StatusOK {
			t.Errorf("status code: got %d, want %d", rec.Code, http.StatusOK)
		}

		// 2) content type
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type: got %q, want %q", ct, "application/json")
		}

		// 3) body: decode the JSON and check the fields we care about
		var body healthResponse
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			// t.Fatalf stops this subtest immediately — no point checking fields
			// on a body we couldn't even parse.
			t.Fatalf("decode body: %v", err)
		}
		if body.Status != "ok" {
			t.Errorf("status field: got %q, want %q", body.Status, "ok")
		}
		if body.Service != "sensor-gateway" {
			t.Errorf("service field: got %q, want %q", body.Service, "sensor-gateway")
		}
	})

	t.Run("wrong method returns 405", func(t *testing.T) {
		// The route is registered as GET only, so a POST must be rejected.
		// chi distinguishes "path exists but method doesn't" (405) from "path
		// doesn't exist at all" (404) — worth pinning both in tests, because
		// routers differ on this and clients rely on the distinction.
		req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status code: got %d, want %d", rec.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("unknown route returns 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/nope", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("status code: got %d, want %d", rec.Code, http.StatusNotFound)
		}
	})
}
