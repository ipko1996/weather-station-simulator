// Package httpapi is the notification-gateway's HTTP surface: a health check
// and the /ws endpoint where browsers subscribe to live aggregates.
package httpapi

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/ipko1996/huweathersim/services/notification-gateway/internal/hub"
)

// NewRouter builds the handler. Same chi shape as the sensor-gateway — but
// note there is NO Timeout middleware here: a WebSocket is a deliberately
// long-lived connection, and a request timeout would cut every client off
// mid-stream at the deadline.
func NewRouter(h *hub.Hub) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", handleHealthz)
	r.Get("/ws", handleWS(h))

	return r
}

// handleWS upgrades the HTTP request to a WebSocket and streams hub messages
// to it until either side disconnects.
func handleWS(h *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Accept performs the upgrade handshake: this request stops being
		// HTTP here and becomes a two-way frame stream.
		//
		// OriginPatterns "*" disables the browser same-origin check for
		// Phase 2 — websocat and curl don't send an Origin, and the React
		// dev server (Phase 3) runs on another port. It MUST be tightened to
		// the real frontend origin when one exists: a wildcard in production
		// lets any website a user visits open a socket here in their name.
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: []string{"*"},
		})
		if err != nil {
			// Accept already wrote an HTTP error to the client.
			log.Printf("ws accept: %v", err)
			return
		}
		defer conn.CloseNow()

		// This service only pushes; clients send nothing. CloseRead spawns
		// the mandatory reader (control frames — pings, close — must be
		// consumed or the connection jams) and returns a context that
		// cancels when the client goes away. Skipping this is the classic
		// coder/websocket mistake.
		ctx := conn.CloseRead(r.Context())

		ch := h.Register()
		defer h.Unregister(ch)
		log.Printf("client connected (%d total)", h.ClientCount())

		for {
			select {
			case <-ctx.Done():
				log.Printf("client disconnected (%d left)", h.ClientCount()-1)
				return
			case msg, ok := <-ch:
				if !ok {
					return // hub closed us (shutdown)
				}
				// A per-write deadline so one wedged connection can't hold
				// this goroutine hostage — the write-side twin of the hub's
				// drop-on-full rule.
				writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := conn.Write(writeCtx, websocket.MessageText, msg)
				cancel()
				if err != nil {
					log.Printf("ws write: %v", err)
					return
				}
			}
		}
	}
}

// healthResponse mirrors the other services' /healthz shape.
type healthResponse struct {
	Status  string    `json:"status"`
	Service string    `json:"service"`
	Time    time.Time `json:"time"`
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(healthResponse{
		Status:  "ok",
		Service: "notification-gateway",
		Time:    time.Now().UTC(),
	})
}
