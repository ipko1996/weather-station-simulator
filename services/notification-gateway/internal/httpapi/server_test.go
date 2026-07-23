package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ipko1996/huweathersim/services/notification-gateway/internal/hub"
)

func TestHealthz(t *testing.T) {
	router := NewRouter(hub.New())
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
}

// TestWebSocketReceivesBroadcast runs the REAL upgrade handshake: httptest
// starts a listening server (NewRecorder can't upgrade a connection — the
// hijacked TCP stream needs to exist), websocket.Dial connects to it, and a
// hub broadcast must arrive as a frame.
func TestWebSocketReceivesBroadcast(t *testing.T) {
	h := hub.New()
	srv := httptest.NewServer(NewRouter(h))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	// The handler registers with the hub during the handshake, but Dial
	// returns as soon as the client side is ready — poll until the server
	// side shows up rather than sleeping a guessed amount.
	for start := time.Now(); h.ClientCount() == 0; {
		if time.Since(start) > 5*time.Second {
			t.Fatal("server never registered the client with the hub")
		}
		time.Sleep(5 * time.Millisecond)
	}

	h.Broadcast([]byte(`{"scope":"national"}`))

	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != websocket.MessageText {
		t.Errorf("message type: got %v, want text", typ)
	}
	if string(data) != `{"scope":"national"}` {
		t.Errorf("payload: got %s", data)
	}
}

// TestWebSocketDisconnectUnregisters: closing the client must remove it from
// the hub, or dead connections would pile up forever.
func TestWebSocketDisconnectUnregisters(t *testing.T) {
	h := hub.New()
	srv := httptest.NewServer(NewRouter(h))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	for start := time.Now(); h.ClientCount() == 0; {
		if time.Since(start) > 5*time.Second {
			t.Fatal("client never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := conn.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("close: %v", err)
	}

	for start := time.Now(); h.ClientCount() != 0; {
		if time.Since(start) > 5*time.Second {
			t.Fatalf("client still registered %d after close", h.ClientCount())
		}
		time.Sleep(5 * time.Millisecond)
	}
}
