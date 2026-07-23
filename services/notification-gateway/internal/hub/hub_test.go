package hub

import (
	"fmt"
	"testing"
	"time"
)

func TestBroadcastReachesAllClients(t *testing.T) {
	h := New()
	a, b := h.Register(), h.Register()
	defer h.Unregister(a)
	defer h.Unregister(b)

	h.Broadcast([]byte("hello"))

	for name, ch := range map[string]chan []byte{"a": a, "b": b} {
		select {
		case msg := <-ch:
			if string(msg) != "hello" {
				t.Errorf("client %s: got %q, want hello", name, msg)
			}
		case <-time.After(time.Second):
			t.Errorf("client %s never received the broadcast", name)
		}
	}
}

// TestSlowClientDoesNotBlockBroadcast is the hub's cardinal rule under test:
// a client that never reads gets messages DROPPED, while a healthy client
// keeps receiving and Broadcast itself never stalls.
func TestSlowClientDoesNotBlockBroadcast(t *testing.T) {
	h := New()
	slow := h.Register() // never read from
	defer h.Unregister(slow)
	healthy := h.Register()
	defer h.Unregister(healthy)

	// Overflow the slow client's buffer with room to spare. If Broadcast
	// ever blocked on the full channel, this loop would hang and the test
	// would time out — passing at all proves non-blocking delivery.
	const sends = clientBuffer * 3
	for i := range sends {
		h.Broadcast(fmt.Appendf(nil, "msg-%d", i))
		// Keep the healthy client drained so only slow's buffer fills.
		select {
		case <-healthy:
		case <-time.After(time.Second):
			t.Fatalf("healthy client starved at message %d", i)
		}
	}

	if got := h.Dropped(); got != sends-clientBuffer {
		t.Errorf("dropped: got %d, want %d (slow client's overflow)", got, sends-clientBuffer)
	}
}

func TestUnregisterClosesChannelAndIsIdempotent(t *testing.T) {
	h := New()
	ch := h.Register()

	h.Unregister(ch)
	if _, ok := <-ch; ok {
		t.Error("channel still open after Unregister")
	}
	h.Unregister(ch) // second call must be a no-op, not a double-close panic

	if got := h.ClientCount(); got != 0 {
		t.Errorf("client count: got %d, want 0", got)
	}
}
