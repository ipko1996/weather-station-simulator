// Package hub is an in-memory broadcast switchboard: one Broadcast in, a copy
// out to every registered client. It knows nothing about WebSockets or Kafka
// — it moves []byte between goroutines, which is what makes it trivially
// unit-testable.
package hub

import (
	"sync"
	"sync/atomic"
)

// clientBuffer is each client's queue length. Small on purpose: aggregates
// arrive every ~10s, so even a briefly stalled client catches up from a
// couple of slots, and anything deeper would just hold stale data.
const clientBuffer = 16

// Hub fans messages out to registered clients.
//
// The cardinal rule, and the reason Broadcast never blocks: one slow client
// must never stall the others. A phone with a dead connection can take
// minutes to time out — if Broadcast did a blocking send to its channel,
// every other browser would freeze for exactly that long. Instead a full
// client buffer means that client skips this message (counted in dropped);
// the next window's aggregate is 10s away anyway.
type Hub struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}

	// dropped counts skipped deliveries — atomic so Phase 5 metrics can read
	// it without the lock.
	dropped atomic.Int64
}

func New() *Hub {
	return &Hub{clients: make(map[chan []byte]struct{})}
}

// Register adds a client and returns the channel its messages will arrive on.
// The caller (the WS handler) reads from it until Unregister closes it.
func (h *Hub) Register() chan []byte {
	ch := make(chan []byte, clientBuffer)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[ch] = struct{}{}
	return ch
}

// Unregister removes a client and closes its channel, which ends the client's
// write loop. Removing from the map and closing happen under the same lock
// Broadcast takes, so Broadcast can never send on a closed channel — that
// ordering is what makes closing safe at all.
func (h *Hub) Unregister(ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[ch]; !ok {
		return // double-unregister (e.g. handler defer after an error path)
	}
	delete(h.clients, ch)
	close(ch)
}

// Broadcast delivers msg to every client that has room, without ever
// blocking. The select-with-default is the same "send only if possible" idiom
// as the manager's Kick.
func (h *Hub) Broadcast(msg []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
			h.dropped.Add(1)
		}
	}
}

// ClientCount reports connected clients — the number this service will scale
// on in Phase 6 (connection count, not message volume: the architectural
// reason it's separate from aggregation at all, PROJECT.md §4).
func (h *Hub) ClientCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// Dropped reports how many deliveries were skipped due to full client buffers.
func (h *Hub) Dropped() int64 {
	return h.dropped.Load()
}
