package consumer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ipko1996/huweathersim/pkg/events"
)

func testReading(id string) events.SensorReading {
	return events.SensorReading{
		SensorID: id,
		Lat:      47.4979,
		Lon:      19.0402,
		TempC:    21.5,
		Time:     time.Now().UTC(),
	}
}

func TestHandlerCountsReadings(t *testing.T) {
	var s Stats
	ctx := context.Background()

	if got := s.Processed(); got != 0 {
		t.Fatalf("initial count: got %d, want 0", got)
	}

	for i := range 5 {
		if err := s.Handler(ctx, testReading("sensor-0001")); err != nil {
			t.Fatalf("reading %d: unexpected error %v", i, err)
		}
	}

	if got := s.Processed(); got != 5 {
		t.Errorf("processed: got %d, want 5", got)
	}
}

// TestHandlerIsConcurrencySafe runs many handlers at once and checks the count
// is exact. Run with `go test -race` and this fails loudly if the counter is
// ever changed from atomic.Int64 to a plain int64 — which is the whole reason
// it's atomic. Phase 2 depends on this when it fans out across partitions.
func TestHandlerIsConcurrencySafe(t *testing.T) {
	var s Stats
	ctx := context.Background()

	const goroutines, perGoroutine = 20, 50

	// WaitGroup is Go's "wait for N goroutines to finish" primitive: Add before
	// launching, Done when each completes, Wait blocks until the count hits zero.
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perGoroutine {
				_ = s.Handler(ctx, testReading("sensor-0001"))
			}
		}()
	}
	wg.Wait()

	if want := int64(goroutines * perGoroutine); s.Processed() != want {
		t.Errorf("processed: got %d, want %d (lost updates indicate a data race)",
			s.Processed(), want)
	}
}
