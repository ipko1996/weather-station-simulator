package consumer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

// fakeWriter satisfies ReadingWriter in memory — the same consumer-side
// interface payoff as everywhere else: pipeline tests with no database. The
// counter is atomic because the concurrency test hammers it from many
// goroutines.
type fakeWriter struct {
	inserted atomic.Int64
	failWith error // when set, every insert fails with it
}

func (f *fakeWriter) InsertReading(context.Context, events.SensorReading) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.inserted.Add(1)
	return nil
}

// fakePublisher satisfies Publisher in memory, mirroring fakeWriter's shape.
type fakePublisher struct {
	published atomic.Int64
	failWith  error
}

func (f *fakePublisher) Publish(context.Context, events.SensorReading) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.published.Add(1)
	return nil
}

func TestPipelineStoresAndCounts(t *testing.T) {
	db, pub := &fakeWriter{}, &fakePublisher{}
	p := NewPipeline(db, pub)
	ctx := context.Background()

	if got := p.Processed(); got != 0 {
		t.Fatalf("initial count: got %d, want 0", got)
	}

	for i := range 5 {
		if err := p.Handle(ctx, testReading("sensor-0001")); err != nil {
			t.Fatalf("reading %d: unexpected error %v", i, err)
		}
	}

	if got := p.Processed(); got != 5 {
		t.Errorf("processed: got %d, want 5", got)
	}
	if got := db.inserted.Load(); got != 5 {
		t.Errorf("inserted: got %d, want 5", got)
	}
	if got := pub.published.Load(); got != 5 {
		t.Errorf("published to clean topic: got %d, want 5", got)
	}
}

// TestPipelinePropagatesPublishErrors: a failed clean-topic publish must also
// block the offset commit. The insert already succeeded — on redelivery it
// becomes an ON CONFLICT no-op and only the publish is effectively retried.
func TestPipelinePropagatesPublishErrors(t *testing.T) {
	kafkaDown := errors.New("broker unreachable")
	db := &fakeWriter{}
	p := NewPipeline(db, &fakePublisher{failWith: kafkaDown})

	err := p.Handle(context.Background(), testReading("sensor-0001"))
	if !errors.Is(err, kafkaDown) {
		t.Fatalf("Handle: got %v, want the publisher's error", err)
	}
	if got := db.inserted.Load(); got != 1 {
		t.Errorf("inserted: got %d, want 1 (insert happens before publish)", got)
	}
	if got := p.Processed(); got != 0 {
		t.Errorf("processed after failed publish: got %d, want 0", got)
	}
}

// TestPipelinePropagatesStoreErrors pins the at-least-once contract: a failed
// insert must surface as a handler error (so the offset is NOT committed and
// the reading is redelivered), and must not be counted as processed.
func TestPipelinePropagatesStoreErrors(t *testing.T) {
	dbDown := errors.New("connection refused")
	p := NewPipeline(&fakeWriter{failWith: dbDown}, &fakePublisher{})

	err := p.Handle(context.Background(), testReading("sensor-0001"))
	if !errors.Is(err, dbDown) {
		t.Fatalf("Handle: got %v, want the store's error", err)
	}
	if got := p.Processed(); got != 0 {
		t.Errorf("processed after failed insert: got %d, want 0", got)
	}
}

// TestPipelineIsConcurrencySafe runs many handlers at once and checks the
// count is exact. Run with `go test -race` and this fails loudly if the
// counter is ever changed from atomic.Int64 to a plain int64 — which is the
// whole reason it's atomic, now that handlers fan out across partitions.
func TestPipelineIsConcurrencySafe(t *testing.T) {
	db, pub := &fakeWriter{}, &fakePublisher{}
	p := NewPipeline(db, pub)
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
				_ = p.Handle(ctx, testReading("sensor-0001"))
			}
		}()
	}
	wg.Wait()

	if want := int64(goroutines * perGoroutine); p.Processed() != want {
		t.Errorf("processed: got %d, want %d (lost updates indicate a data race)",
			p.Processed(), want)
	}
}
