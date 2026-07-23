// Package consumer holds what the ingestion-consumer service does with each
// reading it receives.
//
// Phase 1 just logged. Phase 2 turns it into the real pipeline stage: persist
// to TimescaleDB (re-publishing to sensor.readings.clean lands here next).
// Keeping it behind a small type means those changes land here and nowhere
// else.
package consumer

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/ipko1996/huweathersim/pkg/events"
)

// Stats tracks what this consumer has processed. The counter is atomic because
// Phase 2 runs several handler goroutines at once, and a plain int64 incremented
// from multiple goroutines is a data race — one of the first concurrency bugs
// Go's race detector will catch (`go test -race`).
type Stats struct {
	processed atomic.Int64
}

// Processed returns how many readings have been handled.
func (s *Stats) Processed() int64 {
	return s.processed.Load()
}

// ReadingWriter is what the pipeline needs from storage — declared by the
// consumer (this package), satisfied implicitly by store.Store, faked by a
// map in tests. Same idiom as the gateway's SensorStore.
type ReadingWriter interface {
	InsertReading(ctx context.Context, r events.SensorReading) error
}

// Publisher is what the pipeline needs for the clean topic — satisfied by
// *kafkax.Producer, faked in tests.
type Publisher interface {
	Publish(ctx context.Context, r events.SensorReading) error
}

// Pipeline is the per-reading work: persist, re-publish clean, count.
type Pipeline struct {
	db    ReadingWriter
	clean Publisher
	stats Stats
}

func NewPipeline(db ReadingWriter, clean Publisher) *Pipeline {
	return &Pipeline{db: db, clean: clean}
}

// Processed exposes the underlying counter for the shutdown log.
func (p *Pipeline) Processed() int64 {
	return p.stats.Processed()
}

// Handle persists one reading.
//
// Every error returned here is treated as TRANSIENT, and that's a designed
// invariant, not an assumption: permanently-bad messages (unparseable JSON,
// failed validation) never reach this handler — kafkax filters them out
// before it. So a failure here means infrastructure (database down, network),
// and the full recovery chain is:
//
//	return err → offset NOT committed → Run returns → process exits
//	→ restart (compose restart policy) → Kafka redelivers
//	→ InsertReading's ON CONFLICT absorbs any half-done work
//
// Crash-restart-redeliver IS the retry mechanism — no retry loop in code.
func (p *Pipeline) Handle(ctx context.Context, r events.SensorReading) error {
	if err := p.db.InsertReading(ctx, r); err != nil {
		return err
	}

	// Insert and publish are NOT atomic, and that's a documented trade-off
	// rather than a bug: a crash between them means redelivery, the insert
	// dedupes via ON CONFLICT, but the reading is published to the clean
	// topic a second time. Downstream must tolerate occasional duplicates —
	// the 10s-window average genuinely doesn't care — and in exchange we skip
	// Kafka transactions entirely, which would cost more complexity than this
	// pipeline's guarantees are worth.
	if err := p.clean.Publish(ctx, r); err != nil {
		return err
	}

	n := p.stats.processed.Add(1)
	// Per-reading logging is deliberate while building the pipeline — seeing
	// every message IS the point right now. At the Phase 6 stress rate
	// (2,000 msg/s) this must become sampled or structured-and-leveled, or
	// the firehose drowns every useful line.
	log.Printf("reading #%d stored: sensor=%s temp=%.1f°C ts=%s",
		n, r.SensorID, r.TempC, r.Time.Format("15:04:05"))
	return nil
}
