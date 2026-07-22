// Package consumer holds what the ingestion-consumer service does with each
// reading it receives.
//
// In Phase 1 that's just logging. Phase 2 adds the real work: writing to
// TimescaleDB and re-publishing a normalized event to sensor.readings.clean.
// Keeping it behind a small function now means that change lands here and
// nowhere else.
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

// Handler logs each reading and counts it.
//
// It returns an error type matching kafkax.Handler, and the distinction matters:
// returning an error means "retryable — do not commit the offset", so the
// message is redelivered. Logging can't fail, so Phase 1 always returns nil;
// Phase 2's database write is the first thing that can genuinely fail here.
func (s *Stats) Handler(_ context.Context, r events.SensorReading) error {
	n := s.processed.Add(1)

	log.Printf("reading #%d: sensor=%s temp=%.1f°C at (%.4f, %.4f) ts=%s",
		n, r.SensorID, r.TempC, r.Lat, r.Lon, r.Time.Format("15:04:05"))

	return nil
}
