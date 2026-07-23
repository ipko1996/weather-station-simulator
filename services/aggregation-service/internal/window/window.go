// Package window is a hand-rolled tumbling-window aggregator.
//
// Hand-rolled is a deliberate choice over a stream-processing framework
// (Flink, Kafka Streams): at our scale the whole mechanism is a map, a mutex
// and ~100 lines, and building it exposes the one distinction those
// frameworks exist to manage — event time versus processing time. A reading
// carries WHEN IT WAS MEASURED (event time, the ts field); it arrives at this
// service some time later (processing time). Readings are bucketed by event
// time, so a reading delayed by two seconds still counts toward the window in
// which it was measured — but windows must eventually close by the WALL
// CLOCK, or a single silent sensor would hold its window open forever. The
// gap between the two is bridged by a grace period (allowedLateness): a
// poor-man's watermark, and honestly labeled as such.
package window

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipko1996/huweathersim/pkg/events"
)

// Scope of every aggregate this Tumbler produces. Regional windows (per
// county) are a later feature: they'll bucket by (scope, start) instead.
const scopeNational = "national"

// bucket accumulates one window's readings. Only sums are kept — the average
// is derived at close, so memory per window is constant regardless of rate.
type bucket struct {
	sum, min, max float64
	count         int
}

// Tumbler assigns readings to fixed, non-overlapping windows of `size`
// ("tumbling": 12:00:00–10, 12:00:10–20, ... — each reading belongs to
// exactly one) and closes a window once the wall clock passes its end plus
// the grace period.
//
// It is shared by two goroutines with different jobs — the Kafka consumer
// calls Add, a ticker goroutine calls Closed — so the buckets map is guarded
// by a mutex. This is the project's first genuinely shared mutable state:
// the manager's map had a single owner, the Stats counter was atomic, but a
// map written and harvested by different goroutines needs a lock. (Delete
// the Lock calls and `make test-race` fails immediately — try it.)
type Tumbler struct {
	size     time.Duration
	lateness time.Duration

	mu      sync.Mutex
	buckets map[int64]*bucket // key: window start, unix seconds

	// late counts dropped readings. Atomic rather than mutex-guarded so a
	// metrics endpoint (Phase 5) can read it without touching the lock.
	late atomic.Int64
}

// New builds a Tumbler. size is the window length (PROJECT.md: 10s), and
// allowedLateness is how long after a window's end it still accepts readings.
func New(size, allowedLateness time.Duration) *Tumbler {
	return &Tumbler{
		size:     size,
		lateness: allowedLateness,
		buckets:  make(map[int64]*bucket),
	}
}

// Add buckets one reading by its EVENT time, judged against `now` (wall
// clock). It reports false when the reading is late — its window already
// closed — in which case it was dropped and counted, not stored: a live
// average must not rewrite the past, and the durable history in TimescaleDB
// is complete regardless.
//
// `now` is a parameter rather than time.Now() inside, so tests control the
// clock exactly — no sleeps, no flakes.
func (t *Tumbler) Add(r events.SensorReading, now time.Time) bool {
	// Truncate rounds DOWN to the window boundary: 12:00:03 → 12:00:00. A
	// reading exactly on a boundary (12:00:10.000) truncates to itself and
	// thus opens the LATER window — boundaries belong to the window they
	// start, never the one they end.
	start := r.Time.Truncate(t.size)

	// The window's close moment is end + grace. If the wall clock is already
	// past that, Closed() has (or will have) emitted it — this reading is late.
	if !now.Before(start.Add(t.size + t.lateness)) {
		t.late.Add(1)
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	b, ok := t.buckets[start.Unix()]
	if !ok {
		b = &bucket{min: math.Inf(1), max: math.Inf(-1)}
		t.buckets[start.Unix()] = b
	}
	b.sum += r.TempC
	b.count++
	b.min = math.Min(b.min, r.TempC)
	b.max = math.Max(b.max, r.TempC)
	return true
}

// Closed removes and returns every window whose grace period has expired at
// `now`. Empty windows produce nothing: a system with no sensors stays
// silent rather than publishing averages of zero readings.
func (t *Tumbler) Closed(now time.Time) []events.WindowAggregate {
	t.mu.Lock()
	defer t.mu.Unlock()

	var out []events.WindowAggregate
	for startUnix, b := range t.buckets {
		start := time.Unix(startUnix, 0).UTC()
		if now.Before(start.Add(t.size + t.lateness)) {
			continue // still open
		}

		out = append(out, events.WindowAggregate{
			Scope:       scopeNational,
			WindowStart: start,
			WindowEnd:   start.Add(t.size),
			// Two decimals: readings carry one, so the mean earns one more —
			// beyond that is noise pretending to be precision.
			AvgTempC: math.Round(b.sum/float64(b.count)*100) / 100,
			MinTempC: b.min,
			MaxTempC: b.max,
			Count:    b.count,
		})
		delete(t.buckets, startUnix)
	}
	return out
}

// LateCount reports how many readings have been dropped as late so far.
func (t *Tumbler) LateCount() int64 {
	return t.late.Load()
}
