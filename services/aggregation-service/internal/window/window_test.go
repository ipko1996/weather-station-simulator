package window

import (
	"sync"
	"testing"
	"time"

	"github.com/ipko1996/huweathersim/pkg/events"
)

const (
	size  = 10 * time.Second
	grace = 2 * time.Second
)

// base is an exact window boundary (…:00), so offsets in tests read naturally:
// base+3s is inside window [base, base+10s).
var base = time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

func reading(ts time.Time, temp float64) events.SensorReading {
	return events.SensorReading{
		SensorID: "sensor-0001",
		Lat:      47.4979,
		Lon:      19.0402,
		TempC:    temp,
		Time:     ts,
	}
}

func TestTumblerAggregatesTwoWindows(t *testing.T) {
	tb := New(size, grace)
	now := base // wall clock sits at the first window's start — nothing is late

	// Window 1 [12:00:00, 12:00:10): three readings.
	for _, r := range []events.SensorReading{
		reading(base.Add(1*time.Second), 20.0),
		reading(base.Add(5*time.Second), 22.0),
		reading(base.Add(9*time.Second), 21.0),
	} {
		if !tb.Add(r, now) {
			t.Fatalf("reading at %s rejected as late", r.Time)
		}
	}
	// Window 2 [12:00:10, 12:00:20): one reading.
	if !tb.Add(reading(base.Add(11*time.Second), 30.0), now) {
		t.Fatal("window-2 reading rejected as late")
	}

	// Nothing may close before end+grace.
	if got := tb.Closed(base.Add(11 * time.Second)); len(got) != 0 {
		t.Fatalf("windows closed prematurely: %+v", got)
	}

	// At 12:00:12 (end 12:00:10 + grace 2s) window 1 closes; window 2 stays.
	closed := tb.Closed(base.Add(12 * time.Second))
	if len(closed) != 1 {
		t.Fatalf("closed windows: got %d, want 1", len(closed))
	}
	w := closed[0]
	if !w.WindowStart.Equal(base) || !w.WindowEnd.Equal(base.Add(size)) {
		t.Errorf("window bounds: got [%s, %s), want [%s, %s)", w.WindowStart, w.WindowEnd, base, base.Add(size))
	}
	if w.Count != 3 {
		t.Errorf("count: got %d, want 3", w.Count)
	}
	if w.AvgTempC != 21.0 {
		t.Errorf("avg: got %v, want 21.0", w.AvgTempC)
	}
	if w.MinTempC != 20.0 || w.MaxTempC != 22.0 {
		t.Errorf("min/max: got %v/%v, want 20.0/22.0", w.MinTempC, w.MaxTempC)
	}
	if w.Scope != "national" {
		t.Errorf("scope: got %q, want national", w.Scope)
	}
	if err := w.Validate(); err != nil {
		t.Errorf("emitted aggregate fails its own validation: %v", err)
	}

	// Window 2 closes at 12:00:22.
	closed = tb.Closed(base.Add(22 * time.Second))
	if len(closed) != 1 || closed[0].Count != 1 || closed[0].AvgTempC != 30.0 {
		t.Errorf("window 2: got %+v, want 1 window with count 1 avg 30.0", closed)
	}
}

// TestTumblerBoundaryReading pins the boundary rule: a reading exactly at
// 12:00:10.000 belongs to the window STARTING there, not the one ending there.
func TestTumblerBoundaryReading(t *testing.T) {
	tb := New(size, grace)

	tb.Add(reading(base.Add(size), 25.0), base.Add(size)) // ts exactly 12:00:10

	// Window 1 closes empty → no aggregate. Window 2 must hold the reading.
	if got := tb.Closed(base.Add(12 * time.Second)); len(got) != 0 {
		t.Fatalf("boundary reading landed in the earlier window: %+v", got)
	}
	got := tb.Closed(base.Add(22 * time.Second))
	if len(got) != 1 || got[0].Count != 1 {
		t.Fatalf("boundary reading missing from the later window: %+v", got)
	}
}

// TestTumblerDropsLateReadings: a reading whose window already closed must be
// dropped and counted — the live average never rewrites the past.
func TestTumblerDropsLateReadings(t *testing.T) {
	tb := New(size, grace)

	// Wall clock at 12:00:13 — window [12:00:00, 12:00:10) closed at :12.
	now := base.Add(13 * time.Second)
	if tb.Add(reading(base.Add(5*time.Second), 20.0), now) {
		t.Fatal("late reading accepted into a closed window")
	}
	if got := tb.LateCount(); got != 1 {
		t.Errorf("late count: got %d, want 1", got)
	}

	// The grace period is exactly why a 2s-delayed reading is NOT late: at
	// 12:00:11 the same window is past its end but within grace.
	tb2 := New(size, grace)
	if !tb2.Add(reading(base.Add(9*time.Second), 20.0), base.Add(11*time.Second)) {
		t.Error("reading within the grace period rejected")
	}
}

// TestTumblerEmptyWindowsEmitNothing: silence in, silence out.
func TestTumblerEmptyWindowsEmitNothing(t *testing.T) {
	tb := New(size, grace)
	if got := tb.Closed(base.Add(time.Hour)); len(got) != 0 {
		t.Fatalf("empty tumbler emitted %d aggregates", len(got))
	}
}

// TestTumblerConcurrentAddAndClose is the mutex's reason to exist: a consumer
// goroutine hammering Add while a flusher calls Closed. Run under
// `make test-race` — remove the Tumbler's locks and this fails loudly.
func TestTumblerConcurrentAddAndClose(t *testing.T) {
	tb := New(size, grace)

	var wg sync.WaitGroup
	const writers, perWriter = 8, 500

	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range perWriter {
				// Spread readings across many windows so Add and Closed
				// genuinely contend for the same map entries.
				ts := base.Add(time.Duration(i%40) * time.Second)
				tb.Add(reading(ts, 20.0+float64(w)), base.Add(time.Duration(i%30)*time.Second))
			}
		}(w)
	}

	harvested := 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 50 {
			for _, agg := range tb.Closed(base.Add(time.Duration(i) * time.Second)) {
				harvested += agg.Count
			}
		}
	}()
	wg.Wait()

	// Drain what's left; total stored must equal total accepted (nothing
	// lost or double-counted between Add and Closed).
	total := harvested
	for _, agg := range tb.Closed(base.Add(time.Hour)) {
		total += agg.Count
	}
	accepted := writers*perWriter - int(tb.LateCount())
	if total != accepted {
		t.Errorf("readings accounted for: got %d, want %d", total, accepted)
	}
}
