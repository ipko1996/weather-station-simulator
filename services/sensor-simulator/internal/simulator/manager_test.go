package simulator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ipko1996/huweathersim/pkg/events"
	"github.com/ipko1996/huweathersim/pkg/registry"
)

// safePublisher counts published readings per sensor. Unlike drift_test's
// fakePublisher it takes a mutex, because manager tests run MANY sensor
// goroutines publishing concurrently — an unguarded map here is exactly the
// kind of data race `make test-race` exists to catch.
type safePublisher struct {
	mu     sync.Mutex
	counts map[string]int
}

func newSafePublisher() *safePublisher {
	return &safePublisher{counts: make(map[string]int)}
}

func (p *safePublisher) Publish(_ context.Context, r events.SensorReading) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counts[r.SensorID]++
	return nil
}

func (p *safePublisher) count(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts[id]
}

// fakeLister serves a swappable sensor list. Mutex-guarded because the test
// goroutine swaps it while the manager goroutine reads it.
type fakeLister struct {
	mu      sync.Mutex
	sensors []registry.Sensor
}

func (l *fakeLister) List(_ context.Context) ([]registry.Sensor, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]registry.Sensor(nil), l.sensors...), nil
}

func (l *fakeLister) set(sensors ...registry.Sensor) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sensors = sensors
}

func managedSensor(id string) registry.Sensor {
	return registry.Sensor{
		ID:  id,
		Lat: 47.4979, Lon: 19.0402,
		StartTempC: 20,
		Pattern:    registry.PatternSteady,
		// Milliseconds, far below registry.MinInterval: the manager
		// deliberately does not re-validate (the registry already did, at
		// Add), and tests exploit that for speed.
		Interval: 5 * time.Millisecond,
	}
}

// waitFor polls cond until it's true or the deadline passes. Polling beats
// fixed sleeps in concurrent tests: it passes as soon as the condition holds
// instead of always paying worst-case time.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal(msg)
}

// TestManagerReconcilesFleet walks the full lifecycle: two sensors start,
// one is removed and stops, cancellation drains everything.
func TestManagerReconcilesFleet(t *testing.T) {
	lister := &fakeLister{}
	lister.set(managedSensor("sensor-a"), managedSensor("sensor-b"))
	pub := newSafePublisher()
	m := NewManager(lister, pub, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()

	// Both registered sensors must come alive and publish.
	waitFor(t, 2*time.Second, func() bool {
		return pub.count("sensor-a") > 0 && pub.count("sensor-b") > 0
	}, "both sensors should be publishing")

	// Remove one: after the next reconcile its publishes must stop while the
	// survivor keeps going.
	//
	// Proving "it stopped" needs care: asserting the absence of an event is
	// only sound if the observed silence is LONGER than the interval within
	// which a live worker is guaranteed to act. A live sensor ticks every
	// 5ms, so 50ms with no new reading means it's dead — while a shorter
	// window would flake on an in-flight final tick.
	lister.set(managedSensor("sensor-b"))
	var (
		frozen     int
		lastChange = time.Now()
	)
	waitFor(t, 2*time.Second, func() bool {
		c := pub.count("sensor-a")
		if c != frozen {
			frozen, lastChange = c, time.Now()
			return false
		}
		return c > 0 && time.Since(lastChange) > 50*time.Millisecond
	}, "sensor-a should stop publishing after removal")

	before := pub.count("sensor-b")
	waitFor(t, 2*time.Second, func() bool {
		return pub.count("sensor-b") > before
	}, "sensor-b should keep publishing after sensor-a's removal")

	// Cancel the parent: Run must return only after every worker drained.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of cancellation")
	}
}

// TestManagerKickTriggersImmediateReconcile pins the Pub/Sub contract: with a
// glacial poll interval, a Kick alone must pick up a new sensor.
func TestManagerKickTriggersImmediateReconcile(t *testing.T) {
	lister := &fakeLister{}
	pub := newSafePublisher()
	// One hour: if the sensor starts, it was the Kick, not the ticker.
	m := NewManager(lister, pub, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()

	lister.set(managedSensor("sensor-late"))
	m.Kick()

	waitFor(t, 2*time.Second, func() bool {
		return pub.count("sensor-late") > 0
	}, "kicked sensor should publish without waiting for the poll ticker")

	cancel()
	<-done
}

// TestManagerKickNeverBlocks: Kick is called from the Pub/Sub goroutine, so
// it must be safe to call at any rate regardless of what Run is doing.
func TestManagerKickNeverBlocks(t *testing.T) {
	m := NewManager(&fakeLister{}, newSafePublisher(), time.Hour)
	// Run is NOT started: nothing ever drains the kick channel, which is the
	// worst case. 100 kicks must still return instantly (they coalesce into
	// the single buffered slot).
	for range 100 {
		m.Kick()
	}
}
