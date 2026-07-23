package simulator

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/ipko1996/huweathersim/pkg/events"
	"github.com/ipko1996/huweathersim/pkg/registry"
)

// fixedRand returns a generator seeded identically every time, so tests that
// involve randomness are still perfectly reproducible.
func fixedRand() *rand.Rand {
	return rand.New(rand.NewPCG(42, 1024))
}

// TestDriftTrendDirection checks the defining property of each pattern over many
// readings. Individual steps include jitter, so asserting one step would be
// flaky — the trend across a run is the real contract.
func TestDriftTrendDirection(t *testing.T) {
	tests := []struct {
		name    string
		pattern registry.Pattern
		// wantWarmer reports whether the end should exceed the start.
		wantWarmer bool
		// checkTrend is false for patterns with no directional guarantee.
		checkTrend bool
	}{
		{name: "rising warms up", pattern: registry.PatternRising, wantWarmer: true, checkTrend: true},
		{name: "falling cools down", pattern: registry.PatternFalling, wantWarmer: false, checkTrend: true},
		{name: "steady has no trend", pattern: registry.PatternSteady},
		{name: "noisy has no trend", pattern: registry.PatternNoisy},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDrift(tt.pattern, fixedRand())

			const start = 20.0
			temp := start
			for range 50 {
				temp = d.Next(temp)
			}

			if !tt.checkTrend {
				return
			}
			if tt.wantWarmer && temp <= start {
				t.Errorf("%s: got %.1f°C after 50 readings, want warmer than %.1f", tt.pattern, temp, start)
			}
			if !tt.wantWarmer && temp >= start {
				t.Errorf("%s: got %.1f°C after 50 readings, want cooler than %.1f", tt.pattern, temp, start)
			}
		})
	}
}

// TestDriftIsDeterministic pins the property the other tests rely on: same seed,
// same sequence. Without it, a flaky failure here would be impossible to debug.
func TestDriftIsDeterministic(t *testing.T) {
	a := NewDrift(registry.PatternNoisy, fixedRand())
	b := NewDrift(registry.PatternNoisy, fixedRand())

	tempA, tempB := 20.0, 20.0
	for i := range 20 {
		tempA, tempB = a.Next(tempA), b.Next(tempB)
		if tempA != tempB {
			t.Fatalf("reading %d diverged: %.1f vs %.1f", i, tempA, tempB)
		}
	}
}

// TestDriftStaysWithinValidRange is the important one. A "rising" sensor left
// running for hours must never drift past what the shared contract accepts —
// otherwise the simulator would eventually produce readings its own consumer
// rejects. 5,000 iterations is well past the clamp.
func TestDriftStaysWithinValidRange(t *testing.T) {
	for _, pattern := range []registry.Pattern{registry.PatternSteady, registry.PatternRising, registry.PatternFalling, registry.PatternNoisy} {
		t.Run(string(pattern), func(t *testing.T) {
			d := NewDrift(pattern, fixedRand())

			temp := 20.0
			for i := range 5000 {
				temp = d.Next(temp)

				// Validate through the real contract, not a copy of its bounds.
				r := events.SensorReading{
					SensorID: "sensor-test",
					Lat:      47.4979,
					Lon:      19.0402,
					TempC:    temp,
					Time:     time.Now().UTC(),
				}
				if err := r.Validate(); err != nil {
					t.Fatalf("reading %d (%.1f°C) failed validation: %v", i, temp, err)
				}
			}
		})
	}
}

// TestDriftRoundsToOneDecimal keeps the payload small, per the wire budget.
func TestDriftRoundsToOneDecimal(t *testing.T) {
	d := NewDrift(registry.PatternNoisy, fixedRand())

	temp := 20.0
	for range 100 {
		temp = d.Next(temp)
		// Scaling by 10 must land on a whole number if there's one decimal.
		if scaled := temp * 10; scaled != float64(int(scaled)) {
			t.Fatalf("temperature %v has more than one decimal place", temp)
		}
	}
}

// Pattern.Valid's tests moved to pkg/registry with the type itself.

// fakePublisher records readings in memory instead of sending them to Kafka.
// This is the payoff of Sensor.Run depending on the Publisher interface: the
// whole emit loop is testable with no broker, no network, and no waiting.
type fakePublisher struct {
	readings []events.SensorReading
}

func (f *fakePublisher) Publish(_ context.Context, r events.SensorReading) error {
	f.readings = append(f.readings, r)
	return nil
}

// TestSensorRunEmitsAndStops covers the two behaviours that matter: it produces
// valid readings on its interval, and it exits promptly when the context is
// cancelled (which is what SIGTERM triggers in Kubernetes).
func TestSensorRunEmitsAndStops(t *testing.T) {
	pub := &fakePublisher{}
	// A very short interval keeps the test fast.
	s := NewSensor("sensor-0001", 47.4979, 19.0402, 20.0, 10*time.Millisecond, registry.PatternSteady)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, pub) }()

	// Let a handful of ticks happen, then ask it to stop.
	time.Sleep(60 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop within 1s of cancellation")
	}

	if len(pub.readings) == 0 {
		t.Fatal("no readings published")
	}
	for i, r := range pub.readings {
		if err := r.Validate(); err != nil {
			t.Errorf("reading %d invalid: %v", i, err)
		}
		if r.SensorID != "sensor-0001" {
			t.Errorf("reading %d: got sensor id %q, want %q", i, r.SensorID, "sensor-0001")
		}
	}
}

func TestSensorValidate(t *testing.T) {
	tests := []struct {
		name    string
		sensor  *Sensor
		wantErr bool
	}{
		{
			name:   "valid Budapest sensor",
			sensor: NewSensor("sensor-0001", 47.4979, 19.0402, 20, time.Second, registry.PatternSteady),
		},
		{
			name:    "empty id",
			sensor:  NewSensor("", 47.4979, 19.0402, 20, time.Second, registry.PatternSteady),
			wantErr: true,
		},
		{
			name:    "zero interval",
			sensor:  NewSensor("sensor-0001", 47.4979, 19.0402, 20, 0, registry.PatternSteady),
			wantErr: true,
		},
		{
			name:    "location outside Hungary",
			sensor:  NewSensor("sensor-0001", 52.52, 13.40, 20, time.Second, registry.PatternSteady),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.sensor.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate(): got %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
