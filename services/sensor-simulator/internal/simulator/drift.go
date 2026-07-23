// Package simulator turns a virtual weather station into a stream of readings.
//
// Everything here is deliberately free of Kafka and I/O: a Sensor talks to a
// small Publisher interface, and temperature drift is pure arithmetic. That's
// what makes this package fast to unit-test without a broker anywhere in sight.
package simulator

import (
	"math"
	"math/rand/v2"

	"github.com/ipko1996/huweathersim/pkg/registry"
)

// The Pattern type and its four names (steady/rising/falling/noisy) moved to
// pkg/registry in Phase 2: the gateway must validate pattern names too, and it
// cannot import this internal/ package across module boundaries. The split is
// deliberate — registry owns the *names*, this file owns the *math*.

// Per-reading temperature change, in °C.
const (
	steadyJitter = 0.10 // barely moves
	trendStep    = 0.20 // rising/falling drift per reading
	trendJitter  = 0.05 // makes a trend look natural rather than perfectly linear
	noisyJitter  = 1.50 // deliberately erratic
)

// Simulated temperatures are clamped to a plausible range so a long-running
// "rising" sensor can't drift past what events.SensorReading.Validate accepts
// and start failing its own validation. Kept just inside the validation bounds
// (-50/+60) rather than at them, so the clamp bites first.
const (
	minSimTempC = -45.0
	maxSimTempC = 55.0
)

// Drift produces successive temperatures for one sensor.
//
// It holds its own *rand.Rand rather than using the global rand functions, for
// two reasons: a test can inject a fixed seed and get deterministic output, and
// each sensor goroutine gets its own generator instead of contending on the
// shared global one — which matters at 2,000 sensors.
type Drift struct {
	pattern registry.Pattern
	rng     *rand.Rand
}

// NewDrift builds a Drift for the given pattern. Pass a seeded *rand.Rand for
// reproducible output (tests), or NewSeededRand() for real randomness.
func NewDrift(pattern registry.Pattern, rng *rand.Rand) *Drift {
	return &Drift{pattern: pattern, rng: rng}
}

// NewSeededRand returns a randomly-seeded generator for production use.
func NewSeededRand() *rand.Rand {
	// PCG is math/rand/v2's modern generator. rand.Uint64() seeds it from the
	// runtime's global source, so each sensor starts somewhere different.
	return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
}

// Next returns the temperature following current, according to the pattern.
//
// It's a pure function of (current, pattern, rng state) — no clock, no I/O —
// which is exactly why it can be tested exhaustively in microseconds.
func (d *Drift) Next(current float64) float64 {
	var next float64

	switch d.pattern {
	case registry.PatternRising:
		next = current + trendStep + d.jitter(trendJitter)
	case registry.PatternFalling:
		next = current - trendStep + d.jitter(trendJitter)
	case registry.PatternNoisy:
		next = current + d.jitter(noisyJitter)
	case registry.PatternSteady:
		fallthrough
	default:
		// An unknown pattern degrades to steady rather than panicking: a bad
		// value in a config map shouldn't take a pod down.
		next = current + d.jitter(steadyJitter)
	}

	// Round to one decimal — real sensors don't report 21.4999999999998, and it
	// keeps the JSON payload short (the wire budget from PROJECT.md §3).
	next = math.Round(next*10) / 10

	return clamp(next, minSimTempC, maxSimTempC)
}

// jitter returns a random value in [-magnitude, +magnitude].
func (d *Drift) jitter(magnitude float64) float64 {
	// Float64() yields [0.0, 1.0), so this maps to the symmetric range.
	return (d.rng.Float64()*2 - 1) * magnitude
}

// clamp confines v to [lo, hi].
func clamp(v, lo, hi float64) float64 {
	return math.Min(math.Max(v, lo), hi)
}
