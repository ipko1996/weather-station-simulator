package simulator

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ipko1996/huweathersim/pkg/events"
	"github.com/ipko1996/huweathersim/pkg/registry"
)

// Publisher is anything that can send a reading onward.
//
// Depending on this one-method interface rather than on *kafkax.Producer is a
// core Go idiom: "accept interfaces, return structs". The payoff is immediate —
// tests substitute an in-memory fake and never start a broker, and Phase 2 can
// swap in a batching publisher without touching this file.
type Publisher interface {
	Publish(ctx context.Context, r events.SensorReading) error
}

// Sensor is one simulated weather station: a fixed location that reports a
// temperature on an interval.
type Sensor struct {
	ID       string
	Lat      float64
	Lon      float64
	Interval time.Duration

	temp  float64 // current temperature, mutated on each tick
	drift *Drift
}

// NewSensor builds a sensor starting at startTemp and evolving by pattern.
func NewSensor(id string, lat, lon, startTemp float64, interval time.Duration, pattern registry.Pattern) *Sensor {
	return &Sensor{
		ID:       id,
		Lat:      lat,
		Lon:      lon,
		Interval: interval,
		temp:     startTemp,
		drift:    NewDrift(pattern, NewSeededRand()),
	}
}

// Run emits readings until ctx is cancelled. It blocks, so callers typically
// launch it in a goroutine: `go sensor.Run(ctx, producer)`.
//
// In Phase 1 the service runs exactly one sensor. Phase 2 runs one goroutine per
// sensor — hundreds or thousands of them — which is precisely why Go was chosen:
// a goroutine costs a couple of KB, so 2,000 idle sensors are nearly free, where
// 2,000 OS threads would not be.
func (s *Sensor) Run(ctx context.Context, pub Publisher) error {
	// A Ticker fires on a channel at a fixed interval. Stop() must be deferred
	// or the underlying runtime timer leaks — with one goroutine per sensor,
	// leaks compound fast.
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	log.Printf("sensor %s started at (%.4f, %.4f), %.1f°C every %s",
		s.ID, s.Lat, s.Lon, s.temp, s.Interval)

	for {
		// select waits on several channels at once and proceeds with whichever
		// is ready first. Here it's a race between "time to emit" and "time to
		// stop" — the standard shape of every long-running Go loop.
		select {
		case <-ctx.Done():
			// ctx.Done() closes when the context is cancelled, which happens on
			// SIGTERM. Returning nil marks this as a clean, expected exit.
			log.Printf("sensor %s stopping", s.ID)
			return nil

		case <-ticker.C:
			s.temp = s.drift.Next(s.temp)

			reading := events.SensorReading{
				SensorID: s.ID,
				Lat:      s.Lat,
				Lon:      s.Lon,
				TempC:    s.temp,
				Time:     time.Now().UTC(),
			}

			if err := pub.Publish(ctx, reading); err != nil {
				// A publish failing during shutdown is expected, not an error
				// worth reporting: the context was cancelled mid-write.
				if ctx.Err() != nil {
					return nil
				}
				// Otherwise log and keep going. One failed reading should not
				// kill a sensor that has thousands more to send; the failure is
				// visible in the error-rate metrics added in Phase 5.
				log.Printf("sensor %s publish failed: %v", s.ID, err)
				continue
			}

			log.Printf("sensor %s published %.1f°C", s.ID, s.temp)
		}
	}
}

// Validate checks the sensor's configuration before it starts, so a typo in an
// env var fails loudly at boot instead of producing readings rejected downstream.
func (s *Sensor) Validate() error {
	if s.ID == "" {
		return fmt.Errorf("sensor id is required")
	}
	if s.Interval <= 0 {
		return fmt.Errorf("interval must be positive, got %s", s.Interval)
	}
	// Reuse the shared contract's rules rather than duplicating the bounds here:
	// one definition of "a valid reading", in one place.
	probe := events.SensorReading{
		SensorID: s.ID,
		Lat:      s.Lat,
		Lon:      s.Lon,
		TempC:    s.temp,
		Time:     time.Now().UTC(),
	}
	if err := probe.Validate(); err != nil {
		return fmt.Errorf("sensor %s has invalid configuration: %w", s.ID, err)
	}
	return nil
}
