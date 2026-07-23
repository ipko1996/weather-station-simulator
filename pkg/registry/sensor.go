// Package registry is the shared sensor registry backed by Redis.
//
// It answers one question for the whole system: "which sensors exist right
// now, and with what settings?" The sensor-gateway writes to it on API calls;
// the sensor-simulator reads it to decide which worker goroutines to run.
// Kafka deliberately plays no part here — the registry is *state* (the current
// set), not a *stream* (things that happened). Reaching for Kafka to hold
// state, or for Postgres to hold a few hundred hot ephemeral rows, would be
// the wrong tool; knowing that is part of this project's story (PROJECT.md §6).
//
// Durability trade-off, stated honestly: Redis here runs without persistence
// guarantees. If it dies, the registry is empty and sensors must be re-added.
// That is acceptable by design — sensors are simulated and cheap to recreate,
// and the *readings* history lives safely in TimescaleDB. What Redis buys in
// exchange is sub-millisecond reads and a built-in Pub/Sub channel for change
// notifications.
package registry

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ipko1996/huweathersim/pkg/events"
)

// Pattern names how a sensor's temperature evolves. The drift *math* lives in
// the simulator's internal packages; the *names* must live here, because the
// gateway has to validate them too and Go forbids importing another module's
// internal/ packages. A shared pkg/ package is the only place both services
// can see. (The simulator switches to these constants when it grows its
// registry-driven manager.)
type Pattern string

const (
	PatternSteady  Pattern = "steady"  // hovers around its starting value
	PatternRising  Pattern = "rising"  // warms steadily, e.g. a summer morning
	PatternFalling Pattern = "falling" // cools steadily, e.g. after sunset
	PatternNoisy   Pattern = "noisy"   // jumps around; a flaky or exposed sensor
)

// Valid reports whether p is one of the four known patterns.
func (p Pattern) Valid() bool {
	switch p {
	case PatternSteady, PatternRising, PatternFalling, PatternNoisy:
		return true
	default:
		return false
	}
}

// Emit-interval bounds from PROJECT.md §3 ("configurable 1–30s"). A 100ms
// interval times 2,000 sensors would be self-inflicted load nobody asked for;
// a 10m interval would make the map look dead.
const (
	MinInterval = 1 * time.Second
	MaxInterval = 30 * time.Second
)

// Sentinel errors, same design as pkg/events: callers match them with
// errors.Is to decide on an HTTP status or a log line, without parsing text.
var (
	ErrInvalidPattern     = errors.New("unknown drift pattern")
	ErrIntervalOutOfRange = fmt.Errorf("interval outside %s-%s", MinInterval, MaxInterval)

	// ErrInvalidSensor marks every validation failure from Add, so callers
	// can separate "the caller sent bad data" (HTTP 400) from "the store is
	// broken" (HTTP 500) with ONE errors.Is check instead of enumerating
	// every field-level sentinel. Getting this wrong has real consequences:
	// answering 400 to a Redis outage tells clients "your request is bad,
	// don't retry" about a request that would succeed on retry.
	ErrInvalidSensor = errors.New("invalid sensor")
)

// Sensor is a registered virtual weather station: everything the simulator
// needs to run a worker for it. This is configuration, not a reading —
// readings are pkg/events' job.
type Sensor struct {
	ID         string
	Lat        float64
	Lon        float64
	StartTempC float64
	Pattern    Pattern
	Interval   time.Duration
	CreatedAt  time.Time
}

// Validate checks a sensor before it enters the registry, so nothing
// downstream ever has to re-check.
//
// ID and coordinates follow the exact same rules as a reading's, so instead of
// duplicating the Hungary bounding box here, we build a probe reading and
// borrow events.SensorReading.Validate — one source of truth for those rules.
func (s Sensor) Validate() error {
	probe := events.SensorReading{
		SensorID: s.ID,
		Lat:      s.Lat,
		Lon:      s.Lon,
		TempC:    s.StartTempC,
		Time:     time.Now().UTC(), // not a stored field; just satisfies the probe
	}
	if err := probe.Validate(); err != nil {
		return err
	}
	if !s.Pattern.Valid() {
		return fmt.Errorf("%w: %q (want steady, rising, falling or noisy)", ErrInvalidPattern, s.Pattern)
	}
	if s.Interval < MinInterval || s.Interval > MaxInterval {
		return fmt.Errorf("%w: got %s", ErrIntervalOutOfRange, s.Interval)
	}
	return nil
}

// Redis hash field names. Redis stores only strings, so the two conversion
// functions below ARE the schema — there is no migration tool or ORM layer.
// Change a field here and both conversions must move together, which is why
// they sit side by side and share a round-trip unit test.
const (
	fieldID         = "id"
	fieldLat        = "lat"
	fieldLon        = "lon"
	fieldStartTempC = "start_temp_c"
	fieldPattern    = "pattern"
	fieldInterval   = "interval"
	fieldCreatedAt  = "created_at"
)

// toMap flattens a Sensor into the string fields stored in the Redis hash.
func (s Sensor) toMap() map[string]string {
	return map[string]string{
		fieldID:  s.ID,
		fieldLat: strconv.FormatFloat(s.Lat, 'f', -1, 64),
		fieldLon: strconv.FormatFloat(s.Lon, 'f', -1, 64),
		// 'f', -1: shortest decimal string that round-trips the exact float64.
		fieldStartTempC: strconv.FormatFloat(s.StartTempC, 'f', -1, 64),
		fieldPattern:    string(s.Pattern),
		// time.Duration.String gives "5s"/"1m30s" — the same format the
		// gateway accepts in its API, so a value read back from Redis is
		// indistinguishable from one a client sent.
		fieldInterval:  s.Interval.String(),
		fieldCreatedAt: s.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

// sensorFromMap rebuilds a Sensor from a Redis hash. It fails loudly on any
// malformed field: a corrupt registry entry should surface as an error at the
// read site, not as a zero-valued sensor silently running with lat 0, lon 0
// (which is in the Atlantic, not Hungary).
func sensorFromMap(m map[string]string) (Sensor, error) {
	var (
		s   Sensor
		err error
	)
	s.ID = m[fieldID]
	if s.Lat, err = strconv.ParseFloat(m[fieldLat], 64); err != nil {
		return Sensor{}, fmt.Errorf("field %s: %w", fieldLat, err)
	}
	if s.Lon, err = strconv.ParseFloat(m[fieldLon], 64); err != nil {
		return Sensor{}, fmt.Errorf("field %s: %w", fieldLon, err)
	}
	if s.StartTempC, err = strconv.ParseFloat(m[fieldStartTempC], 64); err != nil {
		return Sensor{}, fmt.Errorf("field %s: %w", fieldStartTempC, err)
	}
	s.Pattern = Pattern(m[fieldPattern])
	if s.Interval, err = time.ParseDuration(m[fieldInterval]); err != nil {
		return Sensor{}, fmt.Errorf("field %s: %w", fieldInterval, err)
	}
	if s.CreatedAt, err = time.Parse(time.RFC3339Nano, m[fieldCreatedAt]); err != nil {
		return Sensor{}, fmt.Errorf("field %s: %w", fieldCreatedAt, err)
	}
	return s, nil
}
