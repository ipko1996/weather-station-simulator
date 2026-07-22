// Package events defines the wire contract shared by every service in the system.
//
// This package is deliberately dependency-free: no Kafka, no HTTP, no database.
// It describes *what* a message looks like, not how it travels. Every other
// service imports it, which is exactly why it must stay small and stable —
// changing a struct tag here changes the on-the-wire format for all six services.
package events

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// TopicSensorReadings is the raw ingest topic: everything a sensor emits lands
// here first, before any validation or cleaning. Naming the topic in the shared
// package (rather than as a string literal in each service) means a producer and
// a consumer can never disagree about where the data lives.
const TopicSensorReadings = "sensor.readings"

// Hungary's approximate bounding box. Readings outside it are rejected — the
// simulated network is national, so an out-of-bounds coordinate means a bug
// upstream (or a malformed message), not a real sensor.
const (
	minLat, maxLat = 45.7, 48.6
	minLon, maxLon = 16.1, 22.9
)

// Plausible physical bounds for a Hungarian air temperature, in Celsius. Records
// are roughly -35 and +42, so this is generous on purpose: the goal is to catch
// garbage (a NaN, a sensor reporting 9999) without rejecting a genuine extreme.
const (
	minTempC, maxTempC = -50.0, 60.0
)

// Sentinel errors let callers test *why* validation failed with errors.Is(),
// instead of string-matching an error message. The consumer uses this in Phase 2
// to decide between "drop this message" and "retry it".
var (
	ErrMissingSensorID = errors.New("sensor id is required")
	ErrOutOfBounds     = errors.New("coordinates outside Hungary")
	ErrTempOutOfRange  = errors.New("temperature out of plausible range")
	ErrMissingTime     = errors.New("timestamp is required")
)

// SensorReading is one measurement from one simulated weather station.
//
// The `json:"..."` struct tags control the field names on the wire. Go's
// convention is exported (Capitalized) fields in code mapped to snake_case JSON —
// the tag is what bridges the two. Field names are kept short because PROJECT.md
// budgets ~200 bytes per message at 2,000 msg/s; every byte here is multiplied by
// the whole fleet.
type SensorReading struct {
	SensorID string    `json:"sensor_id"`
	Lat      float64   `json:"lat"`
	Lon      float64   `json:"lon"`
	TempC    float64   `json:"temp_c"`
	Time     time.Time `json:"ts"`
}

// Validate reports whether the reading is structurally sane.
//
// It returns an error rather than a bool so the caller learns *what* was wrong —
// which ends up in the consumer's logs, and later in the telemetry page's error
// metrics. Returning the first failure (rather than collecting all of them) is
// enough here: a message with two problems is discarded either way.
func (r SensorReading) Validate() error {
	if r.SensorID == "" {
		return ErrMissingSensorID
	}
	if r.Time.IsZero() {
		return ErrMissingTime
	}
	// NaN must be checked explicitly on EVERY float field: any comparison
	// involving NaN is false, so a plain `v < min || v > max` range check lets
	// NaN slip straight through. (An earlier version of this code caught a NaN
	// temperature but not NaN coordinates — exactly that bug.)
	if math.IsNaN(r.TempC) || r.TempC < minTempC || r.TempC > maxTempC {
		return fmt.Errorf("%w: %.1f°C", ErrTempOutOfRange, r.TempC)
	}
	if math.IsNaN(r.Lat) || math.IsNaN(r.Lon) ||
		r.Lat < minLat || r.Lat > maxLat || r.Lon < minLon || r.Lon > maxLon {
		return fmt.Errorf("%w: (%.4f, %.4f)", ErrOutOfBounds, r.Lat, r.Lon)
	}
	return nil
}

// Key returns the Kafka partition key for this reading.
//
// Kafka routes messages by key: the same key always lands on the same partition,
// and order is guaranteed *within* a partition. Keying by sensor ID therefore
// guarantees one sensor's readings arrive in the order it sent them — which is
// what makes the temperature drift patterns meaningful downstream. Keying by
// something random would scatter a single sensor's history across all 6
// partitions and lose that ordering.
func (r SensorReading) Key() []byte {
	return []byte(r.SensorID)
}
