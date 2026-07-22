package events

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

// validReading is the baseline used across the tests below: a plausible reading
// from Budapest. Each test copies it and breaks exactly one field, so a failure
// points at one specific rule.
func validReading() SensorReading {
	return SensorReading{
		SensorID: "sensor-0001",
		Lat:      47.4979,
		Lon:      19.0402,
		TempC:    21.5,
		Time:     time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
	}
}

// TestSensorReadingRoundTrip proves a reading survives the trip through JSON
// unchanged. This matters more than it looks: the producer marshals, Kafka moves
// opaque bytes, and the consumer unmarshals. If the two sides disagree about the
// format, *this* is the test that catches it.
func TestSensorReadingRoundTrip(t *testing.T) {
	original := validReading()

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded SensorReading
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// time.Time must be compared with Equal, not ==. Two times can represent the
	// same instant while carrying different internal monotonic-clock and location
	// data, which would make == report a spurious difference.
	if !decoded.Time.Equal(original.Time) {
		t.Errorf("Time: got %v, want %v", decoded.Time, original.Time)
	}

	// Blank out the times so the remaining fields can be compared in one shot.
	decoded.Time, original.Time = time.Time{}, time.Time{}
	if decoded != original {
		t.Errorf("round trip mismatch:\n got %+v\nwant %+v", decoded, original)
	}
}

// TestSensorReadingJSONFieldNames pins the wire format. The struct tags are a
// contract with every other service (and, in Phase 3, the browser); renaming a
// Go field is safe, but silently changing its JSON name would break consumers
// that are already deployed. This test makes that breakage loud and deliberate.
func TestSensorReadingJSONFieldNames(t *testing.T) {
	data, err := json.Marshal(validReading())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Decode into a map so we inspect the actual wire keys, not the Go names.
	var wire map[string]any
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatalf("unmarshal to map: %v", err)
	}

	for _, key := range []string{"sensor_id", "lat", "lon", "temp_c", "ts"} {
		if _, ok := wire[key]; !ok {
			t.Errorf("missing wire field %q; got keys %v", key, wire)
		}
	}
	if len(wire) != 5 {
		t.Errorf("unexpected field count: got %d, want 5 (%v)", len(wire), wire)
	}
}

// TestSensorReadingWireSize guards the throughput budget from PROJECT.md §3:
// ~200 bytes per message at up to 2,000 msg/s. Nothing breaks the instant a
// message gets bigger, but this test makes growth a conscious decision rather
// than something discovered during a load test in Phase 6.
func TestSensorReadingWireSize(t *testing.T) {
	data, err := json.Marshal(validReading())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	const budget = 200
	if len(data) > budget {
		t.Errorf("encoded reading is %d bytes, over the %d byte budget: %s",
			len(data), budget, data)
	}
	t.Logf("encoded reading is %d bytes (budget %d)", len(data), budget)
}

// TestSensorReadingValidate is table-driven — the standard Go pattern for
// checking many cases against one function. Each case is a named row; t.Run
// turns each into its own subtest, so a failure names the exact rule that broke.
func TestSensorReadingValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*SensorReading) // breaks one field of the valid baseline
		wantErr error                // nil means "should pass validation"
	}{
		{
			name:   "valid reading",
			mutate: func(*SensorReading) {}, // no change
		},
		{
			name:    "empty sensor id",
			mutate:  func(r *SensorReading) { r.SensorID = "" },
			wantErr: ErrMissingSensorID,
		},
		{
			name:    "zero timestamp",
			mutate:  func(r *SensorReading) { r.Time = time.Time{} },
			wantErr: ErrMissingTime,
		},
		{
			name:    "temperature absurdly high",
			mutate:  func(r *SensorReading) { r.TempC = 9999 },
			wantErr: ErrTempOutOfRange,
		},
		{
			name:    "temperature NaN",
			mutate:  func(r *SensorReading) { r.TempC = math.NaN() },
			wantErr: ErrTempOutOfRange,
		},
		{
			name:    "latitude north of Hungary",
			mutate:  func(r *SensorReading) { r.Lat = 52.5 }, // Berlin
			wantErr: ErrOutOfBounds,
		},
		{
			// Regression: range checks alone let NaN through, because every
			// comparison with NaN is false. Each float field needs its own
			// explicit IsNaN check.
			name:    "latitude NaN",
			mutate:  func(r *SensorReading) { r.Lat = math.NaN() },
			wantErr: ErrOutOfBounds,
		},
		{
			name:    "longitude NaN",
			mutate:  func(r *SensorReading) { r.Lon = math.NaN() },
			wantErr: ErrOutOfBounds,
		},
		{
			name:    "longitude west of Hungary",
			mutate:  func(r *SensorReading) { r.Lon = 2.35 }, // Paris
			wantErr: ErrOutOfBounds,
		},
		{
			name:   "cold but plausible winter reading",
			mutate: func(r *SensorReading) { r.TempC = -28 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validReading()
			tt.mutate(&r)

			err := r.Validate()

			// errors.Is unwraps the %w-wrapped sentinel, so a rule can add
			// context ("out of range: 9999.0°C") without breaking this check.
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate(): got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// TestSensorReadingKey checks the partition key, which is what guarantees one
// sensor's readings stay in order within a single Kafka partition.
func TestSensorReadingKey(t *testing.T) {
	r := validReading()

	if got, want := string(r.Key()), "sensor-0001"; got != want {
		t.Errorf("Key(): got %q, want %q", got, want)
	}

	// The same sensor must always produce the same key — otherwise Kafka would
	// scatter its readings across partitions and lose ordering.
	other := validReading()
	other.TempC = -5 // different reading, same sensor
	if string(r.Key()) != string(other.Key()) {
		t.Error("Key() differs between two readings from the same sensor")
	}
}
