package registry

import (
	"errors"
	"testing"
	"time"

	"github.com/ipko1996/huweathersim/pkg/events"
)

// validSensor returns a sensor that passes Validate; each test case mutates
// exactly one field, so a failure names the rule that broke.
func validSensor() Sensor {
	return Sensor{
		ID:         "sensor-0001",
		Lat:        47.4979, // Budapest
		Lon:        19.0402,
		StartTempC: 20.0,
		Pattern:    PatternNoisy,
		Interval:   5 * time.Second,
		CreatedAt:  time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
	}
}

func TestSensorValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Sensor)
		wantErr error // nil means the sensor must be accepted
	}{
		{"valid sensor", func(s *Sensor) {}, nil},
		{"missing id", func(s *Sensor) { s.ID = "" }, events.ErrMissingSensorID},
		// Berlin: proves the Hungary bounding box is enforced here too, via the
		// probe reading — not by a second copy of the coordinates.
		{"outside Hungary", func(s *Sensor) { s.Lat, s.Lon = 52.52, 13.405 }, events.ErrOutOfBounds},
		{"absurd start temp", func(s *Sensor) { s.StartTempC = 100 }, events.ErrTempOutOfRange},
		{"unknown pattern", func(s *Sensor) { s.Pattern = "sideways" }, ErrInvalidPattern},
		{"empty pattern", func(s *Sensor) { s.Pattern = "" }, ErrInvalidPattern},
		{"interval too short", func(s *Sensor) { s.Interval = 500 * time.Millisecond }, ErrIntervalOutOfRange},
		{"interval too long", func(s *Sensor) { s.Interval = time.Minute }, ErrIntervalOutOfRange},
		{"interval at lower bound", func(s *Sensor) { s.Interval = MinInterval }, nil},
		{"interval at upper bound", func(s *Sensor) { s.Interval = MaxInterval }, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := validSensor()
			tc.mutate(&s)

			err := s.Validate()
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Validate() = %v, want errors.Is(..., %v)", err, tc.wantErr)
			}
		})
	}
}

// TestSensorMapRoundTrip guards the Redis "schema": toMap and sensorFromMap
// must stay exact inverses, or a sensor written by the gateway comes back
// subtly different in the simulator.
func TestSensorMapRoundTrip(t *testing.T) {
	want := validSensor()

	got, err := sensorFromMap(want.toMap())
	if err != nil {
		t.Fatalf("sensorFromMap: %v", err)
	}

	// CreatedAt needs time.Equal (wall-clock comparison); everything else is
	// comparable directly. Compare field by field so a failure names the field.
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", got.CreatedAt, want.CreatedAt)
	}
	got.CreatedAt = want.CreatedAt
	if got != want {
		t.Errorf("round trip changed the sensor:\ngot  %+v\nwant %+v", got, want)
	}
}

// TestSensorFromMapRejectsCorruptFields: a mangled registry entry must fail at
// the read site, not come back as a zero-valued sensor.
func TestSensorFromMapRejectsCorruptFields(t *testing.T) {
	fields := []string{fieldLat, fieldLon, fieldStartTempC, fieldInterval, fieldCreatedAt}

	for _, field := range fields {
		t.Run(field, func(t *testing.T) {
			m := validSensor().toMap()
			m[field] = "garbage"

			if _, err := sensorFromMap(m); err == nil {
				t.Fatalf("sensorFromMap accepted corrupt %s", field)
			}
		})
	}
}
