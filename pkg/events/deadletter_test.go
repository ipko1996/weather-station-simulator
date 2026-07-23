package events

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func validDeadLetter() DeadLetter {
	return DeadLetter{
		SourceTopic: "sensor.readings",
		Partition:   3,
		Offset:      1447,
		Reason:      "unmarshal: invalid character 'n'",
		FailedAt:    time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
		Payload:     []byte("not json at all"),
	}
}

// TestDeadLetterSurvivesGarbagePayload is the reason Payload is []byte: the
// envelope must marshal cleanly even when the payload is precisely NOT valid
// JSON — that's the input the DLQ exists for. (json.RawMessage would fail
// here, since it promises its content is valid JSON.)
func TestDeadLetterSurvivesGarbagePayload(t *testing.T) {
	d := validDeadLetter()

	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal envelope with garbage payload: %v", err)
	}

	// encoding/json base64-encodes []byte — pin that wire format, because
	// anyone debugging a dead letter needs to know to base64-decode payload.
	wantB64 := base64.StdEncoding.EncodeToString(d.Payload)
	if !strings.Contains(string(raw), `"payload":"`+wantB64+`"`) {
		t.Errorf("payload not base64-encoded on the wire:\n%s", raw)
	}

	var back DeadLetter
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if string(back.Payload) != string(d.Payload) {
		t.Errorf("payload round-trip: got %q, want %q", back.Payload, d.Payload)
	}
	if !back.FailedAt.Equal(d.FailedAt) {
		t.Errorf("failed_at round-trip: got %v, want %v", back.FailedAt, d.FailedAt)
	}
}

func TestDeadLetterValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*DeadLetter)
		wantErr error // nil = must pass
	}{
		{"valid", func(d *DeadLetter) {}, nil},
		{"missing source topic", func(d *DeadLetter) { d.SourceTopic = "" }, ErrMissingSourceTopic},
		{"missing reason", func(d *DeadLetter) { d.Reason = "" }, ErrMissingReason},
		// An empty payload is legal: a zero-length Kafka message is garbage
		// too, and the envelope must be able to report it.
		{"empty payload is fine", func(d *DeadLetter) { d.Payload = nil }, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := validDeadLetter()
			tc.mutate(&d)

			err := d.Validate()
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
