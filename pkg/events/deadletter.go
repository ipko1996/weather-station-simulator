package events

import (
	"errors"
	"fmt"
	"time"
)

// TopicDeadLetters is where poison messages go instead of being dropped: raw
// bytes that failed to decode, or decoded events that failed validation. A
// dead-letter topic is an INBOX, not a stream — nothing consumes it in Phase
// 2; a human inspects it (Kafka UI) when debugging why data went missing.
const TopicDeadLetters = "sensor.readings.dlq"

// DeadLetter wraps a poison message with everything needed to debug it later:
// where it came from, why it was rejected, and the untouched original bytes.
type DeadLetter struct {
	SourceTopic string `json:"source_topic"`
	Partition   int    `json:"partition"`
	// Offset pinpoints the exact poison message in the source topic — with
	// topic+partition+offset, the original can always be re-inspected in
	// place (until retention expires it).
	Offset   int64     `json:"offset"`
	Reason   string    `json:"reason"` // the decode/validation error, verbatim
	FailedAt time.Time `json:"failed_at"`
	// Payload is the original message, byte for byte. The type MUST be
	// []byte, not json.RawMessage: RawMessage promises "this is valid JSON",
	// and the number-one reason a message lands here is that it ISN'T —
	// marshaling the envelope would then fail on exactly the input the DLQ
	// exists to capture. []byte instead gets base64-encoded by encoding/json,
	// which swallows arbitrary garbage safely (decode with base64 -d).
	Payload []byte `json:"payload"`
}

var _ Event = DeadLetter{}

// Sentinel errors for envelope validation, same design as the reading's.
var (
	ErrMissingSourceTopic = errors.New("source topic is required")
	ErrMissingReason      = errors.New("reason is required")
)

// Key routes by source topic: all dead letters from one topic stay together
// and ordered. With a single-partition DLQ this is currently cosmetic, but
// the key survives a future partition increase without a code change.
func (d DeadLetter) Key() []byte {
	return []byte(d.SourceTopic)
}

// Validate checks the envelope — NOT the payload. The payload is expected to
// be garbage; the envelope around it must still be well-formed enough to
// debug from.
func (d DeadLetter) Validate() error {
	if d.SourceTopic == "" {
		return ErrMissingSourceTopic
	}
	if d.Reason == "" {
		return ErrMissingReason
	}
	if d.FailedAt.IsZero() {
		return fmt.Errorf("dead letter from %s: failed_at timestamp is required", d.SourceTopic)
	}
	return nil
}
