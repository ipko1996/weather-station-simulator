package kafkax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ipko1996/huweathersim/pkg/events"
)

// Handler processes one reading. Returning an error means "this failed and is
// worth retrying" — the offset is not committed, so the message is redelivered.
type Handler func(ctx context.Context, r events.SensorReading) error

// Consumer reads sensor readings from a topic as part of a consumer group.
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer joins the consumer group `groupID` on `topic`.
//
// The group ID is the important argument. Every consumer sharing a group ID:
//   - splits the topic's partitions between them (6 partitions = up to 6 workers)
//   - shares one set of committed offsets, so each message is handled once
//
// Start a second pod with the same group and Kafka rebalances partitions across
// both automatically. That is the entire mechanism behind Phase 6 autoscaling.
// Use a *different* group ID and you get an independent copy of the whole stream
// instead — which is how Phase 2 lets several services read the same topic.
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
			// Where to begin when this group has never committed an offset.
			// FirstOffset replays the retained log from the start; LastOffset
			// would skip everything already in the topic.
			StartOffset: kafka.FirstOffset,
			// Wait for at least 1 byte but no more than 10MB per fetch, and let
			// the broker hold an idle fetch open for at most 3s (the kafka-go
			// default is 10s). With MinBytes 1 the broker answers the moment any
			// data exists, so MaxWait only matters when the topic is idle — a
			// shorter value just bounds how long a quiet consumer sits inside a
			// single blocking fetch.
			MinBytes: 1,
			MaxBytes: 10e6,
			MaxWait:  3 * time.Second,
		}),
	}
}

// Run consumes messages until ctx is cancelled, calling handler for each one.
//
// # At-least-once delivery
//
// The ordering below IS the delivery guarantee, which is why it's written out
// explicitly rather than using kafka-go's auto-commit:
//
//	fetch → handle → commit
//
// The offset is committed only after the handler succeeds. Crash in between and
// the message is redelivered on restart, because Kafka still believes it was
// never processed. That means duplicates are normal and downstream code must
// tolerate them — but a reading is never silently lost. Auto-commit would invert
// this into at-most-once: the offset advances on a timer whether the handler
// succeeded or not.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	for {
		// FetchMessage blocks until a message arrives or ctx is cancelled. It
		// deliberately does NOT commit — that's what makes the ordering above
		// possible.
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			// A cancelled context is the expected exit path on shutdown, not a
			// failure, so it's reported as a clean return.
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		var reading events.SensorReading
		if err := json.Unmarshal(msg.Value, &reading); err != nil {
			// A message that isn't valid JSON will never become valid. Retrying
			// forever would block the partition — the classic "poison pill".
			// So: log it, commit past it, move on. Phase 2 routes these to a
			// dead-letter topic instead of dropping them.
			log.Printf("skipping malformed message at %s[%d]@%d: %v",
				msg.Topic, msg.Partition, msg.Offset, err)
			if err := c.commit(ctx, msg); err != nil {
				return err
			}
			continue
		}

		// Same reasoning: structurally valid JSON that breaks our rules is
		// permanently bad data, not a transient failure.
		if err := reading.Validate(); err != nil {
			log.Printf("skipping invalid reading %s at %s[%d]@%d: %v",
				reading.SensorID, msg.Topic, msg.Partition, msg.Offset, err)
			if err := c.commit(ctx, msg); err != nil {
				return err
			}
			continue
		}

		if err := handler(ctx, reading); err != nil {
			// Handler failures are treated as retryable, so the offset is left
			// uncommitted and the message will be redelivered. Returning here
			// stops the consumer; in Kubernetes the pod restarts and resumes
			// from the last committed offset.
			return fmt.Errorf("handle reading %s at %s[%d]@%d: %w",
				reading.SensorID, msg.Topic, msg.Partition, msg.Offset, err)
		}

		if err := c.commit(ctx, msg); err != nil {
			return err
		}
	}
}

// commit acknowledges a message, advancing this group's committed offset.
func (c *Consumer) commit(ctx context.Context, msg kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("commit offset %d: %w", msg.Offset, err)
	}
	return nil
}

// Close leaves the consumer group cleanly. Skipping it makes Kafka wait for the
// session timeout before rebalancing, stalling the remaining consumers.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
