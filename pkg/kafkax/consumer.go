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

// Consumer reads messages from a topic as part of a consumer group. It is
// deliberately type-agnostic — decoding happens in the generic Run function,
// so one Consumer type serves every topic and event shape in the system.
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

// Run consumes messages until ctx is cancelled, decoding each into T and
// calling handler.
//
// # Why a package-level generic function, not a method
//
// Run must know the concrete type T to unmarshal into and to hand the handler
// a typed value — that requires a type parameter, and Go METHODS CANNOT HAVE
// TYPE PARAMETERS (a deliberate language restriction). So what reads most
// naturally as c.Run[T](...) must instead be the package function
// kafkax.Run(ctx, c, handler, dlq), taking the consumer as an argument.
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
//
// # Poison messages
//
// A message that cannot decode into T, or decodes but fails Validate, will
// never succeed no matter how often it's retried — retrying forever would
// block its partition (the classic "poison pill"). Those messages bypass the
// handler entirely: with dlq set they are wrapped in an events.DeadLetter and
// published there BEFORE the offset commits; with dlq nil they are logged and
// skipped. Either way the partition keeps moving, and the handler can trust
// that every T it receives is valid.
func Run[T events.Event](ctx context.Context, c *Consumer, handler func(context.Context, T) error, dlq *Producer) error {
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

		var event T
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			if err := c.deadLetter(ctx, dlq, msg, fmt.Errorf("unmarshal: %w", err)); err != nil {
				return err
			}
			continue
		}

		// Structurally valid JSON that breaks the event's own rules is just as
		// permanently bad as garbage bytes.
		if err := event.Validate(); err != nil {
			if err := c.deadLetter(ctx, dlq, msg, fmt.Errorf("validate: %w", err)); err != nil {
				return err
			}
			continue
		}

		if err := handler(ctx, event); err != nil {
			// Handler failures are treated as retryable, so the offset is left
			// uncommitted and the message will be redelivered. Returning here
			// stops the consumer; the process restart (compose/Kubernetes)
			// resumes from the last committed offset.
			return fmt.Errorf("handle message at %s[%d]@%d: %w",
				msg.Topic, msg.Partition, msg.Offset, err)
		}

		if err := c.commit(ctx, msg); err != nil {
			return err
		}
	}
}

// deadLetter disposes of one poison message: route it to the dlq (when
// configured), then commit past it so the partition keeps flowing.
//
// The order is deliberate — publish BEFORE commit. A crash between the two
// redelivers the poison message and produces a duplicate dead letter, which
// is harmless (a human reading the DLQ sees the same evidence twice). The
// reverse order could commit and then crash before publishing, silently
// destroying the evidence — the exact failure a DLQ exists to prevent.
func (c *Consumer) deadLetter(ctx context.Context, dlq *Producer, msg kafka.Message, cause error) error {
	if dlq == nil {
		// No DLQ configured (tests, tools): fall back to log-and-skip.
		log.Printf("skipping poison message at %s[%d]@%d: %v",
			msg.Topic, msg.Partition, msg.Offset, cause)
		return c.commit(ctx, msg)
	}

	env := events.DeadLetter{
		SourceTopic: msg.Topic,
		Partition:   msg.Partition,
		Offset:      msg.Offset,
		Reason:      cause.Error(),
		FailedAt:    time.Now().UTC(),
		Payload:     msg.Value,
	}
	// A dlq publish failure is returned WITHOUT committing: the broker is
	// clearly having trouble, and retry-by-restart will reprocess this
	// message and try the dead-lettering again.
	if err := dlq.Publish(ctx, env); err != nil {
		return fmt.Errorf("dead-letter %s[%d]@%d: %w", msg.Topic, msg.Partition, msg.Offset, err)
	}
	log.Printf("dead-lettered poison message at %s[%d]@%d: %v",
		msg.Topic, msg.Partition, msg.Offset, cause)
	return c.commit(ctx, msg)
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
