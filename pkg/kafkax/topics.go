package kafkax

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ipko1996/huweathersim/pkg/events"
)

// A single-broker dev cluster cannot replicate, so the factor must be 1.
// Kubernetes/Strimzi in Phase 4 runs multiple brokers and raises this.
const devReplicationFactor = 1

// TopicSpec is everything a topic needs decided about it. It exists because
// topics genuinely differ: a hot stream, a dead-letter inbox and an ephemeral
// aggregate feed want different partition counts and retention — burying
// those as hardcoded constants (as Phase 1 did, with one topic) would hand
// every future topic a stream's configuration whether it fits or not.
//
// It's also the Go rule of thumb in action: when a function's parameter list
// is about to grow past ~3, stop and introduce a config struct — call sites
// become self-describing (Partitions: 6) instead of positional mystery values.
type TopicSpec struct {
	Name       string
	Partitions int
	Retention  time.Duration
}

// The system's topics, declared in one place so their sizing can be compared
// at a glance. Partition counts cap consumer parallelism (one partition feeds
// at most one consumer in a group) — this is the number Phase 6 autoscaling
// scales *up to*.
var (
	// Raw readings are a stream, not a data lake — TimescaleDB holds history.
	ReadingsTopic = TopicSpec{
		Name:       events.TopicSensorReadings,
		Partitions: 6, // PROJECT.md §3 baseline
		Retention:  24 * time.Hour,
	}

	// Validated readings, same sizing as raw: every raw message that passes
	// validation flows through here, so the load profile is identical.
	CleanTopic = TopicSpec{
		Name:       events.TopicSensorReadingsClean,
		Partitions: 6,
		Retention:  24 * time.Hour,
	}

	// The dead-letter topic is an inbox, not a stream — hence the deliberately
	// different profile. 1 partition: volume is near-zero and ordering across
	// sources doesn't matter. 7 days: a stream's 24h retention would delete
	// the evidence before anyone gets around to reading it.
	DeadLetterTopic = TopicSpec{
		Name:       events.TopicDeadLetters,
		Partitions: 1,
		Retention:  7 * 24 * time.Hour,
	}
)

// EnsureTopic creates a topic to spec if it doesn't exist, and does nothing
// if it does.
//
// # Why this exists at all
//
// The compose stack disables topic auto-creation outright
// (KAFKA_AUTO_CREATE_TOPICS_ENABLE=false), and this function is the reason
// that's safe: every service that touches a topic ensures it at startup.
// With auto-creation, producing to an unknown topic would "just work" but
// silently create it with the broker default of ONE partition — everything
// would run fine until Phase 6, where autoscaling would cap at a single
// consumer no matter how many pods were added, with no error pointing at the
// cause.
//
// # Limitation worth knowing
//
// Ensure means create-if-missing, never alter: changing a spec's partitions
// or retention has NO effect on a topic that already exists. To apply a spec
// change in dev, delete the topic (Kafka UI) or wipe the broker's data dir.
func EnsureTopic(ctx context.Context, brokers []string, spec TopicSpec) error {
	client := &kafka.Client{
		Addr:    kafka.TCP(brokers...),
		Timeout: 10 * time.Second,
	}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{
			Topic:             spec.Name,
			NumPartitions:     spec.Partitions,
			ReplicationFactor: devReplicationFactor,
			ConfigEntries: []kafka.ConfigEntry{{
				ConfigName:  "retention.ms",
				ConfigValue: strconv.FormatInt(spec.Retention.Milliseconds(), 10),
			}},
		}},
	})
	if err != nil {
		return fmt.Errorf("create topic %s: %w", spec.Name, err)
	}

	// The broker reports per-topic outcomes in a map rather than as a single
	// error, because one request can create many topics.
	for name, topicErr := range resp.Errors {
		if topicErr == nil {
			continue
		}
		// Already existing is success for our purposes: EnsureTopic is meant to
		// be safe to call on every service start.
		if errors.Is(topicErr, kafka.TopicAlreadyExists) {
			continue
		}
		return fmt.Errorf("create topic %s: %w", name, topicErr)
	}
	return nil
}
