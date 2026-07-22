package kafkax

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// Topic sizing for sensor.readings, from PROJECT.md §3.
const (
	// ReadingsPartitions caps how many consumers can work in parallel: Kafka
	// assigns each partition to at most one consumer in a group, so a 7th pod
	// against 6 partitions would sit completely idle. This is the number Phase 6
	// autoscaling scales *up to*.
	ReadingsPartitions = 6

	// A single-broker dev cluster cannot replicate, so the factor must be 1.
	// Kubernetes/Strimzi in Phase 4 runs multiple brokers and raises this.
	devReplicationFactor = 1

	// Raw readings are a stream, not a data lake — TimescaleDB holds the history.
	readingsRetention = 24 * time.Hour
)

// EnsureTopic creates a topic if it doesn't exist, and does nothing if it does.
//
// # Why this exists at all
//
// The compose stack sets KAFKA_AUTO_CREATE_TOPICS_ENABLE=true, so producing to
// an unknown topic would appear to "just work". The catch: an auto-created topic
// gets the broker default of ONE partition. Everything would run fine through
// Phase 5, then Phase 6 autoscaling would cap at a single consumer no matter how
// many pods were added — with no error message pointing at the cause.
//
// Creating the topic deliberately, with a partition count chosen for the load,
// is the difference between a system that scales and one that only looks like it.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int) error {
	client := &kafka.Client{
		Addr:    kafka.TCP(brokers...),
		Timeout: 10 * time.Second,
	}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: devReplicationFactor,
			ConfigEntries: []kafka.ConfigEntry{{
				ConfigName:  "retention.ms",
				ConfigValue: strconv.FormatInt(readingsRetention.Milliseconds(), 10),
			}},
		}},
	})
	if err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
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
