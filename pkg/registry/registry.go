package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Storage layout in Redis:
//
//	sensor:<id>  hash  — one sensor's settings (see toMap for the fields)
//	sensors      set   — the ids of all registered sensors (the index)
//
// The set exists because Redis has no "list all hashes matching sensor:*"
// primitive — KEYS blocks the whole (single-threaded!) server and SCAN needs
// cursor bookkeeping. Maintaining our own index costs one SADD/SREM per write
// and makes listing a cheap O(set size) read.
const (
	keyPrefix = "sensor:"
	setKey    = "sensors"

	// ChannelSensorsChanged carries a Pub/Sub message (the sensor id) on every
	// Add/Remove. Subscribers treat it purely as "reconcile now" — the
	// registry itself remains the source of truth, because Pub/Sub is
	// fire-and-forget: a message published while the simulator is down is
	// gone forever, and only the next reconcile poll would catch up.
	ChannelSensorsChanged = "sensors.changed"
)

// ErrNotFound is returned when a sensor id is not in the registry. Callers
// match it with errors.Is — the gateway turns it into a 404.
var ErrNotFound = errors.New("sensor not found")

// Registry stores sensors in Redis. It is safe for concurrent use: the
// *redis.Client manages its own connection pool internally, so one Registry
// is shared by all of a service's goroutines.
type Registry struct {
	rdb *redis.Client
}

// New wraps an already-configured Redis client. Taking the client (rather
// than an address) keeps connection config — timeouts, pool size — in main
// where the rest of the service's config lives.
func New(rdb *redis.Client) *Registry {
	return &Registry{rdb: rdb}
}

// Add validates s and stores it, then notifies subscribers.
//
// The hash write and the index write travel in one TxPipeline: both commands
// are sent in a single round-trip and applied atomically (MULTI/EXEC), so no
// reader can ever observe the hash without its index entry or vice versa.
func (r *Registry) Add(ctx context.Context, s Sensor) error {
	if err := s.Validate(); err != nil {
		// Both %w verbs matter: the error matches ErrInvalidSensor (the
		// category, for HTTP status mapping) AND the specific field sentinel
		// like events.ErrOutOfBounds (the cause) via errors.Is.
		return fmt.Errorf("%w: %w", ErrInvalidSensor, err)
	}

	pipe := r.rdb.TxPipeline()
	pipe.HSet(ctx, keyPrefix+s.ID, s.toMap())
	pipe.SAdd(ctx, setKey, s.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("store sensor %s: %w", s.ID, err)
	}

	// Published AFTER the write is committed, so a subscriber that reconciles
	// immediately is guaranteed to see the new sensor in its List call. A
	// publish failure is logged by callers at worst — the poll loop is the
	// correctness mechanism, so a lost notification only costs latency.
	if err := r.rdb.Publish(ctx, ChannelSensorsChanged, s.ID).Err(); err != nil {
		return fmt.Errorf("notify add of %s: %w", s.ID, err)
	}
	return nil
}

// Get fetches one sensor by id, returning ErrNotFound if it doesn't exist.
func (r *Registry) Get(ctx context.Context, id string) (Sensor, error) {
	m, err := r.rdb.HGetAll(ctx, keyPrefix+id).Result()
	if err != nil {
		return Sensor{}, fmt.Errorf("get sensor %s: %w", id, err)
	}
	// The classic go-redis trap: HGETALL on a missing key is NOT an error —
	// Redis just returns an empty hash. "Not found" is our concept, so we
	// detect it ourselves and give it a matchable name.
	if len(m) == 0 {
		return Sensor{}, fmt.Errorf("sensor %s: %w", id, ErrNotFound)
	}
	return sensorFromMap(m)
}

// List returns every registered sensor.
//
// SMEMBERS gives the ids, then all HGETALLs go through one pipeline: one
// network round-trip instead of one per sensor. At the Phase 6 stress-test
// scale of 2,000 sensors, that is the difference between ~1ms and ~2s per
// reconcile tick.
func (r *Registry) List(ctx context.Context) ([]Sensor, error) {
	ids, err := r.rdb.SMembers(ctx, setKey).Result()
	if err != nil {
		return nil, fmt.Errorf("list sensor ids: %w", err)
	}

	pipe := r.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, keyPrefix+id)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("fetch sensors: %w", err)
	}

	sensors := make([]Sensor, 0, len(ids))
	for i, cmd := range cmds {
		m, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("fetch sensor %s: %w", ids[i], err)
		}
		// An id in the set whose hash is gone means we raced a concurrent
		// Remove between SMEMBERS and HGETALL. The sensor is being deleted;
		// skipping it is the correct answer, not an error.
		if len(m) == 0 {
			continue
		}
		s, err := sensorFromMap(m)
		if err != nil {
			return nil, fmt.Errorf("sensor %s: %w", ids[i], err)
		}
		sensors = append(sensors, s)
	}
	return sensors, nil
}

// Remove deletes a sensor and notifies subscribers. The bool reports whether
// the sensor existed — the gateway needs that to choose between 204 and 404
// without a separate Get round-trip.
func (r *Registry) Remove(ctx context.Context, id string) (bool, error) {
	pipe := r.rdb.TxPipeline()
	delCmd := pipe.Del(ctx, keyPrefix+id)
	pipe.SRem(ctx, setKey, id)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, fmt.Errorf("remove sensor %s: %w", id, err)
	}

	// DEL returns how many keys it deleted: 0 means the sensor never existed,
	// and in that case nothing changed, so nobody needs a notification.
	existed := delCmd.Val() > 0
	if !existed {
		return false, nil
	}

	if err := r.rdb.Publish(ctx, ChannelSensorsChanged, id).Err(); err != nil {
		return true, fmt.Errorf("notify removal of %s: %w", id, err)
	}
	return true, nil
}
