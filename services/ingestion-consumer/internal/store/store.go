// Package store is the ingestion-consumer's TimescaleDB layer: schema setup
// and the one insert the pipeline needs.
//
// Schema management is deliberately an idempotent boot-time migration —
// EnsureSchema mirrors kafkax.EnsureTopic's philosophy exactly: every service
// owns the storage it writes to, creating it on startup if missing, so a
// fresh environment needs zero manual setup. The tempting alternative,
// compose initdb scripts (/docker-entrypoint-initdb.d), has a silent trap:
// they run only when the data volume is FIRST created, so on any machine
// where the volume already exists the script simply never executes.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ipko1996/huweathersim/pkg/events"
)

// NewPool connects to Postgres and verifies the connection.
//
// A pgxpool.Pool is a CONNECTION POOL, not a connection: each query borrows a
// connection and returns it, and the pool is safe for concurrent use — which
// is why one shared pool serves every handler goroutine this service will
// ever run. (This is the machinery Prisma hides under the hood, made
// explicit.)
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	// pgxpool connects lazily; Ping forces the first real connection so a bad
	// DSN or unreachable database fails HERE, at boot, with a clear error —
	// the same fail-fast contract as the gateway's Redis PING.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database unreachable: %w", err)
	}
	return pool, nil
}

// schema is everything the readings table needs, written to be safely
// re-runnable (IF NOT EXISTS everywhere) — running it on every boot is the
// point, not an accident.
//
// Why PRIMARY KEY (sensor_id, ts): it does double duty.
//   - TimescaleDB requires the time-partitioning column (ts) in any unique
//     constraint, because uniqueness is enforced per-chunk.
//   - It is the DEDUPLICATION key for at-least-once delivery: a redelivered
//     reading has the same (sensor_id, ts), so the insert's ON CONFLICT DO
//     NOTHING absorbs it. See InsertReading.
const schema = `
CREATE TABLE IF NOT EXISTS readings (
    sensor_id TEXT             NOT NULL,
    ts        TIMESTAMPTZ      NOT NULL,
    lat       DOUBLE PRECISION NOT NULL,
    lon       DOUBLE PRECISION NOT NULL,
    temp_c    DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (sensor_id, ts)
);

-- create_hypertable turns the plain table into a TimescaleDB hypertable:
-- behind an unchanged SQL interface, rows are routed into time-partitioned
-- chunks, so time-bounded queries scan only the chunks they need and old
-- data can eventually be dropped chunk-by-chunk (retention, later phase).
SELECT create_hypertable('readings', by_range('ts'), if_not_exists => TRUE);
`

// EnsureSchema creates the readings hypertable if it doesn't exist yet.
//
// With a single ingestion replica (Phase 2's compose setup) plain execution
// is enough. Running MULTIPLE replicas of a boot-time migration would need an
// advisory lock so replicas don't race the DDL — noted here because Phase 4+
// scales this service, and this comment is the breadcrumb.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	return nil
}

// Store runs reading queries on a pool it does not own (main owns the pool's
// lifecycle, same as it owns the Kafka consumer's).
type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// InsertReading persists one validated reading.
//
// ON CONFLICT DO NOTHING is the second half of the at-least-once story and
// turns redelivery into a non-event:
//
//	DB error → handler returns error → offset NOT committed → process exits
//	→ restart → Kafka redelivers → same (sensor_id, ts) → conflict → no-op
//
// Net effect: at-least-once delivery, exactly-once STORAGE — achieved by key
// choice alone, no transactions or dedup tables needed.
func (s *Store) InsertReading(ctx context.Context, r events.SensorReading) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO readings (sensor_id, ts, lat, lon, temp_c)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (sensor_id, ts) DO NOTHING`,
		r.SensorID, r.Time, r.Lat, r.Lon, r.TempC,
	)
	if err != nil {
		return fmt.Errorf("insert reading %s@%s: %w", r.SensorID, r.Time, err)
	}
	return nil
}
