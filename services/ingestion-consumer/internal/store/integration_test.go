//go:build integration

// Excluded from `go test` by the build tag; run via `make test-integration`.
// These tests need Docker: they boot a real TimescaleDB per test run, because
// the things they pin — hypertable creation, ON CONFLICT semantics — are
// exactly the parts a mock would fake away.
package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ipko1996/huweathersim/pkg/events"
	"github.com/ipko1996/huweathersim/services/ingestion-consumer/internal/store"
)

// timescaleImage matches deploy/compose/docker-compose.yml exactly — unlike
// the Kafka tests (forced onto Confluent images by the testcontainers
// module), Postgres containers have no such constraint, so tests and compose
// can run the very same image.
const timescaleImage = "timescale/timescaledb:2.17.2-pg16"

// startTimescale boots a throwaway TimescaleDB and returns a connected pool
// with the schema already ensured.
func startTimescale(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx, timescaleImage,
		tcpostgres.WithDatabase("weather"),
		tcpostgres.WithUsername("weather"),
		tcpostgres.WithPassword("weather"),
		// Waits for the "ready to accept connections" log line TWICE —
		// Postgres images restart the server once during init, and the first
		// occurrence is the initdb-time server that immediately goes away.
		tcpostgres.BasicWaitStrategies(),
	)
	// Registered before the error check so a partly-started container can't
	// leak — same pattern as startKafka and startRedis.
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start timescaledb container: %v", err)
	}

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get connection string: %v", err)
	}

	pool, err := store.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := store.EnsureSchema(ctx, pool); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	return pool
}

func testReading(id string, ts time.Time) events.SensorReading {
	return events.SensorReading{
		SensorID: id,
		Lat:      47.4979,
		Lon:      19.0402,
		TempC:    21.5,
		Time:     ts,
	}
}

// TestEnsureSchemaIsIdempotent: running the boot-time migration again (every
// restart does) must be a no-op, not an error.
func TestEnsureSchemaIsIdempotent(t *testing.T) {
	pool := startTimescale(t)
	ctx := context.Background()

	if err := store.EnsureSchema(ctx, pool); err != nil {
		t.Fatalf("second EnsureSchema: %v", err)
	}

	// And the table must actually be a hypertable, not a plain table that
	// happens to accept inserts — this is the one assertion that would catch
	// a silently failed create_hypertable call.
	var count int
	err := pool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.hypertables
		 WHERE hypertable_name = 'readings'`).Scan(&count)
	if err != nil {
		t.Fatalf("query hypertables: %v", err)
	}
	if count != 1 {
		t.Errorf("hypertables named 'readings': got %d, want 1", count)
	}
}

// TestInsertReadingDeduplicates pins the at-least-once contract end to end:
// the same reading inserted twice (= a Kafka redelivery) must land exactly
// once, and a different reading from the same sensor must still land.
func TestInsertReadingDeduplicates(t *testing.T) {
	pool := startTimescale(t)
	s := store.New(pool)
	ctx := context.Background()

	ts := time.Now().UTC().Truncate(time.Millisecond)
	r := testReading("sensor-0001", ts)

	// Insert the same reading twice — the second is what a redelivery after
	// a crash-before-commit looks like to the database.
	if err := s.InsertReading(ctx, r); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := s.InsertReading(ctx, r); err != nil {
		t.Fatalf("redelivered insert: %v", err)
	}

	// A later reading from the same sensor is NOT a duplicate.
	if err := s.InsertReading(ctx, testReading("sensor-0001", ts.Add(5*time.Second))); err != nil {
		t.Fatalf("second reading: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM readings WHERE sensor_id = $1`, "sensor-0001").Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 2 {
		t.Errorf("rows for sensor-0001: got %d, want 2 (duplicate absorbed, distinct kept)", count)
	}
}
