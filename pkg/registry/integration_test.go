//go:build integration

// Excluded from `go test` by the build tag; run via `make test-integration`.
// Same trade-off as the kafkax integration tests: a real Redis in a throwaway
// container means no mocks and no compose dependency, at the cost of a few
// seconds of startup per test run.
package registry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/ipko1996/huweathersim/pkg/registry"
)

// redisImage matches the compose stack (redis:7-alpine) — unlike Kafka, the
// testcontainers Redis module runs the exact image we deploy, so there is no
// version skew to keep in mind here.
const redisImage = "redis:7-alpine"

// startRedis boots a throwaway Redis for one test and returns a connected
// client. Cleanup of both container and client is registered on t.
func startRedis(t *testing.T) *goredis.Client {
	t.Helper()

	ctx := context.Background()

	ctr, err := tcredis.Run(ctx, redisImage)
	// Registered before the error check so a partly-started container can't
	// leak — same pattern as startKafka in pkg/kafkax.
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start redis container: %v", err)
	}

	addr, err := ctr.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("get redis endpoint: %v", err)
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func testSensor(id string) registry.Sensor {
	return registry.Sensor{
		ID:         id,
		Lat:        47.4979,
		Lon:        19.0402,
		StartTempC: 20.0,
		Pattern:    registry.PatternNoisy,
		Interval:   5 * time.Second,
		CreatedAt:  time.Now().UTC().Truncate(time.Millisecond),
	}
}

// TestRegistryRoundTrip walks the full lifecycle against real Redis:
// Add → Get → List → Remove → gone.
func TestRegistryRoundTrip(t *testing.T) {
	reg := registry.New(startRedis(t))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a, b := testSensor("sensor-aaaa"), testSensor("sensor-bbbb")
	for _, s := range []registry.Sensor{a, b} {
		if err := reg.Add(ctx, s); err != nil {
			t.Fatalf("Add(%s): %v", s.ID, err)
		}
	}

	got, err := reg.Get(ctx, a.ID)
	if err != nil {
		t.Fatalf("Get(%s): %v", a.ID, err)
	}
	if !got.CreatedAt.Equal(a.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", got.CreatedAt, a.CreatedAt)
	}
	got.CreatedAt = a.CreatedAt
	if got != a {
		t.Errorf("Get changed the sensor:\ngot  %+v\nwant %+v", got, a)
	}

	list, err := reg.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("List returned %d sensors, want 2", len(list))
	}

	existed, err := reg.Remove(ctx, a.ID)
	if err != nil {
		t.Fatalf("Remove(%s): %v", a.ID, err)
	}
	if !existed {
		t.Errorf("Remove(%s) reported not-found for an existing sensor", a.ID)
	}

	if _, err := reg.Get(ctx, a.ID); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("Get after Remove: got %v, want ErrNotFound", err)
	}
	if list, err = reg.List(ctx); err != nil || len(list) != 1 {
		t.Errorf("List after Remove: got %d sensors (err %v), want 1", len(list), err)
	}
}

// TestRegistryNotFound pins the two not-found behaviors the gateway's HTTP
// statuses will hang off: Get → ErrNotFound, Remove → (false, nil).
func TestRegistryNotFound(t *testing.T) {
	reg := registry.New(startRedis(t))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := reg.Get(ctx, "sensor-ghost"); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("Get(missing): got %v, want ErrNotFound", err)
	}

	existed, err := reg.Remove(ctx, "sensor-ghost")
	if err != nil {
		t.Fatalf("Remove(missing): %v", err)
	}
	if existed {
		t.Errorf("Remove(missing) reported the sensor existed")
	}
}

// TestRegistryWatchDeliversKicks: an Add must reach a running Watch as a kick
// — the pub/sub fast path the simulator's manager hangs off.
func TestRegistryWatchDeliversKicks(t *testing.T) {
	reg := registry.New(startRedis(t))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	kicks := make(chan struct{}, 16)
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- reg.Watch(ctx, func() { kicks <- struct{}{} })
	}()

	// SUBSCRIBE is established asynchronously (Channel() spins up the receive
	// loop in the background), and pub/sub does not queue for late
	// subscribers — so a single immediate Add could be published into the
	// void and hang the test. Publishing repeatedly until the first kick
	// arrives makes the test robust without any sleep guesswork.
	deadline := time.After(10 * time.Second)
	for kicked := false; !kicked; {
		if err := reg.Add(ctx, testSensor("sensor-watch")); err != nil {
			t.Fatalf("Add: %v", err)
		}
		select {
		case <-kicks:
			kicked = true
		case <-time.After(200 * time.Millisecond):
		case <-deadline:
			t.Fatal("no kick within 10s of repeated Adds")
		}
	}

	// Once the subscription is live, delivery is synchronous enough that a
	// single Remove must produce a kick promptly.
	if _, err := reg.Remove(ctx, "sensor-watch"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	select {
	case <-kicks:
	case <-time.After(5 * time.Second):
		t.Fatal("no kick within 5s of Remove")
	}

	// Cancellation must end Watch cleanly — it runs as a bare goroutine in
	// the simulator, so a clean exit is its whole shutdown contract.
	cancel()
	select {
	case err := <-watchDone:
		if err != nil {
			t.Fatalf("Watch returned error on cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Watch did not return within 5s of cancellation")
	}
}

// TestRegistryRejectsInvalidSensor: validation happens at the Add boundary, so
// nothing invalid can ever be stored — downstream readers rely on that.
func TestRegistryRejectsInvalidSensor(t *testing.T) {
	reg := registry.New(startRedis(t))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bad := testSensor("sensor-berlin")
	bad.Lat, bad.Lon = 52.52, 13.405 // Berlin: outside the Hungary bounding box

	if err := reg.Add(ctx, bad); err == nil {
		t.Fatal("Add accepted a sensor outside Hungary")
	}
	if list, err := reg.List(ctx); err != nil || len(list) != 0 {
		t.Errorf("registry not empty after rejected Add: %d sensors (err %v)", len(list), err)
	}
}
