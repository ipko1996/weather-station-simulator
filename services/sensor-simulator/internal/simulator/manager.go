package simulator

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/ipko1996/huweathersim/pkg/registry"
)

// SensorLister is what the Manager needs from the registry — only List.
// Declared here by the consumer (same idiom as the gateway's SensorStore):
// registry.Registry satisfies it implicitly, and tests satisfy it with an
// in-memory fake, so the reconcile logic is testable without Redis.
type SensorLister interface {
	List(ctx context.Context) ([]registry.Sensor, error)
}

// Manager keeps one running sensor goroutine per registered sensor.
//
// It is a reconcile loop: every tick it asks the registry "what SHOULD be
// running?" and diffs that against "what IS running", starting and stopping
// workers until the two match. This design is what makes the registry the
// single source of truth — a manager that (re)starts after downtime converges
// on the correct fleet from its very first reconcile, with no memory of what
// it missed. (Kubernetes controllers work exactly this way; meeting the
// pattern here makes Phase 4 feel familiar.)
type Manager struct {
	lister   SensorLister
	pub      Publisher
	interval time.Duration

	// kick lets other goroutines request an immediate reconcile (the Redis
	// Pub/Sub watcher will use this). Buffer of exactly 1 makes it coalescing:
	// a hundred kicks during one reconcile collapse into a single pending
	// one, which is fine — reconcile diffs the whole world anyway, so "run
	// once soon" carries all the information a hundred kicks do.
	kick chan struct{}

	// running maps sensor id → that worker's off-switch. NO mutex guards it,
	// on purpose: only the Run goroutine ever touches the map (Kick
	// communicates via the channel instead of reaching into shared state).
	// Single-owner data needs no lock — "share memory by communicating".
	running map[string]context.CancelFunc

	// wg counts live workers so shutdown can wait for all of them to finish
	// their in-flight tick before the caller closes the Kafka producer.
	wg sync.WaitGroup
}

// NewManager wires a manager; interval is the reconcile poll period.
func NewManager(lister SensorLister, pub Publisher, interval time.Duration) *Manager {
	return &Manager{
		lister:   lister,
		pub:      pub,
		interval: interval,
		kick:     make(chan struct{}, 1),
		running:  make(map[string]context.CancelFunc),
	}
}

// Kick requests an immediate reconcile without blocking the caller. The
// select-with-default is the idiom for "send only if there's room": if a kick
// is already pending, this one is redundant and dropped.
func (m *Manager) Kick() {
	select {
	case m.kick <- struct{}{}:
	default:
	}
}

// Run reconciles until ctx is cancelled, then waits for every worker to stop.
//
// The poll ticker is the CORRECTNESS mechanism and Kick is only a latency
// optimization: Redis Pub/Sub is fire-and-forget, so a notification published
// while this service was down is gone forever — but the next poll reads the
// full registry and catches up regardless.
func (m *Manager) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	// Reconcile immediately at boot — sensors registered while this service
	// was down must start now, not one poll interval from now.
	m.reconcile(ctx)

	for {
		select {
		case <-ctx.Done():
			// Every worker ctx is a child of this ctx, so cancellation has
			// already reached all of them — nothing to signal, only to wait.
			// Without this Wait, run()'s `defer producer.Close()` would yank
			// the producer out from under workers mid-publish.
			m.wg.Wait()
			log.Printf("manager: all sensor workers stopped")
			return nil

		case <-ticker.C:
			m.reconcile(ctx)
		case <-m.kick:
			m.reconcile(ctx)
		}
	}
}

// reconcile makes the running fleet match the registry.
func (m *Manager) reconcile(ctx context.Context) {
	sensors, err := m.lister.List(ctx)
	if err != nil {
		// Transient by assumption (Redis restarting, network blip): keep the
		// current fleet running on last known state and let the next tick
		// retry. Stopping workers because the registry was briefly
		// unreachable would turn a Redis hiccup into a readings outage.
		log.Printf("manager: list sensors: %v (keeping current fleet)", err)
		return
	}

	desired := make(map[string]registry.Sensor, len(sensors))
	for _, s := range sensors {
		desired[s.ID] = s
	}

	for id, s := range desired {
		if _, ok := m.running[id]; !ok {
			m.start(ctx, s)
		}
	}
	for id, cancel := range m.running {
		if _, ok := desired[id]; !ok {
			// cancel() closes that one worker's ctx.Done() — its Run returns
			// on the next select pass. Deleting from a map while ranging over
			// it is explicitly safe in Go.
			cancel()
			delete(m.running, id)
			log.Printf("manager: stopping sensor %s (removed from registry)", id)
		}
	}
}

// start launches one worker goroutine for s.
func (m *Manager) start(ctx context.Context, s registry.Sensor) {
	// A per-worker child context is the worker's individual off-switch:
	// cancelling it stops exactly this sensor, while a parent cancellation
	// still propagates to every child — both shutdown paths, one mechanism.
	wctx, cancel := context.WithCancel(ctx)
	m.running[s.ID] = cancel

	sensor := NewSensor(s.ID, s.Lat, s.Lon, s.StartTempC, s.Interval, s.Pattern)

	// Add BEFORE launching the goroutine: if Add happened inside it, a
	// shutdown racing this start could reach wg.Wait() while the counter is
	// still 0 and return before the worker even began.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := sensor.Run(wctx, m.pub); err != nil {
			log.Printf("manager: sensor %s stopped with error: %v", s.ID, err)
		}
	}()
}
