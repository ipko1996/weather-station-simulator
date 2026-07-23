//go:build e2e

// The Phase 2 exit criterion (ROADMAP.md), executable: adding a sensor via
// the gateway REST API produces readings that land in TimescaleDB and move a
// live average on the notification WebSocket stream.
//
// DELIBERATELY black-box: no imports from pkg/ or the services. The test
// knows only the public surfaces — HTTP on :8080, WebSocket on :8082, SQL on
// :5432 — exactly what a user (or the Phase 8 CI smoke test) sees. The local
// structs below duplicating the wire shapes are therefore a feature, not a
// smell: they pin the EXTERNAL contract independently of the internal types.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
)

const (
	gatewayURL = "http://localhost:8080"
	wsURL      = "ws://localhost:8082/ws"
	dbURL      = "postgres://weather:weather@localhost:5432/weather"
)

type sensor struct {
	ID string `json:"id"`
}

type aggregate struct {
	Scope       string    `json:"scope"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	AvgTempC    float64   `json:"avg_temp_c"`
	Count       int       `json:"count"`
}

// requireStack skips (not fails) when the compose stack isn't up: `make test`
// and CI unit runs must stay green on a machine with nothing running.
func requireStack(t *testing.T) {
	t.Helper()
	resp, err := http.Get(gatewayURL + "/healthz")
	if err != nil {
		t.Skipf("compose stack not running (%v) — start it with `make up-all`", err)
	}
	resp.Body.Close()
}

func createSensor(t *testing.T, body string) sensor {
	t.Helper()
	resp, err := http.Post(gatewayURL+"/api/sensors", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST sensor: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST sensor: status %d", resp.StatusCode)
	}
	var s sensor
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatalf("decode sensor: %v", err)
	}
	// Cleanup registered immediately: whatever happens below, the sensor is
	// removed and the fleet returns to its pre-test state.
	t.Cleanup(func() {
		req, _ := http.NewRequest(http.MethodDelete, gatewayURL+"/api/sensors/"+s.ID, nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	})
	return s
}

func TestPipelineEndToEnd(t *testing.T) {
	requireStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// --- 1. Add a sensor; its readings must reach TimescaleDB. -------------
	s := createSensor(t, `{"lat":47.4979,"lon":19.0402,"start_temp_c":21,"interval":"1s"}`)
	t.Logf("created sensor %s", s.ID)

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect timescaledb: %v", err)
	}
	defer conn.Close(ctx)

	deadline := time.Now().Add(45 * time.Second)
	var rows int
	for time.Now().Before(deadline) {
		if err := conn.QueryRow(ctx,
			`SELECT count(*) FROM readings WHERE sensor_id = $1`, s.ID).Scan(&rows); err != nil {
			t.Fatalf("count readings: %v", err)
		}
		if rows > 3 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if rows <= 3 {
		t.Fatalf("only %d readings in TimescaleDB after 45s — pipeline stalled before the database", rows)
	}
	t.Logf("%d readings persisted", rows)

	// --- 2. Live aggregates must flow on the WebSocket. ---------------------
	ws, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer ws.CloseNow()

	readAggregate := func() aggregate {
		t.Helper()
		// A window closes every 10s; 30s tolerates a just-missed window plus
		// one slow flush without flaking.
		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, data, err := ws.Read(readCtx)
		if err != nil {
			t.Fatalf("read aggregate: %v", err)
		}
		var a aggregate
		if err := json.Unmarshal(data, &a); err != nil {
			t.Fatalf("decode aggregate %s: %v", data, err)
		}
		return a
	}

	first := readAggregate()
	if first.Scope != "national" {
		t.Errorf("scope: got %q, want national", first.Scope)
	}
	if first.Count <= 0 {
		t.Errorf("count: got %d, want > 0", first.Count)
	}
	second := readAggregate()
	if !second.WindowStart.After(first.WindowStart) {
		t.Errorf("windows must advance: first %s, second %s", first.WindowStart, second.WindowStart)
	}
	t.Logf("live aggregates flowing: avg %.2f°C over %d readings", second.AvgTempC, second.Count)

	// --- 3. A hot sensor must pull the live average up. ---------------------
	// 45°C at 1s interval outweighs the fleet's ~15-21°C baseline quickly.
	// The bar (+0.5°C within 5 windows) is deliberately modest: this asserts
	// "new data visibly moves the average", not a precise value — precise
	// windowing math is the window package's unit tests' job.
	baseline := second.AvgTempC
	hot := createSensor(t, `{"lat":46.2530,"lon":20.1414,"start_temp_c":45,"interval":"1s"}`)
	t.Logf("created hot sensor %s (45°C) against baseline %.2f°C", hot.ID, baseline)

	moved := false
	for range 5 {
		a := readAggregate()
		t.Logf("window [%s]: avg %.2f°C (n=%d)", a.WindowStart.Format("15:04:05"), a.AvgTempC, a.Count)
		if a.AvgTempC > baseline+0.5 {
			moved = true
			break
		}
	}
	if !moved {
		t.Fatalf("average never rose above baseline+0.5 (%.2f) within 5 windows", baseline+0.5)
	}
}
