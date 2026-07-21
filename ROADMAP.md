# HU-WeatherSim — Build Roadmap

The [vision and architecture](./PROJECT.md) delivered as **sequential learning
milestones**. Each phase produces something that runs, and each phase ships tests for
what it built — so CI stays green the whole way.

**Core principle:** learn one new technology at a time. Build the whole Go + Kafka
pipeline on Docker Compose first, get it working end-to-end, *then* re-package onto
Kubernetes. Observability before autoscaling. Chaos last.

**Status legend:** ✅ done · 🚧 in progress · ⬜ not started

---

## Phase 0 — Foundations & repo skeleton ✅

Monorepo, Go workspace, first service, and local infra.

- Monorepo layout + `go.work` workspace (one Go module per service, shared `pkg/`)
- `sensor-gateway` service with a `GET /healthz` endpoint (stdlib `net/http`, graceful shutdown)
- `deploy/compose/docker-compose.yml`: Kafka (KRaft) + Redis + TimescaleDB + Kafka UI
- **Tests:** HTTP handler test for `/healthz` via `net/http/httptest`
- **Done when:** `docker compose up` starts the infra, and `curl /healthz` returns OK. ✅

## Phase 1 — Kafka fundamentals in Go ⬜

First producer + consumer; the concurrency primitives that power the whole system.

- One producer publishing a fake reading on an interval; one consumer logging it
- Shared `pkg/events` (the wire contract) + `pkg/kafkax` (thin producer/consumer wrappers)
- **Learn:** goroutines, channels, `context` cancellation; Kafka topics, partitions,
  offsets, consumer groups, at-least-once delivery
- **Tests:** unit tests for event (de)serialization; integration test producing→consuming
  against a throwaway Kafka (`testcontainers-go`)
- **Done when:** a message flows producer → Kafka → consumer, visible in the Kafka UI.

## Phase 2 — Full pipeline on Docker Compose ⬜

All 6 Go services wired through Kafka. Introduce the `chi` router here.

- `sensor-gateway` (REST + Redis registry), `sensor-simulator` (goroutine per sensor),
  `ingestion-consumer` (→ TimescaleDB + clean topic), `aggregation-service` (windowed
  averages), `notification-gateway` (WS fan-out), `telemetry-api` (stub)
- **Learn:** interfaces, `sync` primitives, worker lifecycle, JSON marshalling
- **Tests:** unit tests for aggregation windowing + drift patterns; handler tests for the
  gateway API; pipeline integration test
- **Done when:** adding a sensor produces readings that land in TimescaleDB and move a live
  average on the notification WS stream.

## Phase 3 — React frontend (Map page) ⬜

True end-to-end: click the map, watch the average move.

- Vite + React + TS + Tailwind; Leaflet map of Hungary; click-to-add sensor; live average
  over WebSocket; bulk-add control (doubles as the load generator)
- **Tests:** component tests (Vitest + Testing Library) for the map/controls; a smoke test
  of the add-sensor → live-update flow
- **Done when:** clicking the map in the browser starts a sensor and moves the live national
  average on screen.

## Phase 4 — Re-package onto Kubernetes (k3d) ⬜

Same app, now on Kubernetes. This is where the Helm charts appear.

- k3d cluster locally; Strimzi operator for Kafka; Redis + TimescaleDB via Helm
- One Helm chart per service; nginx ingress routing UI / API / WS
- **Learn:** Pods, Deployments, Services, ConfigMaps/Secrets, Ingress, probes, requests/limits
- **Tests:** Helm chart lint + template render; a post-deploy smoke test hitting `/healthz`
  through the ingress
- **Done when:** the Phase 3 app runs on k3d via `kubectl`/Helm, reachable through ingress.

## Phase 5 — Observability ⬜

The telemetry page comes alive with real data.

- `prometheus/client_golang` RED metrics on every service; kube-prometheus-stack; consumer
  lag graphed; Loki logs; OpenTelemetry → Jaeger traces
- `telemetry-api` queries Prometheus for real; Telemetry page shows health grid, lag, RED
- **Tests:** assert every service exposes a scrapeable `/metrics`; telemetry-api query tests
- **Done when:** the telemetry page shows real pod counts, consumer lag, and RED metrics.

## Phase 6 — Autoscaling ⬜

The headline demo: pods scale on real load.

- HPA (CPU) where honest; KEDA scaling consumers on Kafka lag and the simulator on active
  sensor count (Redis)
- Load test ramping to the stress-test numbers; capture scaling graphs for the README
- **Tests:** a load-generator script asserted to drive lag past the scale threshold
- **Done when:** triggering load visibly scales pods on lag/sensor-count, with a graph to prove it.

## Phase 7 — SRE polish ⬜

Chaos, SLOs, alerting, and the runbook.

- Chaos Mesh experiments feeding an incidents feed; 1–2 SLOs + error-budget burn;
  Alertmanager → Discord/Slack; one real runbook (`docs/runbook-consumer-lag.md`)
- **Tests:** an automated chaos experiment asserting the service recovers within the SLO
- **Done when:** killing a pod shows K8s reschedule it *and* the telemetry page logs the
  incident + recovery.

## Phase 8 — Deploy to the VPS (public demo + GitOps) ⬜

- Single-node k3s on the VPS; GitHub Actions builds/tests/pushes images to GHCR; ArgoCD
  syncs Helm charts from the repo; cert-manager + Let's Encrypt for HTTPS
- **CI gate:** `go test ./...` + `go vet` across all modules must pass before any image builds
- **Done when:** a public URL serves the live map, and a git push flows CI → ArgoCD → cluster.

---

## Testing philosophy

Tests are a first-class deliverable, not an afterthought — see
[PROJECT.md § Testing strategy](./PROJECT.md#testing-strategy-a-first-class-part-of-the-scope).
Every phase adds tests for what it built; CI runs `go test ./...` on every module before
building images. Run everything locally with `make test`.
