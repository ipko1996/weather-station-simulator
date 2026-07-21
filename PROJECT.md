# Project: HU-WeatherSim

### Simulated Hungarian IoT Weather Sensor Network — Kafka + Kubernetes SRE Portfolio Project

---

## 1. Goal

Build a portfolio project that proves you can design, deploy, scale, and _operate_ an
event-driven microservices system — not just run `docker-compose up`. The product
surface (a live map of Hungary with simulated temperature sensors) is a vehicle;
the real thing being demonstrated is:

- Event-driven architecture with Kafka as the backbone
- A real (if small) multi-service system on Kubernetes, not a monolith
- Autoscaling driven by actual load, not a fake toggle
- Observability and incident response as a first-class feature (the telemetry page),
  not an afterthought
- The judgment to explain _why_ each piece exists — which is what actually gets
  DevOps/Platform/SRE candidates hired

**Elevator pitch for your resume/README:**

> "A simulated national IoT sensor network for Hungary. Users place virtual weather
> sensors on a map; each sensor streams readings through Kafka into a microservices
> pipeline that aggregates live temperature averages. The system autoscales under
> load and exposes a full observability/incident dashboard."

---

## 2. User-Facing Product

### Page 1 — Sensor Map (main SPA view)

- Map of Hungary (Leaflet + OpenStreetMap tiles)
- Click anywhere to drop a sensor pin
- Set a starting temperature + optional drift pattern (steady / rising / falling / noisy)
- Each pin = one simulated "device" that starts emitting readings on an interval
- Live-updating national average temperature, plus regional averages if you cluster
  sensors (e.g., by county)
- A control to bulk-add sensors (e.g., "add 200 sensors") — this is your load-generator
  UI, dual-purposed as a demo feature

### Page 2 — Telemetry / Infra Dashboard

- Service health grid (up/down, pod count per service)
- Kafka consumer lag per topic
- Request rate / error rate / latency (RED metrics) per service
- Active "incidents" — auto-populated when you inject chaos (kill a pod, spike lag)
- Autoscaling events timeline ("API scaled 2→6 pods at 14:32:10")
- Optional: SLO burn-rate indicator (e.g., "99% of readings ingested within 2s")

---

## 3. Numbers (so this isn't hand-wavy)

Pick concrete figures now — interviewers love that you can quantify a system.

| Parameter                                                    | Baseline                                   | Stress test               |
| ------------------------------------------------------------ | ------------------------------------------ | ------------------------- |
| Max simulated sensors                                        | 500                                        | 2,000                     |
| Emit interval per sensor                                     | 5s (configurable 1–30s)                    | 1s                        |
| Peak throughput                                              | 500/5 = **100 msg/s**                      | 2,000/1 = **2,000 msg/s** |
| Avg message size                                             | ~200 bytes (sensor_id, lat, lon, temp, ts) | same                      |
| Peak bandwidth                                               | ~20 KB/s                                   | ~400 KB/s                 |
| Kafka topic partitions (`sensor.readings`)                   | 6                                          | scale to 12 if needed     |
| Retention on raw readings topic                              | 24h (this is a stream, not a data lake)    | —                         |
| Target ingestion latency (sensor → aggregate visible on map) | < 2s p95                                   | —                         |

None of this stresses Kafka itself (Kafka laughs at 2,000 msg/s) — the point isn't
to stress Kafka, it's to stress **your consumer/aggregation services and your HPA/KEDA
config**, which is what you're actually trying to demonstrate.

---

## 4. Microservices

More than two, each with a clear single responsibility — this is the actual meat
of the "I can design a distributed system" story.

1. **sensor-gateway** (REST + WebSocket API)
   Frontend talks only to this. Handles "add sensor", "remove sensor", "set params".
   Registers sensors into a shared store (Redis) and tells the simulator to spin them up.

2. **sensor-simulator**
   The actual IoT simulation. Each active sensor is a lightweight worker (goroutine /
   async task) that publishes a reading to Kafka on its interval. This is the service
   that scales horizontally as sensor count grows — a natural, honest autoscaling target.

3. **ingestion-consumer**
   Consumes `sensor.readings` from Kafka, validates/cleans, writes to TimescaleDB
   (time-series history) and re-publishes a normalized event to `sensor.readings.clean`.

4. **aggregation-service**
   Consumes the clean topic, computes rolling national + regional averages
   (windowed, e.g. 10s tumbling window), publishes results to `weather.aggregates`
   and/or pushes directly over WebSocket to connected frontends.

5. **notification-gateway** (WebSocket push service)
   Subscribes to `weather.aggregates`, fans out live updates to all connected map
   clients. Kept separate from aggregation so it can scale independently based on
   _connected client count_, not message volume — a nice architectural talking point.

6. **telemetry-api**
   Thin API in front of Prometheus (and optionally Loki) that the telemetry page
   queries — pod counts, lag, RED metrics, recent chaos/incident events.

That's 6 services with genuinely different scaling drivers (sensor count, message
volume, client connections, query load) — which is exactly the kind of variety that
makes autoscaling demos interesting instead of trivial.

---

## 5. Architecture

```
                         ┌─────────────────────────┐
   Browser (React SPA)   │  Page 1: Map   Page 2: Telemetry
                         └──────────┬───────────────┘
                                    │ HTTPS / WSS
                                    ▼
                         nginx ingress controller
                          (TLS termination, routing)
                                    │
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
     sensor-gateway API    notification-gateway (WS)   telemetry-api
              │                     ▲                     │
              │ registers sensors   │ pushes aggregates    │ queries
              ▼                     │                     ▼
        Redis (sensor registry)     │              Prometheus / Loki
              │                     │
              ▼                     │
     sensor-simulator (N workers) ──┘
              │  produces
              ▼
   ┌────────────────────────────┐
   │   Kafka (Strimzi on K8s)   │
   │  sensor.readings           │
   │  sensor.readings.clean     │
   │  weather.aggregates        │
   └───────────┬────────────────┘
               │
     ┌─────────┴──────────┐
     ▼                    ▼
ingestion-consumer   aggregation-service
     │                    │
     ▼                    ▼
TimescaleDB          weather.aggregates → notification-gateway
(history)
```

---

## 6. Stack

| Layer                   | Choice                                                         | Why                                                                                             |
| ----------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Frontend                | React + Leaflet + Tailwind                                     | Leaflet is the standard for map SPAs; lightweight                                               |
| Backend services        | Go (or Node if you're faster in it)                            | Go's concurrency model is a natural fit for simulating thousands of sensor workers              |
| Messaging               | Kafka via **Strimzi operator**                                 | Kubernetes-native Kafka, CRD-driven, industry standard                                          |
| Sensor registry / cache | Redis                                                          | Fast, simple, shows you know when _not_ to reach for Kafka/Postgres                             |
| Historical storage      | TimescaleDB (Postgres extension)                               | Purpose-built for time-series, easy to demo range queries                                       |
| Autoscaling             | HPA (CPU/mem) + **KEDA** (Kafka lag, Redis-based sensor count) | KEDA scaling on Kafka lag is the single most "I know what I'm doing" line on this whole project |
| Ingress                 | nginx ingress controller                                       | TLS termination, path routing to UI/API/WS                                                      |
| Observability           | Prometheus + Grafana + Loki + OpenTelemetry + Jaeger           | Full RED/USE metrics, logs, traces                                                              |
| Chaos                   | Chaos Mesh                                                     | Kill pods / inject latency, feeds the telemetry "incidents" feed                                |
| Testing                 | Go stdlib `testing` + `net/http/httptest` (+ `testcontainers-go` for integration) | Tests are a first-class deliverable — CI runs them on every push before building images |
| CI/CD                   | GitHub Actions → build/test/push images                        | Standard, free, easy to show                                                                    |
| GitOps deploy           | ArgoCD                                                         | Declarative deploys, diff view, rollback story                                                  |
| IaC                     | Terraform (if cloud, e.g. GKE/EKS) or kind/k3d for local       | Depends on budget — see note below                                                              |
| Packaging               | Helm charts per service                                        | Reusable, parameterized, versioned deploys                                                      |

**Cost note:** you can build and demo this entirely on a local cluster (`kind` or
`k3d`) with zero cloud spend. Only stand it up on GKE/EKS if you want to show a
public demo URL — worth doing near the end if budget allows, but not required for
the portfolio value.

### Testing strategy (a first-class part of the scope)

Tests are written *alongside* each service, not bolted on at the end. This is what
lets CI (GitHub Actions) fail a bad change before it ever builds an image, and it's
a strong signal to interviewers that you build production-grade, not demo-grade.

- **Unit tests** — pure logic in isolation: aggregation windowing math, drift-pattern
  generation, validation rules. Fast, no I/O. Go's built-in `testing` package.
- **HTTP handler tests** — exercise each endpoint with `net/http/httptest` (no real
  network, no real server). Start here: the `sensor-gateway` `/healthz` test.
- **Integration tests** — the real payoff for a distributed system: spin up throwaway
  Kafka/Redis/Timescale in a container via `testcontainers-go`, produce a reading, and
  assert it flows through the pipeline. Run in CI, tagged so they can be skipped locally.
- **What CI runs:** `go test ./...` in every service module on every push/PR, plus
  `go vet` and formatting checks — all green before any Docker image is built or pushed.

Guiding rule: **every phase adds tests for what that phase built.** A phase isn't
"done" until its new code has tests and they pass.

---

## 7. The SRE Layer (this is what separates this from a tutorial clone)

- **SLOs**: define 1–2 concrete SLOs (e.g. "99% of readings visible on the map
  within 2s of being sent") and track error budget burn on the telemetry page.
- **RED metrics** on every service: Rate, Errors, Duration — expose via
  `/metrics` (Prometheus client libs), scraped automatically.
- **Consumer lag as a first-class metric** — graph it, alert on it, scale on it.
- **Chaos experiments as demo content**: kill a `sensor-simulator` pod on camera,
  watch K8s reschedule it, watch the telemetry page log the incident and recovery
  time. This is the single best "show, don't tell" moment for an SRE interview.
- **Runbook**: write one real runbook (e.g. "consumer lag > 5000 for aggregation-service")
  with diagnosis steps and remediation. Put it in the repo. Interviewers rarely see
  candidates who've actually written one.
- **Alerting**: Alertmanager → a Slack/Discord webhook when lag or error rate breaches
  threshold.

---

## 8. Build Plan (weekend → 2 weeks)

**Phase 1 (Day 1–2) — Skeleton**

- Local cluster (kind/k3d), Strimzi Kafka up, one topic
- One hardcoded sensor-simulator producing fake readings
- One consumer logging them to stdout
- Confirm the Kafka-on-K8s mechanics work before building anything else

**Phase 2 (Day 3–5) — Full pipeline + basic UI**

- All 6 services stood up, wired through Kafka topics
- React map: click to add sensor → real end-to-end flow to live average on screen
- Redis-backed sensor registry, TimescaleDB for history

**Phase 3 (Day 6–8) — Observability**

- Prometheus + Grafana + Loki stack
- Telemetry page wired to real metrics (not mocked)
- Basic alerting rules

**Phase 4 (Day 9–11) — Autoscaling**

- KEDA scaling `sensor-simulator` on active sensor count (Redis) or custom metric
- KEDA/HPA scaling `ingestion-consumer` / `aggregation-service` on Kafka lag
- Load test: script that adds sensors up to your stress-test number, record scaling
  behavior with graphs (screenshot these for your README — very compelling)

**Phase 5 (Day 12–14) — SRE polish**

- Chaos Mesh experiments + incident logging on telemetry page
- GitOps via ArgoCD, CI via GitHub Actions
- Write the runbook, write the README with architecture diagram + demo GIF/video

If you only have a weekend: do Phase 1–2 fully, and get _one_ good autoscaling demo
working (Phase 4, scoped down) even if observability stays basic. A working, demoable
scaling event beats a lot of half-built dashboards.

---

## 9. What to put in the README (this matters as much as the code)

- Architecture diagram (the one above, cleaned up)
- The numbers table from section 3 — shows you thought about scale, not just "it works"
- A GIF of: adding sensors → watching pods scale → watching a chaos kill → watching recovery
- The one SLO you defined and how you measured it
- "What I'd do differently at 10x scale" — a short section. This single paragraph
  does more for an SRE interview than another 500 lines of YAML.
