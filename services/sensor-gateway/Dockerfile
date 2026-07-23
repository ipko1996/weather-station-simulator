# Multi-stage build: the `build` stage carries the full Go toolchain
# (~800MB); the final image carries only the compiled binary on Alpine
# (~15MB). Nothing from the build stage ships except what COPY --from takes.
#
# IMPORTANT — build context is the REPO ROOT, not this directory:
#   docker build -f services/sensor-gateway/Dockerfile .
# because the service's go.mod resolves the shared module via
# `replace ../../pkg`, and Docker can only COPY files inside the context.
# Compose sets this up via `context: ../..` + `dockerfile: ...`.
FROM golang:1.26-alpine AS build

WORKDIR /src

# go.work is deliberately NOT copied: the workspace file lists every module,
# but each image holds only pkg + one service. GOWORK=off makes go ignore any
# workspace and rely purely on the replace directive, which resolves to the
# pkg/ copied below.
ENV GOWORK=off

COPY pkg/ pkg/
COPY services/sensor-gateway/ services/sensor-gateway/

WORKDIR /src/services/sensor-gateway

# CGO_ENABLED=0 produces a fully static binary — no libc dependency, so it
# runs on any base image (even scratch; alpine is kept for a debug shell).
RUN CGO_ENABLED=0 go build -o /bin/app ./cmd/gateway

# Alpine over distroless while learning: `docker exec -it <ctr> sh` works,
# which is worth the few extra MB. Distroless is a Phase 4+ hardening step.
FROM alpine:3.21

COPY --from=build /bin/app /bin/app

ENTRYPOINT ["/bin/app"]
