# Same shape as services/sensor-gateway/Dockerfile — the full commentary
# (repo-root build context, GOWORK=off, static binary, alpine-vs-distroless)
# lives there and applies here unchanged.
FROM golang:1.26-alpine AS build
WORKDIR /src
ENV GOWORK=off
COPY pkg/ pkg/
COPY services/telemetry-api/ services/telemetry-api/
WORKDIR /src/services/telemetry-api
RUN CGO_ENABLED=0 go build -o /bin/app ./cmd/telemetry

FROM alpine:3.21
COPY --from=build /bin/app /bin/app
ENTRYPOINT ["/bin/app"]
