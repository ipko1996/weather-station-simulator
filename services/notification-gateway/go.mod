module github.com/ipko1996/huweathersim/services/notification-gateway

go 1.26.0

require (
	github.com/coder/websocket v1.8.15
	github.com/go-chi/chi/v5 v5.3.1
	github.com/ipko1996/huweathersim/pkg v0.0.0
)

require (
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/segmentio/kafka-go v0.4.51 // indirect
)

// See sensor-simulator/go.mod: pkg is in-repo and never published, so it is
// resolved from disk rather than from a module proxy.
replace github.com/ipko1996/huweathersim/pkg => ../../pkg
