module github.com/ipko1996/huweathersim/services/sensor-simulator

go 1.26.0

require github.com/ipko1996/huweathersim/pkg v0.0.0

require (
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/segmentio/kafka-go v0.4.51 // indirect
)

// The shared pkg module lives in this repo and is never published to a module
// proxy. `replace` points at it on disk so `go mod tidy` and CI builds resolve
// it locally instead of trying to fetch github.com/ipko1996/huweathersim/pkg.
replace github.com/ipko1996/huweathersim/pkg => ../../pkg
