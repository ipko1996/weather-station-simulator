module github.com/ipko1996/huweathersim/services/sensor-gateway

go 1.26.0

require (
	github.com/go-chi/chi/v5 v5.3.1
	github.com/ipko1996/huweathersim/pkg v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.21.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace github.com/ipko1996/huweathersim/pkg => ../../pkg
