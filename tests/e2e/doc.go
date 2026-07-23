// Package e2e holds black-box end-to-end tests for the whole pipeline.
//
// Everything in here is gated behind the `e2e` build tag (the third tier of
// the test pyramid: unit < integration < e2e) and assumes the full compose
// stack is already running — `make up-all` first, then `make test-e2e`.
//
// This file itself carries no build tag ON PURPOSE: it gives the package an
// always-visible declaration so `go vet ./...` and `go test ./...` see a
// valid (empty) package instead of failing with "build constraints exclude
// all Go files".
package e2e
