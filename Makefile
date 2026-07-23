# HU-WeatherSim developer commands.
# Because this is a multi-module Go workspace (one module per service), a plain
# `go test ./...` from the root doesn't reach into the service modules. These
# targets loop over every directory that has a go.mod and run the command there.
# This is exactly what CI (GitHub Actions) will call in Phase 8.

# Every dir containing a go.mod (each service module + shared pkg once it has one).
GO_MODULES := $(shell find services pkg -name go.mod -exec dirname {} \; 2>/dev/null)

# Modules that actually contain Go source. A module scaffolded ahead of its code
# (an empty `pkg/`, say) has a go.mod but no packages, and `go vet ./...` treats
# "matched no packages" as an error — which would fail CI for no real reason.
# The code targets below iterate this filtered list; `tidy` still covers them all.
GO_CODE_MODULES := $(shell for m in $(GO_MODULES); do \
	[ -n "$$(find $$m -name '*.go' -print -quit)" ] && echo $$m; done)

.PHONY: help test test-integration test-race vet fmt build tidy check up down up-all down-all logs

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

test: ## Run all Go tests in every module (fast; no Docker needed)
	@for m in $(GO_CODE_MODULES); do echo "==> test $$m"; (cd $$m && go test ./...) || exit 1; done

test-integration: ## Run integration tests (needs Docker; starts real Kafka containers)
	@for m in $(GO_CODE_MODULES); do echo "==> integration $$m"; \
		(cd $$m && go test -tags=integration -timeout 10m ./...) || exit 1; done

test-race: ## Run tests with the race detector (catches concurrency bugs)
	@for m in $(GO_CODE_MODULES); do echo "==> race $$m"; (cd $$m && go test -race ./...) || exit 1; done

vet: ## Run go vet in every module
	@for m in $(GO_CODE_MODULES); do echo "==> vet $$m"; (cd $$m && go vet ./...) || exit 1; done

fmt: ## Format all Go code
	@for m in $(GO_CODE_MODULES); do echo "==> fmt $$m"; (cd $$m && go fmt ./...); done

build: ## Build all modules
	@for m in $(GO_CODE_MODULES); do echo "==> build $$m"; (cd $$m && go build ./...) || exit 1; done

tidy: ## Sync go.mod/go.sum in every module
	@for m in $(GO_MODULES); do echo "==> tidy $$m"; (cd $$m && go mod tidy); done

check: vet test ## Run vet + tests (what CI gates on)

up: ## Start local infra only (Kafka, Redis, TimescaleDB, Kafka UI) — dev loop: go run on host
	cd deploy/compose && docker compose up -d

down: ## Stop local infra
	cd deploy/compose && docker compose down

up-all: ## Start EVERYTHING: infra + all six Go services, built from source
	cd deploy/compose && docker compose --profile app up -d --build

down-all: ## Stop everything, including the app services
	cd deploy/compose && docker compose --profile app down

logs: ## Follow logs of the whole stack
	cd deploy/compose && docker compose --profile app logs -f --tail=50
