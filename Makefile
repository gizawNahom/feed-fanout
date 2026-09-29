# Shortcuts for common tasks. Every target is a plain command you can also run directly.

CLUSTER := docker compose -f docker-compose.yml -f docker-compose.cluster.yml

.PHONY: up up-cluster up-observability down reset logs smoke failover test test-fast mutation lint rules help

up:          ## Whole stack, single Kafka broker
	docker compose up -d --build --wait

up-cluster:  ## Whole stack, 3-broker Kafka cluster (needs a reset when switching: make reset)
	$(CLUSTER) up -d --build --wait

up-observability: ## Whole stack plus Prometheus (:9090), Grafana (:3000), Loki and Alloy
	docker compose --profile observability up -d --build --wait

down:        ## Stop the stack (single broker or cluster, with or without observability), keep the data
	$(CLUSTER) --profile observability down --remove-orphans

reset:       ## Stop the stack and delete its data
	$(CLUSTER) --profile observability down --volumes --remove-orphans

logs:        ## Follow the app's logs
	docker compose logs -f api relay worker

smoke:       ## End-to-end check: post -> follower's feed
	scripts/smoke_test.sh

failover:    ## Kill brokers in a 3-broker cluster and check nothing is lost (isolated stack)
	scripts/failover_test.sh

test:        ## All tests (needs Docker)
	uv run pytest

test-fast:   ## Acceptance scenarios on the in-memory assembly: milliseconds, no Docker
	uv run pytest tests/acceptance -k memory -q

mutation:    ## Mutation testing on domain/ + application/ (fails below the floor)
	uv run python scripts/mutation_check.py

lint:        ## Lint, formatting and types
	uv run ruff check . && uv run ruff format --check . && uv run mypy feed_fanout tests

rules:       ## Validate the observability configs (Prometheus, Loki, Alloy) and unit-test the alert rules
	docker run --rm -v "$$PWD/observability/prometheus:/cfg:ro" -w /cfg --entrypoint promtool prom/prometheus:v3.14.0 check config prometheus.yml
	docker run --rm -v "$$PWD/observability/prometheus:/cfg:ro" -w /cfg --entrypoint promtool prom/prometheus:v3.14.0 test rules rules_test.yml
	docker run --rm -v "$$PWD/observability/loki:/etc/loki:ro" grafana/loki:3.7.8 -config.file=/etc/loki/loki.yml -verify-config
	docker run --rm -v "$$PWD/observability/alloy:/cfg:ro" grafana/alloy:v1.20.1 validate /cfg/config.alloy

help:        ## List targets
	@grep -E '^[a-z-]+:.*## ' Makefile | sed 's/:.*## /\t/'
