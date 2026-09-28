# Shortcuts for common tasks. Every target is a plain command you can also run directly.

CLUSTER := docker compose -f docker-compose.yml -f docker-compose.cluster.yml

.PHONY: up up-cluster up-observability down reset logs smoke failover test lint rules help

up:          ## Whole stack, single Kafka broker
	docker compose up -d --build --wait

up-cluster:  ## Whole stack, 3-broker Kafka cluster (needs a reset when switching: make reset)
	$(CLUSTER) up -d --build --wait

up-observability: ## Whole stack plus Prometheus (:9090) and Grafana (:3000)
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

test:        ## Unit and integration tests
	uv run pytest

lint:        ## Lint, formatting and types
	uv run ruff check . && uv run ruff format --check . && uv run mypy feed_fanout tests

rules:       ## Validate the Prometheus config and unit-test the alert rules
	docker run --rm -v "$$PWD/observability/prometheus:/cfg:ro" -w /cfg --entrypoint promtool prom/prometheus:v3.14.0 check config prometheus.yml
	docker run --rm -v "$$PWD/observability/prometheus:/cfg:ro" -w /cfg --entrypoint promtool prom/prometheus:v3.14.0 test rules rules_test.yml

help:        ## List targets
	@grep -E '^[a-z-]+:.*## ' Makefile | sed 's/:.*## /\t/'
