# Shortcuts for common tasks. Every target is a plain command you can also run directly.

CLUSTER := docker compose -f docker-compose.yml -f docker-compose.cluster.yml

.PHONY: up up-cluster down reset logs smoke failover test lint

up:          ## Whole stack, single Kafka broker
	docker compose up -d --build --wait

up-cluster:  ## Whole stack, 3-broker Kafka cluster (needs a reset when switching: make reset)
	$(CLUSTER) up -d --build --wait

down:        ## Stop the stack (single broker or cluster), keep the data
	$(CLUSTER) down --remove-orphans

reset:       ## Stop the stack and delete its data
	$(CLUSTER) down --volumes --remove-orphans

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

help:        ## List targets
	@grep -E '^[a-z-]+:.*## ' Makefile | sed 's/:.*## /\t/'
