# Shortcuts for common tasks. Every target is a plain command you can also run directly.

CLUSTER := docker compose -f docker-compose.yml -f docker-compose.cluster.yml
BACKUP := docker compose -f docker-compose.yml -f docker-compose.backup.yml
# Every file, so down/reset also clean up whatever overlay was started.
ALL := docker compose -f docker-compose.yml -f docker-compose.cluster.yml -f docker-compose.backup.yml
LOAD := COMPOSE_PROJECT_NAME=feed-fanout-load docker compose -f docker-compose.yml -f docker-compose.loadtest.yml --profile observability --profile loadtest

.PHONY: up up-cluster up-backup up-observability down reset backup restore token logs smoke failover restore-test loadtest-setup loadtest loadtest-down test test-fast mutation lint rules help

up:          ## Whole stack, single Kafka broker
	docker compose up -d --build --wait

up-cluster:  ## Whole stack, 3-broker Kafka cluster (needs a reset when switching: make reset)
	$(CLUSTER) up -d --build --wait

up-backup:   ## Whole stack plus continuous backups (pgBackRest): point-in-time restore
	$(BACKUP) up -d --build --wait

up-observability: ## Whole stack plus Prometheus (:9090), Grafana (:3000), Loki and Alloy
	docker compose --profile observability up -d --build --wait

down:        ## Stop the stack (any overlay, with or without observability), keep the data
	$(ALL) --profile observability down --remove-orphans

reset:       ## Stop the stack and delete its data (and its backups)
	$(ALL) --profile observability down --volumes --remove-orphans

backup:      ## Take a full backup now (stack started with make up-backup)
	$(BACKUP) exec backup pgbackrest --stanza=main --type=full backup

restore:     ## Restore the database: make restore (everything archived) or make restore TO="2026-09-30 10:15:00+00"
	scripts/restore.sh $(if $(TO),"$(TO)")

token:       ## Print a dev access token: make token AS=alice
	@docker compose run --rm --no-deps -T dev-keys python -m feed_fanout.devtools token $(AS)

logs:        ## Follow the app's logs
	docker compose logs -f api relay worker

smoke:       ## End-to-end check: post -> follower's feed
	scripts/smoke_test.sh

failover:    ## Kill brokers in a 3-broker cluster and check nothing is lost (isolated stack)
	scripts/failover_test.sh

restore-test: ## Back up, DELETE FROM posts, restore to the moment before (isolated stack)
	scripts/restore_test.sh

loadtest-setup: ## Load test stack (resource limits), 100k seeded users, tokens: see loadtest/README.md
	loadtest/setup.sh

loadtest:    ## Run the load steps and write a report to loadtest/results/ (after loadtest-setup)
	loadtest/run.sh

loadtest-down: ## Stop the load test stack and delete its data
	$(LOAD) down --volumes --remove-orphans

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
