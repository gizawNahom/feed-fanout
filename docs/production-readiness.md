# Production readiness

What "production ready" means for this service, claim by claim, each with the evidence behind
it: a test, a CI job, a measurement or a document. What it doesn't cover yet is under
[Known limitations](#known-limitations).

CI jobs below are in [`ci.yml`](../.github/workflows/ci.yml) (every push and pull request) and
[`security.yml`](../.github/workflows/security.yml) (every push, and weekly).

## Behavior

| Claim | Evidence |
|---|---|
| The rules hold: posting, following, reading, paging, posting limits | Acceptance scenarios in [`tests/acceptance/`](../tests/acceptance/), each run twice: on the real stack (HTTP, Postgres, Kafka) and in memory |
| The fast in-memory tests can be trusted | Contract tests run the same checks on Postgres and the in-memory fakes ([`tests/contracts/`](../tests/contracts/)) |
| The tests would notice a broken rule | Mutation testing of the core, 100% of mutants detected, enforced in CI (job *Mutation testing*) |
| A minimum share of the code is exercised by tests | Coverage floor, enforced in CI (job *Tests*) |
| Works on the supported Pythons | Tests on 3.12 and 3.14 (job *Tests*); strict type checking (job *Lint and type-check*) |
| The database schema is exactly what the code expects | `test_migrations.py`: migrated schema compared with the models; every test database is built by the real migrations |

## Reliability

| Claim | Evidence |
|---|---|
| A saved post is never lost on its way to Kafka | Transactional outbox: `test_outbox.py`; the API doesn't depend on Kafka at all (`test_api_does_not_depend_on_kafka`) |
| An event processed twice adds nothing twice | `test_fanout.py::test_reprocessing_same_event_adds_no_duplicates` (fanout is idempotent) |
| An offset is committed only once its event is handled | `test_offset_is_committed_after_dead_lettering`, `test_worker_joining_mid_stream_loses_nothing` |
| A database outage is waited out, not turned into lost events | `test_database_outage_is_retried_until_it_succeeds`; only connection-type errors count as transient (`test_error_classification.py`) |
| A bad event can't block a partition | Dead-lettered with its context: `test_malformed_event_is_dead_lettered_with_context`, `test_event_for_unknown_post_is_dead_lettered`; replay tool: `test_replay_dlt.py` |
| Deploys and scaling lose nothing | SIGTERM finishes, commits and leaves the group (`test_shutdown.py`); rebalances are tolerated (`test_commit_rejected_after_reassignment_does_not_crash`) |
| Survives losing a Kafka broker; refuses unsafe writes when two are down, without losing posts | [`scripts/failover_test.sh`](../scripts/failover_test.sh), on a 3-broker cluster (job *Broker failover*) |
| The whole stack works end to end | [`scripts/smoke_test.sh`](../scripts/smoke_test.sh): follow, post, the post reaches the feed (job *End-to-end*) |

## Data

| Claim | Evidence |
|---|---|
| The database can be restored to any second since the oldest backup; at most ~60 s lost if the machine is | pgBackRest, continuous WAL archiving + daily full backups ([`docker-compose.backup.yml`](../docker-compose.backup.yml)) |
| The restore works, and the app works on the restored database | [`scripts/restore_test.sh`](../scripts/restore_test.sh): `DELETE FROM posts`, restore to the second before, check (job *Backup and point-in-time restore*) |
| Backups are encrypted, and noticed when they stop | AES-256 repository; alerts `WALArchivingFailing`, `BackupsStale`, with promtool tests |
| Someone can do it under pressure | [Restore runbook](runbooks/restore.md), the same script the CI job runs |

## Security

| Claim | Evidence |
|---|---|
| Only verified users act, and only as themselves | RS256 JWTs from an identity provider, keys rotated through JWKS; no user id in bodies or paths. `test_auth.py`: expired, tampered, unsigned and HMAC-forged tokens refused; every user route needs a token |
| No production deploy runs on development defaults | `DATABASE_URL` required outside dev (`test_config.py`); the dev signing key refused outside dev, and the API refuses to start without a way to verify tokens (`test_auth.py`) |
| One user can't flood everyone's feeds | Posting limit (token bucket, 429 + Retry-After), held under concurrency: `test_posting_limits.py`, `test_posting_buckets.py` |
| No known vulnerable dependencies or image packages; no committed secrets | pip-audit, Trivy (image, secrets), weekly too (`security.yml`); ruff's security rules (job *Lint*); Dependabot; actions pinned to commit SHAs |
| Injection and XSS | Bound parameters only (ruff S608); JSON only, `nosniff` on every response (`test_http_headers.py`). See [SECURITY.md](../SECURITY.md) |
| A small attack surface | Image runs as an unprivileged user, without pip; dev ports bound to 127.0.0.1; only the public API is routed through the gateway, `/metrics` stays internal (`scripts/gateway_test.sh`, job *API gateway*) |
| Abuse at the edge is cut off before the API | Kong refuses oversized bodies (413) and bursts beyond a global rate limit (429, with a retry hint): `scripts/gateway_test.sh` |

## Capacity

| Claim | Evidence |
|---|---|
| Capacity is known, on a stated budget | [Baseline load test](performance/2026-09-30-baseline/README.md): open-model k6 against 100,000 users and 25 million feed rows, each service capped |
| Where the CPU goes is known, and the biggest cost was fixed | [Profile and fix](performance/2026-10-02-api-overhead/README.md): 7.3 → 3.3 ms of CPU per request |
| Overload degrades gracefully, without errors | [Overload runs](performance/2026-10-02-overload/README.md): at 400 requests/s offered, 146 requests/s served, the rest turned away with 503, 0 errors; `test_overload.py` |
| Capacity scales out | The gateway balances across API instances and picks up new ones while running (`scripts/gateway_test.sh`: 2 instances; 2 → 3 split 21/19/20). Capacity with 2 instances: next load run |
| Database connections are reused, not churned | Pool settings (`test_config.py::test_database_connections_are_kept_and_reused`); measured session counts in the overload entry |
| Posts reach feeds quickly: SLO 99% within 2 s | Measured p99 ~1 s ([performance log](performance/)); burn-rate alert `PostToFeedSLOBurn` |

## Operability

| Claim | Evidence |
|---|---|
| Problems are visible | Prometheus metrics, a Grafana dashboard as code, logs in Loki searchable by `post_id` (`test_metrics.py`, `test_logs.py`) |
| Alerts fire when they should, and only then | promtool unit tests for every rule (job *Lint*); the metric contract test checks every metric and histogram bucket the rules and dashboard query exists (`test_metric_contract.py`) |
| A stuck worker can't hide | Consumer lag measured from the broker (`test_consumer_group_lag.py`); relay and worker health = ready **and** live |
| Every page has instructions | [Alert runbooks](runbooks/alerts.md) for the paging alerts; [restore runbook](runbooks/restore.md) |
| Misconfiguration fails at startup | Settings validated on start (pydantic-settings, `test_config.py`) |

## Known limitations

See [Known limitations and next steps](../README.md#known-limitations-and-next-steps) in the README:
what this deliberately doesn't do yet, and why it's acceptable for now.
