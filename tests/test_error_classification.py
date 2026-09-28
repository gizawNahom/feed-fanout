"""Which database errors are retried forever (transient) vs. dead-lettered after a few tries."""

import psycopg
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

import feed_fanout.worker
from feed_fanout.kafka import headers_of
from feed_fanout.retry import is_transient
from feed_fanout.worker import Outcome
from tests.conftest import TopicReader, Worker
from tests.helpers import create_post, follow


def raised_by(engine: Engine, sql: str) -> DBAPIError:
    """Run SQL against a real Postgres and return the error it raises."""
    with pytest.raises(DBAPIError) as e, engine.connect() as conn:
        for statement in sql.split(";"):
            conn.execute(text(statement))
    return e.value


def test_connection_refused_is_transient() -> None:
    engine = create_engine("postgresql+psycopg://nobody:nothing@localhost:1/none")
    assert is_transient(raised_by(engine, "SELECT 1"))


def test_statement_timeout_is_not_transient(engine: Engine) -> None:
    # A query that times out will likely time out again: retrying it forever would block the
    # partition. It must go down the "unexpected" path (a few retries, then dead-letter).
    error = raised_by(engine, "SET statement_timeout = 10; SELECT pg_sleep(1)")

    assert isinstance(error, OperationalError)  # same exception class as a connection failure...
    assert getattr(error.orig, "sqlstate", None) == "57014"  # ...told apart by its SQLSTATE
    assert not is_transient(error)


def test_constraint_violation_is_not_transient(engine: Engine) -> None:
    assert not is_transient(raised_by(engine, "INSERT INTO follows (follower_id, followee_id) VALUES ('a', 'a')"))


@pytest.mark.parametrize("sqlstate", ["40001", "40P01", "57P01", "08006"])
def test_retryable_sqlstates_are_transient(sqlstate: str) -> None:
    error = OperationalError("SELECT 1", {}, psycopg.errors.lookup(sqlstate)())
    assert is_transient(error)


def test_non_database_errors_are_not_transient() -> None:
    assert not is_transient(ValueError("bug"))


def test_statement_timeout_during_fanout_is_dead_lettered(
    client: TestClient,
    worker: Worker,
    read_dead_letters: TopicReader,
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    # Given fanout queries that always hit the statement timeout (e.g. a huge fanout)
    timeout = raised_by(engine, "SET statement_timeout = 10; SELECT pg_sleep(1)")
    calls: list[int] = []

    def always_times_out(*args, **kwargs):
        calls.append(1)
        raise timeout

    monkeypatch.setattr(feed_fanout.worker, "fan_out", always_times_out)
    follow(client, "bob", "alice")
    create_post(client, "alice")

    # Then the worker gives up after 3 attempts instead of blocking forever
    assert worker.process_next() == Outcome.DEAD_LETTERED
    assert len(calls) == 3
    [dead] = read_dead_letters(1)
    assert dict((k, v.decode()) for k, v in headers_of(dead))["dlt.reason"] == "unexpected"
