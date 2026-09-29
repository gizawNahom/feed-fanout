"""The full assembly: the real stack. HTTP API, Postgres, outbox, relay, Kafka and worker."""

from typing import Any

from fastapi.testclient import TestClient
from httpx2 import Response
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.models import OutboxEvent
from feed_fanout.relay import OutboxRelay
from feed_fanout.worker import Outcome
from tests.acceptance.contract import FeedPage, Post, Rejected
from tests.conftest import Worker


class FullSystem:
    def __init__(
        self, client: TestClient, relay: OutboxRelay, worker: Worker, session_factory: sessionmaker[Session]
    ) -> None:
        self.client = client
        self.relay = relay
        self.worker = worker
        self.session_factory = session_factory

    def follow(self, user: str, followee: str) -> None:
        self._expect(self.client.put(f"/users/{user}/following/{followee}"), 204)

    def unfollow(self, user: str, followee: str) -> None:
        self._expect(self.client.delete(f"/users/{user}/following/{followee}"), 204)

    def post(self, author: str, text: str = "hello") -> Post:
        body = self._expect(self.client.post("/posts", json={"author_id": author, "text": text}), 201).json()
        return Post(id=body["id"], author_id=body["author_id"], text=body["text"])

    def feed(self, user: str, limit: int = 20, cursor: str | None = None) -> FeedPage:
        params: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        body = self._expect(self.client.get(f"/users/{user}/feed", params=params), 200).json()
        return FeedPage(
            posts=[Post(id=p["id"], author_id=p["author_id"], text=p["text"]) for p in body["items"]],
            next_cursor=body["next_cursor"],
        )

    def settle(self) -> None:
        """Deterministic, no sleeps: publish every outbox event, then process exactly that many."""
        pending = self._outbox_size()
        while self._outbox_size():
            self.relay.publish_batch()
        for _ in range(pending):
            assert self.worker.process_next() == Outcome.FANNED_OUT

    def _outbox_size(self) -> int:
        with self.session_factory() as session:
            return session.scalar(select(func.count()).select_from(OutboxEvent)) or 0

    @staticmethod
    def _expect(response: Response, status: int) -> Response:
        if response.status_code in (400, 422):
            raise Rejected(response.text)
        assert response.status_code == status, response.text
        return response
