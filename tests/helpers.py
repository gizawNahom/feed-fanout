from typing import Any

from fastapi.testclient import TestClient


def follow(client: TestClient, follower: str, followee: str) -> None:
    assert client.put(f"/users/{follower}/following/{followee}").status_code == 204


def create_post(client: TestClient, author: str, text: str = "hello") -> dict[str, Any]:
    response = client.post("/posts", json={"author_id": author, "text": text})
    assert response.status_code == 201
    body: dict[str, Any] = response.json()
    return body


def feed_ids(client: TestClient, user: str, **params: Any) -> list[str]:
    response = client.get(f"/users/{user}/feed", params=params)
    assert response.status_code == 200
    return [item["id"] for item in response.json()["items"]]
