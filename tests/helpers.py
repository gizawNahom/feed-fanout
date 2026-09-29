from typing import Any

from fastapi.testclient import TestClient

from tests.auth_keys import auth_header


def follow(client: TestClient, follower: str, followee: str) -> None:
    assert client.put(f"/me/following/{followee}", headers=auth_header(follower)).status_code == 204


def create_post(client: TestClient, author: str, text: str = "hello") -> dict[str, Any]:
    response = client.post("/posts", json={"text": text}, headers=auth_header(author))
    assert response.status_code == 201
    body: dict[str, Any] = response.json()
    return body


def feed_ids(client: TestClient, user: str, **params: Any) -> list[str]:
    response = client.get("/me/feed", params=params, headers=auth_header(user))
    assert response.status_code == 200
    return [item["id"] for item in response.json()["items"]]
