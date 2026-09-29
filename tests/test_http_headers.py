"""Security headers on every kind of response: data, errors, probes and metrics."""

from typing import Any

import pytest

from tests.auth_keys import auth_header


@pytest.mark.parametrize(
    ("method", "path", "auth", "status"),
    [
        ("POST", "/posts", True, 201),  # user content, as JSON
        ("GET", "/me/feed", False, 401),  # an error
        ("GET", "/me/feed?limit=0", True, 422),  # a validation error that echoes input
        ("GET", "/health/live", False, 200),
        ("GET", "/metrics", False, 200),
    ],
)
def test_browsers_are_told_not_to_guess_content_types(
    client: Any, method: str, path: str, auth: bool, status: int
) -> None:
    headers = auth_header("alice") if auth else {}
    response = client.request(method, path, json={"text": "<script>alert(1)</script>"}, headers=headers)

    assert response.status_code == status
    assert response.headers["X-Content-Type-Options"] == "nosniff"
