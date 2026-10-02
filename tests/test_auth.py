"""Token verification: only a valid token, from our issuer, for our API, identifies a user."""

import base64
import hashlib
import hmac
import json
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm

from feed_fanout.adapters.auth import AuthConfigError, InvalidToken, JwksKeys, StaticKey, TokenVerifier, build_verifier
from feed_fanout.config import Settings
from tests.auth_keys import AUDIENCE, ISSUER, PUBLIC_KEY, auth_header, new_key_pair, token_for

VERIFIER = TokenVerifier(StaticKey(PUBLIC_KEY), issuer=ISSUER, audience=AUDIENCE)


def test_a_valid_token_identifies_its_user() -> None:
    assert VERIFIER.user_id(token_for("alice")) == "alice"


def test_small_clock_differences_are_tolerated() -> None:
    assert VERIFIER.user_id(token_for("alice", exp=int(time.time()) - 10)) == "alice"


@pytest.mark.parametrize(
    ("why", "token"),
    [
        ("expired", token_for("alice", exp=int(time.time()) - 120)),
        ("another issuer", token_for("alice", iss="someone-else")),
        ("for another API", token_for("alice", aud="another-api")),
        ("no expiry", token_for("alice", exp=None)),
        ("no subject", token_for("alice", sub=None)),
        ("empty subject", token_for("")),
        ("subject too long", token_for("a" * 65)),
        ("signed by someone else's key", token_for("alice", private_key=new_key_pair()[0])),
        ("not a token", "garbage"),
    ],
)
def test_invalid_tokens_are_refused(why: str, token: str) -> None:
    with pytest.raises(InvalidToken):
        VERIFIER.user_id(token)


def _b64(data: dict[str, Any]) -> str:
    return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()


def test_a_tampered_token_is_refused() -> None:
    header, _, signature = token_for("alice").split(".")
    forged_payload = _b64({"sub": "admin", "iss": ISSUER, "aud": AUDIENCE, "exp": int(time.time()) + 900})

    with pytest.raises(InvalidToken):
        VERIFIER.user_id(f"{header}.{forged_payload}.{signature}")


def test_an_unsigned_token_is_refused() -> None:
    # The "alg: none" attack: a token that claims to need no signature.
    claims = {"sub": "admin", "iss": ISSUER, "aud": AUDIENCE, "exp": int(time.time()) + 900}
    unsigned = f"{_b64({'alg': 'none', 'typ': 'JWT'})}.{_b64(claims)}."

    with pytest.raises(InvalidToken):
        VERIFIER.user_id(unsigned)


def test_an_hmac_token_signed_with_the_public_key_is_refused() -> None:
    # The algorithm-confusion attack: the public key is public, so if a verifier let the token
    # pick HS256, anyone could "sign" with it. The algorithm is pinned to RS256.
    claims = {"sub": "admin", "iss": ISSUER, "aud": AUDIENCE, "exp": int(time.time()) + 900}
    header = _b64({"alg": "HS256", "typ": "JWT"})
    body = _b64(claims)

    sig = hmac.new(PUBLIC_KEY, f"{header}.{body}".encode(), hashlib.sha256).digest()  # "signed" with a public key
    forged = f"{header}.{body}.{base64.urlsafe_b64encode(sig).rstrip(b'=').decode()}"

    with pytest.raises(InvalidToken):
        VERIFIER.user_id(forged)


# --- Key rotation through JWKS ------------------------------------------------------


class JwksServer:
    """Serves a JWKS document, like an identity provider does, and lets tests rotate keys."""

    def __init__(self) -> None:
        self.keys: list[dict[str, Any]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                body = json.dumps({"keys": outer.keys}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/.well-known/jwks.json"

    def publish(self, kid: str, public_key: bytes) -> None:
        jwk = json.loads(RSAAlgorithm.to_jwk(RSAAlgorithm(RSAAlgorithm.SHA256).prepare_key(public_key)))
        self.keys.append(jwk | {"kid": kid, "use": "sig", "alg": "RS256"})

    def stop(self) -> None:
        self.server.shutdown()


@pytest.fixture
def jwks() -> Iterator[JwksServer]:
    server = JwksServer()
    yield server
    server.stop()


def test_verifies_with_the_providers_published_keys(jwks: JwksServer) -> None:
    jwks.publish("key-1", PUBLIC_KEY)
    verifier = TokenVerifier(JwksKeys(jwks.url), issuer=ISSUER, audience=AUDIENCE)

    assert verifier.user_id(token_for("alice", kid="key-1")) == "alice"


def test_picks_up_a_rotated_key_without_a_restart(jwks: JwksServer) -> None:
    jwks.publish("key-1", PUBLIC_KEY)
    # (No cooldown here, so the test needn't wait 30 s between the two fetches.)
    verifier = TokenVerifier(JwksKeys(jwks.url, refresh_cooldown=0), issuer=ISSUER, audience=AUDIENCE)
    verifier.user_id(token_for("alice", kid="key-1"))  # keys fetched and cached

    # The provider rotates: a new key appears, and new tokens are signed with it.
    new_private, new_public = new_key_pair()
    jwks.publish("key-2", new_public)

    assert verifier.user_id(token_for("bob", kid="key-2", private_key=new_private)) == "bob"


def test_made_up_key_ids_dont_make_us_refetch_immediately(jwks: JwksServer) -> None:
    # Protection against being used to flood the provider: within the cooldown, an unknown
    # key id is refused from cache, even if the key has since been published.
    jwks.publish("key-1", PUBLIC_KEY)
    verifier = TokenVerifier(JwksKeys(jwks.url, refresh_cooldown=60), issuer=ISSUER, audience=AUDIENCE)
    verifier.user_id(token_for("alice", kid="key-1"))
    new_private, new_public = new_key_pair()
    jwks.publish("key-2", new_public)

    with pytest.raises(InvalidToken):
        verifier.user_id(token_for("bob", kid="key-2", private_key=new_private))


def test_a_token_with_an_unknown_key_is_refused(jwks: JwksServer) -> None:
    jwks.publish("key-1", PUBLIC_KEY)
    verifier = TokenVerifier(JwksKeys(jwks.url), issuer=ISSUER, audience=AUDIENCE)

    with pytest.raises(InvalidToken):
        verifier.user_id(token_for("alice", kid="nobody-published-this", private_key=new_key_pair()[0]))


# --- Configuration guards -----------------------------------------------------------


def test_production_without_a_way_to_verify_tokens_refuses_to_start() -> None:
    with pytest.raises(AuthConfigError):
        build_verifier(Settings(environment="production"))


def test_the_dev_key_is_refused_outside_dev() -> None:
    with pytest.raises(ValueError, match="only allowed with ENVIRONMENT=dev"):
        Settings(environment="production", auth_dev_public_key_file="/keys/public.pem")


def test_dev_uses_the_local_key(tmp_path: Any) -> None:
    key_file = tmp_path / "public.pem"
    key_file.write_bytes(PUBLIC_KEY)
    settings = Settings(
        environment="dev", auth_dev_public_key_file=str(key_file), auth_issuer=ISSUER, auth_audience=AUDIENCE
    )

    assert build_verifier(settings).user_id(token_for("alice")) == "alice"


def test_jwks_takes_precedence_even_in_dev(jwks: JwksServer) -> None:
    jwks.publish("key-1", PUBLIC_KEY)
    settings = Settings(environment="dev", auth_jwks_url=jwks.url, auth_issuer=ISSUER, auth_audience=AUDIENCE)

    assert isinstance(build_verifier(settings).keys, JwksKeys)


# --- The HTTP API -------------------------------------------------------------------

PROTECTED = [
    ("POST", "/posts"),
    ("PUT", "/me/following/alice"),
    ("DELETE", "/me/following/alice"),
    ("GET", "/me/feed"),
]


@pytest.mark.parametrize(("method", "path"), PROTECTED)
def test_every_user_action_needs_a_token(client: Any, method: str, path: str) -> None:
    response = client.request(method, path, json={"text": "hi"})

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"


@pytest.mark.parametrize(("method", "path"), PROTECTED)
def test_an_invalid_token_is_refused_without_saying_why(client: Any, method: str, path: str) -> None:
    expired = token_for("alice", exp=int(time.time()) - 3600)

    response = client.request(method, path, json={"text": "hi"}, headers={"Authorization": f"Bearer {expired}"})

    assert response.status_code == 401
    assert response.json() == {"detail": "invalid token"}  # the reason goes to our logs, not the caller


def test_the_author_is_whoever_the_token_says(client: Any) -> None:
    response = client.post("/posts", json={"text": "hi", "author_id": "mallory"}, headers=auth_header("alice"))

    assert response.status_code == 201
    assert response.json()["author_id"] == "alice"  # an author_id in the body is simply ignored


@pytest.mark.parametrize("path", ["/health/live", "/health/ready", "/metrics"])
def test_probes_and_metrics_need_no_token(client: Any, path: str) -> None:
    assert client.get(path).status_code == 200


def test_the_api_refuses_to_start_without_a_way_to_verify_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    import feed_fanout.adapters.http as http

    monkeypatch.setattr(http, "settings", Settings(environment="production"))  # no JWKS URL configured
    http.token_verifier.cache_clear()
    try:
        with pytest.raises(AuthConfigError), TestClient(http.app):
            pass
    finally:
        http.token_verifier.cache_clear()
