"""A test identity provider: an RSA key pair made at import time, and tokens signed with it."""

import time
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

ISSUER = "feed-fanout-test"
AUDIENCE = "feed-fanout-api"


def new_key_pair() -> tuple[bytes, bytes]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    public = key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    return private, public


PRIVATE_KEY, PUBLIC_KEY = new_key_pair()


def token_for(user: str, *, private_key: bytes = PRIVATE_KEY, kid: str | None = None, **claims: Any) -> str:
    """A valid access token for `user`, unless claims override something."""
    now = int(time.time())
    payload = {"sub": user, "iss": ISSUER, "aud": AUDIENCE, "iat": now, "exp": now + 900} | claims
    headers = {"kid": kid} if kid else None
    return jwt.encode(
        {k: v for k, v in payload.items() if v is not None}, private_key, algorithm="RS256", headers=headers
    )


def auth_header(user: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token_for(user)}"}
