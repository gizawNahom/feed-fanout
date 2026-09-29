"""
Authentication: who is making this request? Answered by verifying a JWT access token.

We don't manage users or passwords. An identity provider (Auth0, Keycloak, Cognito, ...)
logs users in and signs tokens with its *private* key; we only verify them with its
*public* keys, published at a JWKS URL. A public key can check a token but not create one,
so even a compromised API can't forge identities. The provider rotates its keys: each
token names its key (`kid`), and the JWKS client re-fetches when it sees a new one.

Every token must have: a valid RS256 signature (the algorithm is pinned; "none" and HMAC
are refused), an unexpired `exp`, our issuer (`iss`), our API as audience (`aud`), and a
subject (`sub`: the user id).
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import jwt
from jwt import PyJWKClient

from feed_fanout.config import Settings
from feed_fanout.domain.model import MAX_USER_ID_LENGTH

ALGORITHMS = ["RS256"]  # pinned: never trust the algorithm a token claims for itself
CLOCK_SKEW_SECONDS = 30  # tolerate small clock differences between us and the provider


class InvalidToken(Exception):
    """Missing, malformed, expired, forged, or not meant for us."""


class AuthConfigError(Exception):
    """Authentication is misconfigured: refuse to start rather than run insecurely."""


class KeySource(Protocol):
    def key_for(self, token: str) -> object: ...


class JwksKeys:
    """The provider's public keys, fetched from its JWKS URL, cached, refreshed on rotation.

    A token naming a key we haven't seen triggers a re-fetch, at most once per
    `refresh_cooldown` seconds: otherwise anyone could make us hammer the provider by
    sending tokens with made-up key ids. Providers publish a new key before signing with
    it, so a real rotation is picked up well within the cooldown.
    """

    def __init__(self, jwks_url: str, refresh_cooldown: float = 30.0) -> None:
        self.client = PyJWKClient(jwks_url, cache_keys=True, lifespan=3600, cooldown_duration=refresh_cooldown)

    def key_for(self, token: str) -> object:
        try:
            return self.client.get_signing_key_from_jwt(token).key
        except jwt.PyJWKClientError as e:  # unknown kid, unreachable JWKS, ...
            raise InvalidToken(str(e)) from e


@dataclass(frozen=True)
class StaticKey:
    """One fixed public key: local development and tests only."""

    public_key_pem: bytes

    def key_for(self, token: str) -> object:
        return self.public_key_pem


class TokenVerifier:
    def __init__(self, keys: KeySource, issuer: str, audience: str) -> None:
        self.keys = keys
        self.issuer = issuer
        self.audience = audience

    def user_id(self, token: str) -> str:
        """The verified user id (the token's subject). Raises InvalidToken otherwise."""
        try:
            claims = jwt.decode(
                token,
                self.keys.key_for(token),  # type: ignore[arg-type]
                algorithms=ALGORITHMS,
                issuer=self.issuer,
                audience=self.audience,
                leeway=CLOCK_SKEW_SECONDS,
                options={"require": ["exp", "iss", "aud", "sub"]},
            )
        except jwt.PyJWTError as e:
            raise InvalidToken(str(e)) from e
        sub = claims["sub"]
        if not isinstance(sub, str) or not 1 <= len(sub) <= MAX_USER_ID_LENGTH:
            raise InvalidToken("invalid subject")
        return sub


def build_verifier(settings: Settings) -> TokenVerifier:
    """Production verifies against the provider's JWKS; dev may use a local key, nothing else may."""
    if settings.auth_jwks_url:
        keys: KeySource = JwksKeys(settings.auth_jwks_url)
    elif settings.environment == "dev" and settings.auth_dev_public_key_file:
        keys = StaticKey(Path(settings.auth_dev_public_key_file).read_bytes())
    else:
        raise AuthConfigError(
            "no way to verify tokens: set AUTH_JWKS_URL (production), "
            "or ENVIRONMENT=dev with AUTH_DEV_PUBLIC_KEY_FILE (local development)"
        )
    return TokenVerifier(keys, settings.auth_issuer, settings.auth_audience)
