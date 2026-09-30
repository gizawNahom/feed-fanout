"""
Local development only: a stand-in for the identity provider.

    python -m feed_fanout.devtools keys --dir /keys          # make a key pair, if there isn't one
    python -m feed_fanout.devtools token alice --dir /keys   # print an access token for alice
    python -m feed_fanout.devtools tokens --users 100000 --every 10 --dir /keys
                                          # tokens for the load test's users, as JSON

In compose, the dev-keys service runs the first; `make token AS=alice` runs the second. The
API only trusts these tokens with ENVIRONMENT=dev (see config.py); production verifies tokens
from a real provider, through its JWKS URL.
"""

import argparse
import json
import sys
import time
from pathlib import Path

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from feed_fanout.config import settings


def make_keys(directory: Path) -> None:
    private_file, public_file = directory / "private.pem", directory / "public.pem"
    if private_file.exists() and public_file.exists():
        print(f"dev keys already in {directory}")
        return
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_file.write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    private_file.chmod(0o600)
    public_file.write_bytes(
        key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    )
    print(f"made dev keys in {directory}")


def _private_key(directory: Path) -> rsa.RSAPrivateKey:
    key = serialization.load_pem_private_key((directory / "private.pem").read_bytes(), None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise TypeError(f"{directory / 'private.pem'} is not an RSA key (make it with: devtools keys)")
    return key


def make_token(user: str, directory: Path, minutes: int) -> str:
    return _sign(user, _private_key(directory), minutes)


def _sign(user: str, key: rsa.RSAPrivateKey, minutes: int) -> str:
    now = int(time.time())
    claims = {
        "sub": user,
        "iss": settings.auth_issuer,
        "aud": settings.auth_audience,
        "iat": now,
        "exp": now + minutes * 60,
    }
    return jwt.encode(claims, key, algorithm="RS256")


def load_test_user(i: int) -> str:
    """The load test's user ids: u000000, u000001, ... (loadtest/seed.sql makes the same ones)."""
    return f"u{i:06d}"


def make_load_test_tokens(users: int, every: int, directory: Path, minutes: int) -> list[dict[str, str]]:
    """Tokens for every `every`-th user: the load test's active users, spread across the whole
    follower graph (user 0 is the most followed, so a few celebrities are among them)."""
    key = _private_key(directory)
    return [
        {"user": load_test_user(i), "token": _sign(load_test_user(i), key, minutes)} for i in range(0, users, every)
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    keys = commands.add_parser("keys", help="make a dev key pair if missing")
    keys.add_argument("--dir", type=Path, default=Path("/keys"))
    token = commands.add_parser("token", help="print an access token")
    token.add_argument("user")
    token.add_argument("--dir", type=Path, default=Path("/keys"))
    token.add_argument("--minutes", type=int, default=60)
    tokens = commands.add_parser("tokens", help="print tokens for the load test's users, as JSON")
    tokens.add_argument("--users", type=int, required=True, help="how many users the seed made")
    tokens.add_argument("--every", type=int, default=10, help="a token for every n-th user")
    tokens.add_argument("--dir", type=Path, default=Path("/keys"))
    tokens.add_argument("--minutes", type=int, default=360)
    args = parser.parse_args(argv)

    if args.command == "keys":
        make_keys(args.dir)
    elif args.command == "token":
        print(make_token(args.user, args.dir, args.minutes))
    else:
        print(json.dumps(make_load_test_tokens(args.users, args.every, args.dir, args.minutes)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
