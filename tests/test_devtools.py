"""The dev stand-in for an identity provider: its tokens must pass the API's real verification."""

from pathlib import Path

from feed_fanout.adapters.auth import StaticKey, TokenVerifier
from feed_fanout.config import settings
from feed_fanout.devtools import make_keys, make_load_test_tokens, make_token


def verifier(keys: Path) -> TokenVerifier:
    public_key = (keys / "public.pem").read_bytes()
    return TokenVerifier(StaticKey(public_key), issuer=settings.auth_issuer, audience=settings.auth_audience)


def test_a_dev_token_identifies_its_user(tmp_path: Path) -> None:
    make_keys(tmp_path)

    assert verifier(tmp_path).user_id(make_token("alice", tmp_path, minutes=5)) == "alice"


def test_load_test_tokens_cover_every_nth_user_with_valid_tokens(tmp_path: Path) -> None:
    make_keys(tmp_path)

    tokens = make_load_test_tokens(users=25, every=10, directory=tmp_path, minutes=5)

    assert [t["user"] for t in tokens] == ["u000000", "u000010", "u000020"]
    assert [verifier(tmp_path).user_id(t["token"]) for t in tokens] == ["u000000", "u000010", "u000020"]


def test_keys_are_made_once(tmp_path: Path) -> None:
    make_keys(tmp_path)
    first = (tmp_path / "private.pem").read_bytes()

    make_keys(tmp_path)

    assert (tmp_path / "private.pem").read_bytes() == first
