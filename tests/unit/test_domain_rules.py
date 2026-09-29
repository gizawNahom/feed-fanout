"""The domain rules, at their exact boundaries. Pure: no fixtures, no I/O."""

import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest

from feed_fanout.domain.feed import FeedPosition
from feed_fanout.domain.model import (
    MAX_PAGE_SIZE,
    MAX_POST_LENGTH,
    MAX_USER_ID_LENGTH,
    CannotFollowYourself,
    InvalidCursor,
    InvalidPageSize,
    InvalidPost,
    Post,
    check_can_follow,
    check_page_size,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


# --- Posts ------------------------------------------------------------------------


@pytest.mark.parametrize("author", ["a", "a" * MAX_USER_ID_LENGTH])
def test_author_id_length_limits_are_inclusive(author: str) -> None:
    assert Post.create(author, "hi", NOW).author_id == author


@pytest.mark.parametrize("author", ["", "a" * (MAX_USER_ID_LENGTH + 1)])
def test_author_id_outside_the_limits_is_rejected(author: str) -> None:
    with pytest.raises(InvalidPost) as e:
        Post.create(author, "hi", NOW)
    assert str(e.value) == "author_id must be 1 to 64 characters"


@pytest.mark.parametrize("text", ["x", "x" * MAX_POST_LENGTH])
def test_text_length_limits_are_inclusive(text: str) -> None:
    assert Post.create("alice", text, NOW).text == text


@pytest.mark.parametrize("text", ["", "x" * (MAX_POST_LENGTH + 1)])
def test_text_outside_the_limits_is_rejected(text: str) -> None:
    with pytest.raises(InvalidPost) as e:
        Post.create("alice", text, NOW)
    assert str(e.value) == "text must be 1 to 5000 characters"


def test_a_new_post_gets_a_fresh_id_and_the_given_time() -> None:
    first, second = Post.create("alice", "hi", NOW), Post.create("alice", "hi", NOW)

    assert first.id != second.id
    assert first.created_at == NOW


# --- Follows and pages --------------------------------------------------------------


def test_following_someone_else_is_allowed() -> None:
    check_can_follow("bob", "alice")


def test_following_yourself_is_rejected() -> None:
    with pytest.raises(CannotFollowYourself) as e:
        check_can_follow("bob", "bob")
    assert str(e.value) == "users cannot follow themselves"


@pytest.mark.parametrize("limit", [1, MAX_PAGE_SIZE])
def test_page_size_limits_are_inclusive(limit: int) -> None:
    check_page_size(limit)


@pytest.mark.parametrize("limit", [0, -1, MAX_PAGE_SIZE + 1])
def test_page_size_outside_the_limits_is_rejected(limit: int) -> None:
    with pytest.raises(InvalidPageSize) as e:
        check_page_size(limit)
    assert str(e.value) == "limit must be 1 to 100"


# --- Cursors ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "created_at",
    [
        NOW,
        NOW + timedelta(microseconds=123456),  # precision survives the trip
        datetime(2026, 1, 1, 3, 0, tzinfo=timezone(timedelta(hours=3))),  # so does the offset
    ],
)
def test_a_cursor_round_trips(created_at: datetime) -> None:
    position = FeedPosition(created_at, uuid.uuid4())

    assert FeedPosition.from_cursor(position.to_cursor()) == position


def test_a_cursor_is_url_safe() -> None:
    cursor = FeedPosition(NOW, uuid.uuid4()).to_cursor()

    assert set(cursor) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_=")


def _b64(raw: bytes) -> str:
    import base64

    return base64.urlsafe_b64encode(raw).decode()


@pytest.mark.parametrize(
    "cursor",
    [
        "not-a-cursor",  # not base64
        "",  # empty
        _b64(b"no separator"),
        _b64(b"a|b|c"),  # too many parts
        _b64(f"not-a-date|{uuid.uuid4()}".encode()),
        _b64(f"{NOW.isoformat()}|not-a-uuid".encode()),
        _b64(b"\xff\xfe|\x00"),  # not UTF-8
    ],
)
def test_malformed_cursors_are_rejected(cursor: str) -> None:
    with pytest.raises(InvalidCursor) as e:
        FeedPosition.from_cursor(cursor)
    assert str(e.value) == "invalid cursor"


def test_positions_order_by_time_then_post_id() -> None:
    low, high = sorted([uuid.uuid4(), uuid.uuid4()])

    assert FeedPosition(NOW, high) < FeedPosition(NOW + timedelta(microseconds=1), low)
    assert FeedPosition(NOW, low) < FeedPosition(NOW, high)
