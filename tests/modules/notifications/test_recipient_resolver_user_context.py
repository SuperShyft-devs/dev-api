"""RecipientResolver participant user_context_key."""

from __future__ import annotations

import pytest

from modules.notifications.recipient_resolver import RecipientResolver


@pytest.mark.asyncio
async def test_user_recipient_prefers_configured_key(test_db_session):
    resolver = RecipientResolver()
    target = await resolver.resolve(
        test_db_session,
        recipient_type="user",
        context={"user_id": 10, "participant_user_id": 20},
        recipient_config={"user_context_key": "participant_user_id"},
    )
    assert target.user_ids == [20]


@pytest.mark.asyncio
async def test_user_recipient_user_ids_key(test_db_session):
    resolver = RecipientResolver()
    target = await resolver.resolve(
        test_db_session,
        recipient_type="user",
        context={"user_id": 10, "user_ids": [30, 31]},
        recipient_config={"user_context_key": "user_ids"},
    )
    assert target.user_ids == [30, 31]


@pytest.mark.asyncio
async def test_user_recipient_resolves_phone_from_context(test_db_session, monkeypatch):
    from modules.users.models import User

    class _FakeUsers:
        async def get_user_by_phone(self, db, phone):
            assert phone == "8762830757"
            return User(user_id=42, phone=phone, email=None)

        async def get_user_by_email(self, db, email):
            return None

    resolver = RecipientResolver(users_repository=_FakeUsers())
    target = await resolver.resolve(
        test_db_session,
        recipient_type="user",
        context={"phone": "8762830757"},
        recipient_config={"user_context_key": "phone"},
    )
    assert target.user_ids == [42]


@pytest.mark.asyncio
async def test_user_recipient_falls_back_without_config(test_db_session):
    resolver = RecipientResolver()
    target = await resolver.resolve(
        test_db_session,
        recipient_type="user",
        context={"user_id": 10, "participant_user_id": 20},
    )
    assert target.user_ids == [10]
