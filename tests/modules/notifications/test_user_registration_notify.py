"""Unit tests for users.registered notification emit."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from modules.notifications.user_registration_notify import notify_user_registered


@pytest.mark.asyncio
async def test_notify_user_registered_emits_event(monkeypatch):
    emit_mock = AsyncMock(return_value=[1])
    monkeypatch.setattr(
        "modules.notifications.user_registration_notify.get_notification_engine",
        lambda: SimpleNamespace(emit=emit_mock),
    )
    user = SimpleNamespace(
        user_id=42,
        first_name="Sakshi",
        last_name="Test",
        email="s@test.com",
        phone="9000000001",
        age=21,
        gender="female",
        address=None,
        pin_code=None,
    )

    await notify_user_registered(None, user=user)

    emit_mock.assert_awaited_once()
    kwargs = emit_mock.await_args.kwargs
    assert kwargs["event_code"] == "users.registered"
    assert kwargs["context"]["user_id"] == 42
    assert kwargs["context"]["participant_details"]["name"] == "Sakshi Test"
    assert kwargs["context"]["participant_details"]["phone"] == "9000000001"
