"""NotificationEngine emit tests."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text

from modules.notifications.binding_resolver import ResolvedBinding
from modules.notifications.engine import NotificationEngine


@pytest.mark.asyncio
async def test_emit_resolves_engagement_override(test_db_session):
    engine = NotificationEngine(
        binding_resolver=MagicMock(),
        recipient_resolver=MagicMock(),
        notifications_service=AsyncMock(),
        notifications_repository=AsyncMock(),
    )
    engine._binding_resolver.resolve = AsyncMock(
        return_value=[
            ResolvedBinding(
                recipient_type="user",
                notification_services=[{"service_key": "pretest-wa"}],
                event_name="Pretest",
            )
        ]
    )
    engine._recipient_resolver.resolve = AsyncMock(
        return_value=MagicMock(user_ids=[1], contacts=[])
    )
    svc = MagicMock()
    svc.is_active = True
    engine._notifications_repository.get_service_by_key = AsyncMock(return_value=svc)
    engine._notifications_service.dispatch = AsyncMock(return_value={"notification_id": 99})

    ids = await engine.emit(
        test_db_session,
        event_code="reminders.pretest",
        engagement_id=10,
        context={"user_id": 1},
    )
    assert ids == [99]
    engine._binding_resolver.resolve.assert_awaited_once()


@pytest.mark.asyncio
async def test_emit_payload_binding_uses_dispatch_to_contacts(test_db_session):
    engine = NotificationEngine(
        binding_resolver=MagicMock(),
        recipient_resolver=MagicMock(),
        notifications_service=AsyncMock(),
        notifications_repository=AsyncMock(),
    )
    engine._binding_resolver.resolve = AsyncMock(
        return_value=[
            ResolvedBinding(
                recipient_type="payload",
                notification_services=[{"service_key": "pay-wa"}],
                event_name="Payment completed",
                recipient_config={"payload_path": "booker.phone", "value_type": "phone"},
            )
        ]
    )
    engine._recipient_resolver.resolve = AsyncMock(
        return_value=MagicMock(user_ids=[], contacts=[{"phone": "9000000001"}])
    )
    svc = MagicMock()
    svc.is_active = True
    engine._notifications_repository.get_service_by_key = AsyncMock(return_value=svc)
    engine._notifications_service.dispatch_to_contacts = AsyncMock(return_value={"notification_id": 42})

    ids = await engine.emit(
        test_db_session,
        event_code="payments.completed",
        context={"booker": {"phone": "9000000001"}},
    )
    assert ids == [42]
    engine._notifications_service.dispatch_to_contacts.assert_awaited_once()
    engine._notifications_service.dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_onboarding_maps_to_users_onboarded(test_db_session):
    from modules.notifications.events_catalog import map_legacy_event_code

    assert map_legacy_event_code("onboarding") == "users.onboarded"
