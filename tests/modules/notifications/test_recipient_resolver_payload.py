"""RecipientResolver payload recipient type."""

from __future__ import annotations

import pytest

from modules.notifications.recipient_resolver import RecipientResolver


@pytest.mark.asyncio
async def test_payload_recipient_resolves_mixed_targets(test_db_session):
    resolver = RecipientResolver()
    target = await resolver.resolve(
        test_db_session,
        recipient_type="payload",
        context={
            "phone": "9876543210",
            "user_id": 42,
        },
        recipient_config={"payload_field_ids": ["phone", "user_id"]},
        event_code="auth.otp_sent",
    )
    assert target.user_ids == [42]
    assert any(c.get("phone") == "9876543210" for c in target.contacts)
