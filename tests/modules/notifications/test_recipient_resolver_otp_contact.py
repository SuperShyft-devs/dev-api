"""RecipientResolver contact fallback for OTP emits without user_id."""

from __future__ import annotations

import pytest

from modules.notifications.recipient_resolver import RecipientResolver


@pytest.mark.asyncio
async def test_user_recipient_from_phone_and_email_in_context(test_db_session):
    resolver = RecipientResolver()
    target = await resolver.resolve(
        test_db_session,
        recipient_type="user",
        context={
            "phone": "8762830757",
            "email": "ops@example.com",
            "otp": "123456",
        },
    )
    assert not target.user_ids
    assert len(target.contacts) == 1
    assert target.contacts[0]["phone"] == "8762830757"
    assert target.contacts[0]["email"] == "ops@example.com"


@pytest.mark.asyncio
async def test_user_recipient_staff_participant_details(test_db_session):
    resolver = RecipientResolver()
    target = await resolver.resolve(
        test_db_session,
        recipient_type="user",
        context={
            "participant_details": {
                "staff": {
                    "name": "Jane Doe",
                    "phone": "8762830757",
                    "email": "jane@example.com",
                }
            },
        },
    )
    assert target.contacts == [
        {
            "first_name": "Jane",
            "last_name": "Doe",
            "phone": "8762830757",
            "email": "jane@example.com",
        }
    ]
