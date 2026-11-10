"""Tests for staging WhatsApp E2E helpers."""

from __future__ import annotations

from modules.notifications.models import NotificationService
from modules.notifications.staging_e2e_context import (
    e2e_skip_reason,
    merge_e2e_contact,
)


def test_merge_e2e_contact_adds_engagement_block():
    svc = NotificationService(
        service_key="welcome-whatsapp",
        display_name="Welcome",
        channel="whatsapp",
        webhook_path="/x",
    )
    contact = merge_e2e_contact(
        {"first_name": "A", "phone": "8762830757", "email": ""},
        svc,
        phone="8762830757",
        otp="111222",
        participant={},
        session_details=None,
    )
    assert isinstance(contact.get("engagement"), dict)
    assert contact["engagement"].get("engagement_name")


def test_e2e_skip_reason_support_refs():
    svc = NotificationService(
        service_key="support-wa",
        display_name="Support",
        channel="whatsapp",
        webhook_path="/x",
        template_variables={
            "slots": [{"index": 1, "source": "support.ticket_id"}],
        },
    )
    reason = e2e_skip_reason(svc, phone="8762830757", otp="111222", participant={})
    assert reason is not None
    assert "support" in reason.lower()
