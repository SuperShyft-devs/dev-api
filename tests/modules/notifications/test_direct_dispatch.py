"""Tests for direct notification dispatch."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from core.config import settings
from modules.notifications.direct_dispatch import DirectNotificationDispatch
from modules.notifications.models import NotificationService


@pytest.fixture(autouse=True)
def _whatapi_log_urls(monkeypatch):
    monkeypatch.setattr(settings, "WHATAPI_DOMAIN", "https://crmapi1.whatapi.in")
    monkeypatch.setattr(settings, "WHATAPI_VERSION", "v19.0")
    monkeypatch.setattr(settings, "WHATAPI_WABA_ID", "waba-123")
    monkeypatch.setattr(settings, "WHATAPI_PHONE_NUMBER_ID", "phone-999")


@pytest.fixture(autouse=True)
def _mock_integration_sync_logs(monkeypatch):
    async def fake_persist(**kwargs):
        return 1

    async def fake_finalize(**kwargs):
        return None

    monkeypatch.setattr(
        "modules.notifications.direct_dispatch.persist_integration_sync_log_isolated",
        fake_persist,
    )
    monkeypatch.setattr(
        "modules.notifications.direct_dispatch.finalize_integration_sync_log_isolated",
        fake_finalize,
    )

    async def fake_log_template_get(*, operation, **kwargs):
        return await operation()

    monkeypatch.setattr(
        "modules.notifications.direct_dispatch.log_whatapi_get_message_templates",
        fake_log_template_get,
    )


@pytest.mark.asyncio
async def test_whatsapp_post_pending(monkeypatch):
    svc = NotificationService(
        service_key="whatapi-otp",
        display_name="OTP",
        channel="whatsapp",
        webhook_path="/send-otp-whatsapp-v3",
        template_name="otp",
        template_variables={
            "format": "POSITIONAL",
            "kind": "leaf",
            "slots": [{"index": 1, "source": "otp"}],
        },
        is_active=True,
    )

    class FakeClient:
        async def fetch_message_template(self, *, template_name: str):
            return {
                "data": [
                    {
                        "name": template_name,
                        "language": "en",
                        "components": [{"type": "BODY", "text": "Code {{1}}"}],
                    }
                ]
            }

        async def send_template_message(self, **kwargs):
            return {"queue_id": "q-test-001", "message_status": "queued"}

    dispatch = DirectNotificationDispatch(client=FakeClient())
    result = await dispatch.deliver_batch(
        svc=svc,
        notification_id=1,
        members=[{"first_name": "Test", "phone": "8762830757", "otp": "654321"}],
    )
    assert result.status == "pending"
    assert result.queue_id == "q-test-001"


def test_build_member_context_otp_nested_for_catalog_variables():
    from modules.notifications.context_builder import build_member_context
    from modules.notifications.template_variables import interpolate_email_template, resolve_slots

    context = build_member_context(
        {"first_name": "Pratheek", "email": "p@example.com", "otp": "482913"},
    )
    assert context["otp"] == {"otp": "482913"}
    html = interpolate_email_template(
        "<p>Code: {{otp.otp}} and legacy {{otp}}</p>",
        context,
    )
    assert "482913" in html
    assert "{{" not in html

    slots = resolve_slots(
        {"slots": [{"index": 1, "source": "otp.otp"}, {"index": 2, "source": "user.first_name"}]},
        context,
    )
    assert slots == ["482913", "Pratheek"]


def test_interpolate_email_support_placeholders():
    from modules.notifications.context_builder import build_member_context
    from modules.notifications.template_variables import interpolate_email_template

    context = build_member_context(
        {"first_name": "Pratheek", "email": "ops@example.com"},
        participant_details={
            "user": {
                "first_name": "Asha",
                "last_name": "User",
                "name": "Asha User",
                "phone": "8762830777",
            },
            "support": {
                "query_text": "Need help with report",
                "ticket_id": "129",
            },
        },
    )
    html = (
        "Hi {{staff.name}}, customer {{user.name}}, phone {{user.phone}}, "
        "query {{support.query_text}}, otp {{otp}}"
    )
    out = interpolate_email_template(html, {**context, "otp": "999"})
    assert "Pratheek" in out
    assert "Asha User" in out
    assert "8762830777" in out
    assert "Need help with report" in out
    assert "{{" not in out


@pytest.mark.asyncio
async def test_email_uses_staff_address_when_participant_user_present(monkeypatch):
    svc = NotificationService(
        service_key="support-email",
        display_name="Support",
        channel="email",
        email_subject="Alert",
        email_body="Hi",
        is_active=True,
    )
    captured: dict = {}

    async def fake_send_email(**kwargs):
        captured.update(kwargs)
        return {"status": "sent"}

    monkeypatch.setattr(
        "modules.notifications.direct_dispatch.send_email",
        fake_send_email,
    )

    dispatch = DirectNotificationDispatch()
    result = await dispatch.deliver_batch(
        svc=svc,
        notification_id=99,
        members=[{"first_name": "Pratheek", "email": "staff@example.com"}],
        participant_details={
            "user": {"first_name": "Asha", "email": "customer@example.com"},
            "support": {"query_text": "help"},
        },
    )
    assert result.status == "sent"
    assert captured["to"] == "staff@example.com"


@pytest.mark.asyncio
async def test_email_sent_immediately(monkeypatch):
    svc = NotificationService(
        service_key="email-otp",
        display_name="Email OTP",
        channel="email",
        webhook_path="/send-otp-email-v3",
        email_subject="Your code",
        email_body="OTP: {{otp}}",
        is_active=True,
    )

    async def fake_send_email(**kwargs):
        return {"status": "sent"}

    monkeypatch.setattr(
        "modules.notifications.direct_dispatch.send_email",
        fake_send_email,
    )

    dispatch = DirectNotificationDispatch()
    result = await dispatch.deliver_batch(
        svc=svc,
        notification_id=2,
        members=[{"first_name": "Test", "email": "test@example.com", "otp": "111111"}],
    )
    assert result.status == "sent"
    assert result.completed_at is not None
    assert isinstance(result.completed_at, datetime)


@pytest.mark.asyncio
async def test_whatsapp_post_failure_uses_fallback(monkeypatch):
    monkeypatch.setattr(
        settings,
        "WHATAPI_FALLBACK_WEBHOOK_URL",
        "https://webhook.whatapi.in/webhook/test",
    )

    svc = NotificationService(
        service_key="booking-confirmation-whatsapp",
        display_name="Booking",
        channel="whatsapp",
        webhook_path="/booking-confirmation-whatsapp-v1",
        template_name="booking",
        template_variables={
            "slots": [{"index": 1, "source": "user.first_name"}],
        },
        is_active=True,
        require_otp=False,
    )

    class FailingClient:
        async def fetch_message_template(self, *, template_name: str):
            return {"data": [{"components": [{"type": "BODY", "text": "{{1}}"}], "language": "en"}]}

        async def send_template_message(self, **kwargs):
            raise RuntimeError("POST failed")

    async def fake_fallback(**kwargs):
        return {"accepted": True, "queue_id": None, "response": {"accepted": True}, "url": "http://x"}

    monkeypatch.setattr(
        "modules.notifications.direct_dispatch.send_whatsapp_fallback_get",
        fake_fallback,
    )

    dispatch = DirectNotificationDispatch(client=FailingClient())
    result = await dispatch.deliver_batch(
        svc=svc,
        notification_id=3,
        members=[{"first_name": "Test", "phone": "8762830757", "otp": "654321"}],
    )
    assert result.status == "sent"
    assert result.queue_id is None


@pytest.mark.asyncio
async def test_whatsapp_otp_post_failure_skips_fallback(monkeypatch):
    monkeypatch.setattr(
        settings,
        "WHATAPI_FALLBACK_WEBHOOK_URL",
        "https://webhook.whatapi.in/webhook/test",
    )

    svc = NotificationService(
        service_key="whatapi-otp",
        display_name="OTP",
        channel="whatsapp",
        webhook_path="/send-otp-whatsapp-v3",
        template_name="otpmsg",
        template_variables={"slots": [{"index": 1, "source": "otp.otp"}]},
        is_active=True,
        require_otp=True,
    )

    class FailingClient:
        async def fetch_message_template(self, *, template_name: str):
            return {"data": [{"components": [{"type": "BODY", "text": "{{1}}"}], "language": "en"}]}

        async def send_template_message(self, **kwargs):
            raise RuntimeError("POST failed")

    fallback_called = False

    async def fake_fallback(**kwargs):
        nonlocal fallback_called
        fallback_called = True
        return {"accepted": True, "queue_id": None, "response": {"accepted": True}, "url": "http://x"}

    monkeypatch.setattr(
        "modules.notifications.direct_dispatch.send_whatsapp_fallback_get",
        fake_fallback,
    )

    dispatch = DirectNotificationDispatch(client=FailingClient())
    result = await dispatch.deliver_batch(
        svc=svc,
        notification_id=4,
        members=[{"first_name": "Test", "phone": "8762830757", "otp": "654321"}],
    )
    assert result.status == "failed"
    assert fallback_called is False
