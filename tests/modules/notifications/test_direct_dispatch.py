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
        service_key="whatapi-otp",
        display_name="OTP",
        channel="whatsapp",
        webhook_path="/send-otp-whatsapp-v3",
        template_name="otp",
        template_variables={
            "slots": [{"index": 1, "source": "otp"}],
        },
        is_active=True,
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
