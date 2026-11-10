"""Tests for SMTP email sender."""

from __future__ import annotations

import pytest

from core.config import settings
from modules.notifications import email_sender


@pytest.mark.asyncio
async def test_send_email_calls_smtp(monkeypatch):
    monkeypatch.setattr(settings, "SMTP_HOST", "smtp.gmail.com")
    monkeypatch.setattr(settings, "SMTP_PORT", 587)
    monkeypatch.setattr(settings, "SMTP_USERNAME", "support@supershyft.com")
    monkeypatch.setattr(settings, "SMTP_PASSWORD", "secret")
    monkeypatch.setattr(settings, "SMTP_FROM_EMAIL", "support@supershyft.com")
    monkeypatch.setattr(settings, "SMTP_FROM_NAME", "SuperShyft")
    monkeypatch.setattr(settings, "SMTP_USE_TLS", True)

    calls: list[dict] = []

    def fake_send(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(email_sender, "_send_smtp_sync", fake_send)

    result = await email_sender.send_email(
        to="pratheek.fitnastic@gmail.com",
        subject="Test",
        body_text="Hello",
        body_html="<p>Hello</p>",
    )
    assert result == {"status": "sent"}
    assert len(calls) == 1
    assert calls[0]["to"] == "pratheek.fitnastic@gmail.com"
