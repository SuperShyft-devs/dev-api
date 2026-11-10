"""Tests for WhatAPI GET fallback."""

from __future__ import annotations

import pytest

from core.config import settings
from modules.notifications.whatsapp_fallback_get import send_whatsapp_fallback_get


@pytest.mark.asyncio
async def test_fallback_get_accepted(monkeypatch):
    monkeypatch.setattr(
        settings,
        "WHATAPI_FALLBACK_WEBHOOK_URL",
        "https://webhook.whatapi.in/webhook/test",
    )

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"accepted": True}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, url):
            return FakeResponse()

    monkeypatch.setattr(
        "modules.notifications.whatsapp_fallback_get.httpx.AsyncClient", FakeClient
    )

    result = await send_whatsapp_fallback_get(
        phone="919362830757",
        message_csv="otp,123456",
    )
    assert result["accepted"] is True
    assert result["queue_id"] is None
