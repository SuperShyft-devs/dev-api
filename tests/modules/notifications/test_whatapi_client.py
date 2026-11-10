"""Tests for WhatAPI HTTP client."""

from __future__ import annotations

import pytest

from core.config import settings
from modules.notifications.whatapi_client import WhatApiClient


@pytest.mark.asyncio
async def test_fetch_message_template_success(monkeypatch):
    monkeypatch.setattr(settings, "WHATAPI_DOMAIN", "https://crmapi1.whatapi.in")
    monkeypatch.setattr(settings, "WHATAPI_VERSION", "v19.0")
    monkeypatch.setattr(settings, "WHATAPI_ACCESS_TOKEN", "test-token")
    monkeypatch.setattr(settings, "WHATAPI_WABA_ID", "waba-123")

    captured: dict = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"data": [{"name": "camp17"}]}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, url, params=None, headers=None):
            captured["url"] = url
            captured["params"] = params
            captured["headers"] = headers
            return FakeResponse()

    monkeypatch.setattr(
        "modules.notifications.whatapi_client.httpx.AsyncClient", FakeClient
    )

    client = WhatApiClient()
    body = await client.fetch_message_template(template_name="camp17")
    assert body["data"][0]["name"] == "camp17"
    assert "waba-123" in captured["url"]
    assert captured["params"] == {"name": "camp17"}


@pytest.mark.asyncio
async def test_send_template_message_parses_queue_id(monkeypatch):
    monkeypatch.setattr(settings, "WHATAPI_DOMAIN", "https://crmapi1.whatapi.in")
    monkeypatch.setattr(settings, "WHATAPI_VERSION", "v19.0")
    monkeypatch.setattr(settings, "WHATAPI_ACCESS_TOKEN", "test-token")
    monkeypatch.setattr(settings, "WHATAPI_PHONE_NUMBER_ID", "phone-999")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "messaging_channel": "whatsapp",
                "message": {
                    "queue_id": "q-abc-123",
                    "message_status": "queued",
                },
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None, headers=None):
            return FakeResponse()

    monkeypatch.setattr(
        "modules.notifications.whatapi_client.httpx.AsyncClient", FakeClient
    )

    client = WhatApiClient()
    result = await client.send_template_message(
        to="919362830757",
        template_name="otp",
    )
    assert result == {"queue_id": "q-abc-123", "message_status": "queued"}
