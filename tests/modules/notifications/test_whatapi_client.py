"""Tests for WhatAPI HTTP client."""

from __future__ import annotations

import pytest

from core.config import settings
import httpx

from modules.notifications.whatapi_client import (
    WhatApiClient,
    format_whatapi_http_error,
    parse_http_error_body,
)


def test_format_whatapi_http_error_includes_provider_message():
    msg = format_whatapi_http_error(
        http_method="GET",
        url="https://crmapi1.whatapi.in/api/meta/v19.0/waba/message_templates?name=otp",
        status_code=401,
        body={"success": False, "error": "token invalidated"},
    )
    assert "401" in msg
    assert "token invalidated" in msg


def test_parse_http_error_body_json():
    response = httpx.Response(
        401,
        json={"success": False, "error": "token invalidated"},
        request=httpx.Request("GET", "https://example.com"),
    )
    body = parse_http_error_body(response)
    assert body == {"success": False, "error": "token invalidated"}


@pytest.mark.asyncio
async def test_fetch_message_template_http_401_propagates_body(monkeypatch):
    monkeypatch.setattr(settings, "WHATAPI_DOMAIN", "https://crmapi1.whatapi.in")
    monkeypatch.setattr(settings, "WHATAPI_VERSION", "v19.0")
    monkeypatch.setattr(settings, "WHATAPI_API_KEY", "bad-key")
    monkeypatch.setattr(settings, "WHATAPI_WABA_ID", "waba-123")

    url = "https://crmapi1.whatapi.in/api/meta/v19.0/waba-123/message_templates?name=otp"

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, request_url, headers=None):
            request = httpx.Request("GET", request_url)
            response = httpx.Response(
                401,
                request=request,
                json={"success": False, "error": "token invalidated"},
            )
            raise httpx.HTTPStatusError("401", request=request, response=response)

    monkeypatch.setattr(
        "modules.notifications.whatapi_client.httpx.AsyncClient", FakeClient
    )

    from core.exceptions import AppError

    client = WhatApiClient()
    with pytest.raises(AppError) as exc_info:
        await client.fetch_message_template(template_name="otp")
    assert "token invalidated" in str(exc_info.value)
    assert exc_info.value.details is not None
    assert exc_info.value.details["http_status"] == 401
    assert exc_info.value.details["response_body"] == {
        "success": False,
        "error": "token invalidated",
    }


@pytest.mark.asyncio
async def test_fetch_message_template_success(monkeypatch):
    monkeypatch.setattr(settings, "WHATAPI_DOMAIN", "https://crmapi1.whatapi.in")
    monkeypatch.setattr(settings, "WHATAPI_VERSION", "v19.0")
    monkeypatch.setattr(settings, "WHATAPI_API_KEY", "test-token")
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

        async def get(self, url, headers=None):
            captured["url"] = url
            captured["headers"] = headers
            return FakeResponse()

    monkeypatch.setattr(
        "modules.notifications.whatapi_client.httpx.AsyncClient", FakeClient
    )

    client = WhatApiClient()
    body = await client.fetch_message_template(template_name="camp17")
    assert body["data"][0]["name"] == "camp17"
    assert "waba-123" in captured["url"]
    assert "name=camp17" in captured["url"]
    assert captured["headers"] == {"API-KEY": "test-token"}


@pytest.mark.asyncio
async def test_send_template_message_parses_queue_id(monkeypatch):
    monkeypatch.setattr(settings, "WHATAPI_DOMAIN", "https://crmapi1.whatapi.in")
    monkeypatch.setattr(settings, "WHATAPI_VERSION", "v19.0")
    monkeypatch.setattr(settings, "WHATAPI_API_KEY", "test-token")
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
