"""Unit tests for auth providers."""

from __future__ import annotations

import pytest

from core.exceptions import AppError
from modules.auth.providers import WhatApiOtpSender


class _FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _FakeAsyncClient:
    def __init__(self, response: _FakeResponse):
        self._response = response
        self.last_url = None
        self.last_params = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url: str, params: dict):
        self.last_url = url
        self.last_params = params
        return self._response


@pytest.mark.asyncio
async def test_whatapi_sender_sends_expected_query_params(monkeypatch):
    response = _FakeResponse(status_code=200, payload={"accepted": True})
    fake_client = _FakeAsyncClient(response)

    monkeypatch.setattr("modules.auth.providers.httpx.AsyncClient", lambda timeout: fake_client)

    sender = WhatApiOtpSender(webhook_url="https://webhook.whatapi.in/webhook/test-token", country_code="91")
    await sender.send_otp("8058516003", "123456")

    assert fake_client.last_url == "https://webhook.whatapi.in/webhook/test-token"
    assert fake_client.last_params == {"number": "918058516003", "message": "otp,123456"}


@pytest.mark.asyncio
async def test_whatapi_sender_raises_when_not_accepted(monkeypatch):
    response = _FakeResponse(status_code=200, payload={"accepted": False})
    fake_client = _FakeAsyncClient(response)

    monkeypatch.setattr("modules.auth.providers.httpx.AsyncClient", lambda timeout: fake_client)

    sender = WhatApiOtpSender(webhook_url="https://webhook.whatapi.in/webhook/test-token")
    with pytest.raises(AppError) as exc:
        await sender.send_otp("8058516003", "123456")

    assert exc.value.status_code == 503
    assert exc.value.error_code == "EXTERNAL_SERVICE_UNAVAILABLE"
