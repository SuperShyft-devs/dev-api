"""Healthians access token should be reused in-process."""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_get_access_token_uses_in_process_cache(monkeypatch):
    from modules.diagnostics.healthians import client as healthians_client

    healthians_client.clear_access_token_cache()
    calls = {"n": 0}

    class _FakeResp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"success": True, "access_token": "tok-1"}

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, *args, **kwargs):
            calls["n"] += 1
            return _FakeResp()

    monkeypatch.setattr(healthians_client.httpx, "AsyncClient", _FakeAsyncClient)

    t1 = await healthians_client.get_access_token()
    t2 = await healthians_client.get_access_token()
    assert t1 == "tok-1"
    assert t2 == "tok-1"
    assert calls["n"] == 1

    healthians_client.clear_access_token_cache()
