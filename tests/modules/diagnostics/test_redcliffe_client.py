"""Tests for Redcliffe Labs HTTP client error mapping."""

from __future__ import annotations

import pytest

from modules.diagnostics.redcliffe.client import (
    RedcliffeApiError,
    check_location_serviceable,
    get_coordinates,
    is_serviceable_response,
    search_area,
)


@pytest.mark.asyncio
async def test_search_area_success(monkeypatch):
    class FakeResponse:
        status_code = 200

        def json(self):
            return {"results": [{"eloc": "P3I97P", "placeName": "Test"}]}

    class FakeClient:
        async def get(self, *args, **kwargs):
            return FakeResponse()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    monkeypatch.setattr(
        "modules.diagnostics.redcliffe.client.httpx.AsyncClient",
        lambda **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "modules.diagnostics.redcliffe.client.settings.REDCLIFFE_BASE_URL",
        "https://example.test",
    )
    monkeypatch.setattr(
        "modules.diagnostics.redcliffe.client.settings.REDCLIFFE_API_KEY",
        "key",
    )

    data = await search_area(place_query="Sector 62 Noida")
    assert data["results"][0]["eloc"] == "P3I97P"


@pytest.mark.asyncio
async def test_search_area_not_found(monkeypatch):
    class FakeResponse:
        status_code = 200

        def json(self):
            return {"status": "failure", "message": "No record(s) found", "data": []}

    class FakeClient:
        async def get(self, *args, **kwargs):
            return FakeResponse()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    monkeypatch.setattr(
        "modules.diagnostics.redcliffe.client.httpx.AsyncClient",
        lambda **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "modules.diagnostics.redcliffe.client.settings.REDCLIFFE_BASE_URL",
        "https://example.test",
    )

    with pytest.raises(RedcliffeApiError) as exc:
        await search_area(place_query="nowhere")
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_get_coordinates_invalid_eloc(monkeypatch):
    class FakeResponse:
        status_code = 200

        def json(self):
            return {"latitude": "", "longitude": ""}

    class FakeClient:
        async def get(self, *args, **kwargs):
            return FakeResponse()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    monkeypatch.setattr(
        "modules.diagnostics.redcliffe.client.httpx.AsyncClient",
        lambda **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "modules.diagnostics.redcliffe.client.settings.REDCLIFFE_BASE_URL",
        "https://example.test",
    )

    with pytest.raises(RedcliffeApiError):
        await get_coordinates(eloc="BAD000")


@pytest.mark.asyncio
async def test_check_location_serviceable_non_serviceable(monkeypatch):
    class FakeResponse:
        status_code = 200

        def json(self):
            return {"status": "failure", "message": "Location is non-serviceable"}

    class FakeClient:
        async def get(self, *args, **kwargs):
            return FakeResponse()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    monkeypatch.setattr(
        "modules.diagnostics.redcliffe.client.httpx.AsyncClient",
        lambda **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "modules.diagnostics.redcliffe.client.settings.REDCLIFFE_BASE_URL",
        "https://example.test",
    )

    data = await check_location_serviceable(latitude=28.6, longitude=77.2)
    assert is_serviceable_response(data) is False


@pytest.mark.asyncio
async def test_check_location_serviceable_unauthorized(monkeypatch):
    class FakeResponse:
        status_code = 401
        text = "Unauthorized"

    class FakeClient:
        async def get(self, *args, **kwargs):
            return FakeResponse()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    monkeypatch.setattr(
        "modules.diagnostics.redcliffe.client.httpx.AsyncClient",
        lambda **kwargs: FakeClient(),
    )
    monkeypatch.setattr(
        "modules.diagnostics.redcliffe.client.settings.REDCLIFFE_BASE_URL",
        "https://example.test",
    )

    with pytest.raises(RedcliffeApiError) as exc:
        await check_location_serviceable(latitude=28.6, longitude=77.2)
    assert exc.value.status_code == 401
