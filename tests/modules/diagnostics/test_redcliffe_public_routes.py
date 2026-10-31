"""Tests for Redcliffe public routes and address serviceability integration."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text


@pytest.mark.asyncio
async def test_redcliffe_search_area_route_writes_sync_log(async_client, test_db_session):
    class FakeResponse:
        status_code = 200

        def json(self):
            return {"results": [{"eloc": "P3I97P"}]}

    class FakeClient:
        async def get(self, *args, **kwargs):
            return FakeResponse()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    with patch(
        "modules.diagnostics.redcliffe.client.httpx.AsyncClient",
        lambda **kwargs: FakeClient(),
    ), patch(
        "modules.diagnostics.redcliffe.client.settings.REDCLIFFE_BASE_URL",
        "https://example.test",
    ), patch(
        "modules.diagnostics.redcliffe.client.settings.REDCLIFFE_API_KEY",
        "key",
    ):
        response = await async_client.get(
            "/diagnostics/public/redcliffe/search-area",
            params={"place_query": "Sector 62"},
        )

    assert response.status_code == 200
    assert response.json()["data"]["results"][0]["eloc"] == "P3I97P"

    rows = await test_db_session.execute(
        text(
            "SELECT provider, status FROM integration_sync_logs "
            "WHERE provider = 'redcliffe' ORDER BY sync_log_id DESC LIMIT 1"
        )
    )
    row = rows.first()
    assert row is not None
    assert row[0] == "redcliffe"
    assert row[1] in (2, "success")


@pytest.mark.asyncio
async def test_redcliffe_serviceability_route(async_client):
    mock_check = AsyncMock(
        return_value={"status": "success", "message": "Location is serviceable"}
    )

    with patch(
        "modules.diagnostics.redcliffe.router.redcliffe_service.check_location_serviceable",
        mock_check,
    ):
        response = await async_client.get(
            "/diagnostics/public/redcliffe/serviceability",
            params={"latitude": 28.6111, "longitude": 77.3689},
        )

    assert response.status_code == 200
    body = response.json()["data"]
    assert body["provider"] == "redcliffe"
    assert body["serviceable"] is True


@pytest.mark.asyncio
async def test_public_serviceability_includes_redcliffe(async_client, test_db_session):
    mock_redcliffe = AsyncMock(
        return_value={"status": "success", "message": "Location is serviceable"}
    )

    with patch(
        "modules.diagnostics.public_service._get_healthians_token",
        new_callable=AsyncMock,
        return_value="token",
    ), patch(
        "modules.diagnostics.public_service.healthians_client.check_serviceability_by_location_v2",
        new_callable=AsyncMock,
        return_value={"status": True, "message": "ok", "data": {"zone_id": "Z1"}},
    ), patch(
        "modules.diagnostics.public_service.orange_health_flow.check_serviceability_at_location",
        new_callable=AsyncMock,
        return_value={"status": "success", "message": "ok"},
    ), patch(
        "modules.diagnostics.public_service.redcliffe_service.check_location_serviceable",
        mock_redcliffe,
    ), patch(
        "modules.diagnostics.public_service.enrich_location_fields",
        new_callable=AsyncMock,
        return_value={"latitude": 28.6, "longitude": 77.2, "city": "Delhi"},
    ):
        response = await async_client.post(
            "/diagnostics/public/serviceability",
            json={
                "address_line": "Test address",
                "city": "Delhi",
                "pincode": "110001",
            },
        )

    assert response.status_code == 200
    providers = response.json()["data"]["providers"]
    assert len(providers) == 3
    by_name = {p["provider"]: p for p in providers}
    assert by_name["redcliffe"]["serviceable"] is True
    mock_redcliffe.assert_awaited_once()
