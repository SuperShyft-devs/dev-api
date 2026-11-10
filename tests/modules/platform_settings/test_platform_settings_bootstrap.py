"""Platform settings bootstrap aggregates core GET payloads."""

from __future__ import annotations

import pytest

from tests.helpers.auth import employee_auth_header, seed_employee


@pytest.mark.asyncio
async def test_platform_settings_bootstrap(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=8810, role="admin")
    response = await async_client.get(
        "/platform-settings/bootstrap",
        headers=employee_auth_header(8810),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert "b2c_onboarding" in data
    assert "support_query_notification" in data
    assert "geocoding_provider" in data
    assert "engagement_types" in data
    assert isinstance(data["engagement_types"], list)
