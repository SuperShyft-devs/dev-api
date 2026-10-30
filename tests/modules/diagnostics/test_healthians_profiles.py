"""Tests for Healthians profile catalog search."""

from __future__ import annotations

import pytest

from tests.helpers.auth import employee_auth_header, seed_employee


@pytest.mark.asyncio
async def test_search_healthians_profiles(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=950, role="admin")
    response = await async_client.get(
        "/diagnostics/healthians/profiles",
        params={"search": "lipid"},
        headers=employee_auth_header(950),
    )
    assert response.status_code == 200
    profiles = response.json()["data"]["profiles"]
    assert len(profiles) >= 1
    assert any("lipid" in p["name"].lower() for p in profiles)
