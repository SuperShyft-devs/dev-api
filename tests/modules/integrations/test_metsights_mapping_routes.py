"""Tests for integrations mapping metadata routes."""

from __future__ import annotations

import pytest

from tests.helpers.auth import employee_auth_header


@pytest.mark.asyncio
async def test_get_metsights_blood_mapping(async_client, test_db_session):
    from modules.employee.models import Employee
    from modules.users.models import User

    test_db_session.add(User(user_id=99101, phone="9910100000", age=30, status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Employee(
            employee_id=99101,
            name="Mapping Admin",
            phone="9910100001",
            email="mapping@test.example",
            role="admin",
            status="active",
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/integrations/metsights-blood-mapping",
        headers=employee_auth_header(99101),
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert "flow" in body
    assert body["counts"]["key_aliases"] >= 30
    assert any(row["package_code"] == "METSIGHTS_PRO" for row in body["package_matrix"])
    assert body["hormone_placeholders"][0]["unit_code"] in {"1", "2"}
