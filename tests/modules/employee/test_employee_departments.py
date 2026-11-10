"""Employee department assignment API."""

from __future__ import annotations

import pytest

from tests.helpers.auth import employee_auth_header, seed_employee
from tests.helpers.employee_departments import seed_employee_departments


@pytest.mark.asyncio
async def test_put_departments_requires_full_admin(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=701, role="inferior_admin")
    await seed_employee(test_db_session, employee_id=702, role="admin")

    response = await async_client.put(
        "/employees/702/departments",
        headers=employee_auth_header(701),
        json={"departments": ["ops"]},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_put_departments_replaces_m2m(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=711, role="admin")
    await seed_employee(test_db_session, employee_id=712, role="inferior_admin")
    await seed_employee_departments(test_db_session, employee_id=712, departments=["tech"])

    response = await async_client.put(
        "/employees/712/departments",
        headers=employee_auth_header(711),
        json={"departments": ["ops", "design", "ops"]},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["employee_id"] == 712
    assert sorted(data["departments"]) == ["design", "ops"]

    get_res = await async_client.get(
        "/employees/712/departments",
        headers=employee_auth_header(711),
    )
    assert get_res.status_code == 200
    assert sorted(get_res.json()["data"]["departments"]) == ["design", "ops"]
