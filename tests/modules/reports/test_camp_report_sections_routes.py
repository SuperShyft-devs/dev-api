"""Integration tests for camp report sections routes."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_list_camp_sections_requires_auth(async_client):
    response = await async_client.get("/reports/camp-sections")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_camp_sections_crud(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7301, employee_id=41)
    headers = _auth_header(7301)

    create_res = await async_client.post(
        "/reports/camp-sections",
        headers=headers,
        json={
            "section": "Overview",
            "section_key": "overview",
            "description": "Camp overview section",
        },
    )
    assert create_res.status_code == 201
    section_id = create_res.json()["data"]["report_sections"]
    assert section_id is not None

    dup_res = await async_client.post(
        "/reports/camp-sections",
        headers=headers,
        json={
            "section": "Duplicate",
            "section_key": "overview",
            "description": "Should fail",
        },
    )
    assert dup_res.status_code == 409

    list_res = await async_client.get("/reports/camp-sections?page=1&limit=10", headers=headers)
    assert list_res.status_code == 200
    rows = list_res.json()["data"]
    assert any(row["report_sections"] == section_id for row in rows)
    assert any(row["section_key"] == "overview" for row in rows)

    update_res = await async_client.put(
        f"/reports/camp-sections/{section_id}",
        headers=headers,
        json={"section": "Camp Overview", "description": "Updated"},
    )
    assert update_res.status_code == 200
    updated = update_res.json()["data"]
    assert updated["section"] == "Camp Overview"
    assert updated["description"] == "Updated"

    delete_res = await async_client.delete(f"/reports/camp-sections/{section_id}", headers=headers)
    assert delete_res.status_code == 200
    assert delete_res.json()["data"]["deleted"] is True

    missing_res = await async_client.delete(f"/reports/camp-sections/{section_id}", headers=headers)
    assert missing_res.status_code == 404
