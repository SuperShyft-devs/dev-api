"""Integration tests for internal employee users routes."""

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


@pytest.mark.asyncio
async def test_employee_create_user_requires_auth(async_client):
    response = await async_client.post("/users", json={"phone": "1234567890"})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_employee_create_user_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=9001, phone="9001000000", status="active"))
    await test_db_session.commit()

    response = await async_client.post("/users", headers=_auth_header(9001), json={"phone": "1234567890"})
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_employee_create_user_creates_user(async_client, test_db_session):
    test_db_session.add(User(user_id=9002, phone="9002000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=1, user_id=9002, role="admin", status="active"))
    await test_db_session.commit()

    payload = {"phone": "5550000000", "first_name": "A", "status": "active"}
    response = await async_client.post("/users", headers=_auth_header(9002), json=payload)

    assert response.status_code == 200
    user_id = response.json()["data"]["user_id"]
    assert isinstance(user_id, int)

    created = await test_db_session.get(User, user_id)
    assert created is not None
    assert created.phone == "5550000000"


@pytest.mark.asyncio
async def test_employee_list_users_paginates_and_filters(async_client, test_db_session):
    test_db_session.add(User(user_id=9003, phone="9003000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=2, user_id=9003, role="admin", status="active"))

    test_db_session.add(User(user_id=9101, phone="9101000000", status="active", city="Pune"))
    test_db_session.add(User(user_id=9102, phone="9102000000", status="inactive", city="Pune"))
    await test_db_session.commit()

    response = await async_client.get("/users?page=1&limit=10&status=active", headers=_auth_header(9003))
    assert response.status_code == 200

    body = response.json()
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 10
    assert body["meta"]["total"] >= 1

    for row in body["data"]:
        assert row["status"] == "active"


@pytest.mark.asyncio
async def test_employee_get_user_returns_details(async_client, test_db_session):
    test_db_session.add(User(user_id=9004, phone="9004000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=3, user_id=9004, role="admin", status="active"))

    test_db_session.add(User(user_id=9201, phone="9201000000", status="active", first_name="X"))
    await test_db_session.commit()

    response = await async_client.get("/users/9201", headers=_auth_header(9004))
    assert response.status_code == 200
    assert response.json()["data"]["user_id"] == 9201
    assert response.json()["data"]["first_name"] == "X"


@pytest.mark.asyncio
async def test_employee_update_user_updates_fields(async_client, test_db_session):
    test_db_session.add(User(user_id=9005, phone="9005000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=4, user_id=9005, role="admin", status="active"))

    test_db_session.add(User(user_id=9301, phone="9301000000", status="active", first_name="Old"))
    await test_db_session.commit()

    payload = {"phone": "9301000000", "first_name": "New", "status": "active"}
    response = await async_client.put("/users/9301", headers=_auth_header(9005), json=payload)
    assert response.status_code == 200

    updated = await test_db_session.get(User, 9301)
    assert updated is not None
    assert updated.first_name == "New"


@pytest.mark.asyncio
async def test_employee_deactivate_user_sets_inactive(async_client, test_db_session):
    test_db_session.add(User(user_id=9006, phone="9006000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=5, user_id=9006, role="admin", status="active"))

    test_db_session.add(User(user_id=9401, phone="9401000000", status="active"))
    await test_db_session.commit()

    response = await async_client.patch("/users/9401/deactivate", headers=_auth_header(9006))
    assert response.status_code == 200

    updated = await test_db_session.get(User, 9401)
    assert updated is not None
    assert (updated.status or "").lower() == "inactive"
