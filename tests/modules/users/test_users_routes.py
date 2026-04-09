"""Integration tests for users routes."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_get_me_requires_auth(async_client):
    response = await async_client.get("/users/me")
    assert response.status_code == 401
    assert response.json() == {"error_code": "AUTH_FAILED", "message": "Authentication failed"}


@pytest.mark.asyncio
async def test_get_me_returns_profile(async_client, test_db_session):
    user = User(user_id=1010, age=30, phone="5555555555", status="active", first_name="A")
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(1010)
    response = await async_client.get("/users/me", headers=headers)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["user_id"] == 1010
    assert data["phone"] == "5555555555"
    assert data["first_name"] == "A"


@pytest.mark.asyncio
async def test_update_me_updates_editable_fields(async_client, test_db_session):
    user = User(user_id=1010, age=30, phone="5555555555", status="active", first_name="A")
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(1010)
    payload = {"age": 30, "first_name": "New", "city": "Pune"}

    response = await async_client.put("/users/me", headers=headers, json=payload)
    assert response.status_code == 200

    data = response.json()["data"]
    assert data["first_name"] == "New"
    assert data["city"] == "Pune"


@pytest.mark.asyncio
async def test_update_me_updates_phone(async_client, test_db_session):
    user = User(user_id=1010, age=30, phone="5555555555", status="active", first_name="A")
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(1010)
    payload = {"age": 30, "phone": "5999999999"}

    response = await async_client.put("/users/me", headers=headers, json=payload)
    assert response.status_code == 200
    assert response.json()["data"]["phone"] == "5999999999"


@pytest.mark.asyncio
async def test_get_me_status_returns_active_flag(async_client, test_db_session):
    user = User(user_id=1010, age=30, phone="5555555555", status="active")
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(1010)
    response = await async_client.get("/users/me/status", headers=headers)

    assert response.status_code == 200
    assert response.json()["data"] == {"user_id": 1010, "status": "active", "is_active": True}


@pytest.mark.asyncio
async def test_inactive_user_is_forbidden(async_client, test_db_session):
    user = User(user_id=1010, age=30, phone="5555555555", status="inactive")
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(1010)
    response = await async_client.get("/users/me", headers=headers)

    assert response.status_code == 403
    assert response.json() == {
        "error_code": "FORBIDDEN",
        "message": "You do not have permission to perform this action",
    }


@pytest.mark.asyncio
async def test_update_sub_profile_persists_email(async_client, test_db_session):
    parent = User(
        user_id=9501,
        age=40,
        phone="6222222221",
        status="active",
        email="parent9501@example.com",
        relationship="self",
    )
    child = User(
        user_id=9502,
        age=20,
        phone="6222222222",
        status="active",
        email="child+old@example.com",
        parent_id=9501,
        relationship="child",
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()

    headers = _auth_header(9501)
    response = await async_client.put(
        "/users/me/profiles/9502",
        headers=headers,
        json={
            "age": 20,
            "email": "pratheek@gmail.com",
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["email"] == "pratheek@gmail.com"


@pytest.mark.asyncio
async def test_child_can_unlink_own_profile(async_client, test_db_session):
    parent = User(
        user_id=9301,
        age=40,
        phone="6111111111",
        status="active",
        email="parent9301@example.com",
        relationship="self",
    )
    child = User(
        user_id=9302,
        age=10,
        phone="6111111112",
        status="active",
        email="child9302@example.com",
        parent_id=9301,
        relationship="child",
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()

    headers = _auth_header(9302)
    response = await async_client.post(
        "/users/me/unlink",
        headers=headers,
        json={},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["user_id"] == 9302
    assert data["parent_id"] is None
    assert data["email"] == "child9302@example.com"


@pytest.mark.asyncio
async def test_child_unlink_rejects_child_user_id(async_client, test_db_session):
    parent = User(
        user_id=9401,
        age=40,
        phone="6111111111",
        status="active",
        email="parent9401@example.com",
        relationship="self",
    )
    child = User(
        user_id=9402,
        age=10,
        phone="6111111112",
        status="active",
        email="child9402@example.com",
        parent_id=9401,
        relationship="child",
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()

    headers = _auth_header(9402)
    response = await async_client.post(
        "/users/me/unlink",
        headers=headers,
        json={"child_user_id": 9402},
    )

    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_primary_unlink_requires_child_user_id(async_client, test_db_session):
    primary = User(
        user_id=9303,
        age=35,
        phone="6111111113",
        status="active",
        email="primary9303@example.com",
        relationship="self",
    )
    test_db_session.add(primary)
    await test_db_session.commit()

    headers = _auth_header(9303)
    response = await async_client.post(
        "/users/me/unlink",
        headers=headers,
        json={},
    )

    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_primary_can_unlink_sub_profile(async_client, test_db_session):
    parent = User(
        user_id=9310,
        age=40,
        phone="6111111111",
        status="active",
        email="parent9310@example.com",
        relationship="self",
    )
    child = User(
        user_id=9311,
        age=12,
        phone="6111111114",
        status="active",
        email="child9311@example.com",
        parent_id=9310,
        relationship="child",
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()

    headers = _auth_header(9310)
    response = await async_client.post(
        "/users/me/unlink",
        headers=headers,
        json={"child_user_id": 9311},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["user_id"] == 9311
    assert data["parent_id"] is None
    assert data["email"] == "child9311@example.com"


@pytest.mark.asyncio
async def test_primary_cannot_unlink_unrelated_sub_profile(async_client, test_db_session):
    actual_parent = User(
        user_id=9320,
        age=40,
        phone="6111111112",
        status="active",
        email="parent9320@example.com",
        relationship="self",
    )
    child = User(
        user_id=9321,
        age=11,
        phone="6111111113",
        status="active",
        email="child9321@example.com",
        parent_id=9320,
        relationship="child",
    )
    other_primary = User(
        user_id=9330,
        age=38,
        phone="6111111114",
        status="active",
        email="other9330@example.com",
        relationship="self",
    )
    test_db_session.add_all([actual_parent, child, other_primary])
    await test_db_session.commit()

    headers = _auth_header(9330)
    response = await async_client.post(
        "/users/me/unlink",
        headers=headers,
        json={"child_user_id": 9321},
    )

    assert response.status_code == 403
    assert response.json()["error_code"] == "FORBIDDEN"
