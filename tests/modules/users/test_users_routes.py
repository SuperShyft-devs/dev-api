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
