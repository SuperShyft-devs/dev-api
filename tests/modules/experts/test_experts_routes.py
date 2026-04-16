"""Integration tests for experts routes (public list/detail; employee mutations)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.experts.models import Expert
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    user = User(user_id=user_id, age=30, phone=f"{user_id}0000000000"[:15], status="active")
    test_db_session.add(user)
    await test_db_session.flush()
    employee = Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active")
    test_db_session.add(employee)
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_list_experts_public_returns_specialization_key(async_client, test_db_session):
    """Public GET uses API field name specialization (not display_name)."""
    expert_user = User(
        user_id=78501,
        age=35,
        phone="785010000000",
        first_name="Test",
        last_name="Expert",
        status="active",
    )
    test_db_session.add(expert_user)
    await test_db_session.flush()
    test_db_session.add(
        Expert(
            user_id=78501,
            expert_type="doctor",
            specialization="Cardiology",
            status="active",
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/experts?page=1&limit=20")
    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    rows = [r for r in body["data"] if r.get("expert_id")]
    assert any(r.get("specialization") == "Cardiology" for r in rows)
    for r in rows:
        assert "specialization" in r
        assert "display_name" not in r


@pytest.mark.asyncio
async def test_get_expert_detail_public_includes_specialization(async_client, test_db_session):
    expert_user = User(user_id=78502, age=40, phone="785020000000", status="active")
    test_db_session.add(expert_user)
    await test_db_session.flush()
    expert = Expert(
        user_id=78502,
        expert_type="nutritionist",
        specialization="Sports nutrition",
        status="active",
    )
    test_db_session.add(expert)
    await test_db_session.flush()
    await test_db_session.commit()
    expert_id = expert.expert_id

    response = await async_client.get(f"/experts/{expert_id}")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["specialization"] == "Sports nutrition"
    assert "display_name" not in data


@pytest.mark.asyncio
async def test_create_expert_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=78503, age=30, phone="785030000000", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/experts",
        headers=_auth_header(78503),
        json={
            "user_id": 78503,
            "expert_type": "doctor",
            "specialization": "General medicine",
        },
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_expert_persists_specialization(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=78504, employee_id=401)
    test_db_session.add(User(user_id=78505, age=28, phone="785050000000", status="active"))
    await test_db_session.commit()

    payload = {
        "user_id": 78505,
        "expert_type": "doctor",
        "specialization": "Pediatrics",
    }
    response = await async_client.post("/experts", headers=_auth_header(78504), json=payload)
    assert response.status_code == 201
    expert_id = response.json()["data"]["expert_id"]

    row = await test_db_session.get(Expert, expert_id)
    assert row is not None
    assert row.specialization == "Pediatrics"
    assert row.expert_type == "doctor"
    assert row.user_id == 78505


@pytest.mark.asyncio
async def test_update_expert_specialization(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=78506, employee_id=402)
    test_db_session.add(User(user_id=78507, age=32, phone="785070000000", status="active"))
    await test_db_session.flush()
    expert = Expert(
        user_id=78507,
        expert_type="doctor",
        specialization="Old spec",
        status="active",
    )
    test_db_session.add(expert)
    await test_db_session.flush()
    await test_db_session.commit()
    expert_id = expert.expert_id

    response = await async_client.put(
        f"/experts/{expert_id}",
        headers=_auth_header(78506),
        json={
            "user_id": 78507,
            "expert_type": "nutritionist",
            "specialization": "Updated specialization",
        },
    )
    assert response.status_code == 200
    await test_db_session.refresh(expert)
    assert expert.specialization == "Updated specialization"
    assert expert.expert_type == "nutritionist"
