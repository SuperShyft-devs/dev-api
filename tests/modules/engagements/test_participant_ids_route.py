"""Tests for participant IDs endpoint (select-all across pages)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.engagements.models import Engagement, EngagementParticipant
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token(
        {"sub": str(user_id)},
        timedelta(minutes=5),
        secret_key=settings.JWT_SECRET_KEY,
    )
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id:010d}", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _engagement_type_id(test_db_session, code: str) -> int:
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES (:code, :dn, true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        ),
        {"code": code, "dn": code},
    )
    await test_db_session.commit()
    row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = :code"),
            {"code": code},
        )
    ).one()
    return int(row[0])


async def _seed_engagement(test_db_session, *, engagement_id: int, type_code: str):
    type_id = await _engagement_type_id(test_db_session, type_code)
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (:pid, :pcode, :dname, 'active') ON CONFLICT (package_id) DO NOTHING"
        ),
        {"pid": engagement_id, "pcode": f"PKG{engagement_id}", "dname": f"Package {engagement_id}"},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, bookings_count) "
            "VALUES (:did, :ref, :pname, 'test_provider', 'active', 0) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        ),
        {"did": engagement_id, "ref": f"REF{engagement_id}", "pname": f"Diag {engagement_id}"},
    )
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_code=f"ENG{engagement_id}",
            engagement_type=type_id,
            assessment_package_id=engagement_id,
            diagnostic_package_id=engagement_id,
            slot_duration=20,
            start_date=date(2026, 9, 1),
            end_date=date(2026, 9, 30),
            status="running",
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_participant_ids_route_requires_auth(async_client):
    response = await async_client.get("/engagements/1/participants/ids")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_participant_ids_route_returns_all_matching_ids(async_client, test_db_session):
    engagement_id = 88401
    admin_id = 88402
    await _seed_employee(test_db_session, user_id=admin_id, employee_id=admin_id)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, type_code="participant_ids")

    participant_user_ids = [884001, 884002, 884003]
    departments = ["eng", "eng", "sales"]
    for user_id, department in zip(participant_user_ids, departments, strict=True):
        test_db_session.add(
            User(
                user_id=user_id,
                first_name=f"User{user_id}",
                age=30,
                phone=f"{user_id:010d}",
                status="active",
            )
        )
        await test_db_session.flush()
        test_db_session.add(
            EngagementParticipant(
                engagement_id=engagement_id,
                user_id=user_id,
                participant_department=department,
            )
        )
    await test_db_session.commit()

    response = await async_client.get(
        f"/engagements/{engagement_id}/participants/ids",
        headers=_auth_header(admin_id),
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["total"] == 3
    assert sorted(payload["user_ids"]) == sorted(participant_user_ids)

    filtered = await async_client.get(
        f"/engagements/{engagement_id}/participants/ids?department=eng",
        headers=_auth_header(admin_id),
    )
    assert filtered.status_code == 200
    filtered_payload = filtered.json()["data"]
    assert filtered_payload["total"] == 2
    assert sorted(filtered_payload["user_ids"]) == [884001, 884002]
