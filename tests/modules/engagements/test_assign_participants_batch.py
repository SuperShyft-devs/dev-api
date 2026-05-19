"""Tests for POST /engagements/{id}/assign-participants-batch."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}0000000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_assessment_package(test_db_session, *, package_id: int = 1):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (:pid, 'PKG1', 'Test Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        ),
        {"pid": package_id},
    )
    await test_db_session.commit()


async def _seed_engagement(
    test_db_session,
    *,
    engagement_id: int = 9001,
    assessment_package_id: int | None = 1,
    code: str = "ENG9001",
):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    pkg_sql = "NULL" if assessment_package_id is None else str(int(assessment_package_id))
    await test_db_session.execute(
        text(
            f"INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            f"assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, "
            f"status, participant_count, organization_id) "
            f"VALUES ({engagement_id}, 'Camp', '{code}', 'bio_ai', {pkg_sql}, 1, 'BLR', 20, "
            f"'2026-02-01', '2026-02-28', 'active', 0, NULL)"
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_assign_participants_batch_requires_auth(async_client):
    response = await async_client.post(
        "/engagements/1/assign-participants-batch",
        json={"rows": [{"metsights_record_id": "REC1", "phone": "+919876543210"}]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_assign_participants_batch_rejects_more_than_50_rows(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8001)
    rows = [{"metsights_record_id": f"R{i}", "phone": "+919876543210"} for i in range(51)]
    response = await async_client.post(
        "/engagements/1/assign-participants-batch",
        headers=_auth_header(8001),
        json={"rows": rows},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_assign_participants_batch_skips_already_assigned(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8002)
    await _seed_assessment_package(test_db_session)
    await _seed_engagement(test_db_session)

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, age, phone, status) VALUES (5001, 30, '+919876543210', 'active')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_instances (user_id, engagement_id, package_id, status, metsights_record_id, assigned_at) "
            "VALUES (5001, 9001, 1, 'active', 'EXISTING1', NOW())"
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/9001/assign-participants-batch",
        headers=_auth_header(8002),
        json={"rows": [{"metsights_record_id": "EXISTING1", "phone": "+919876543210"}]},
    )
    assert response.status_code == 200
    row = response.json()["data"]["results"][0]
    assert row["status"] == "skipped"
    assert row["reason"] == "already_assigned"


@pytest.mark.asyncio
async def test_assign_participants_batch_skips_user_not_found(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8003)
    await _seed_assessment_package(test_db_session)
    await _seed_engagement(test_db_session)

    response = await async_client.post(
        "/engagements/9001/assign-participants-batch",
        headers=_auth_header(8003),
        json={"rows": [{"metsights_record_id": "NEWREC1", "phone": "+919999999999"}]},
    )
    assert response.status_code == 200
    row = response.json()["data"]["results"][0]
    assert row["status"] == "skipped"
    assert row["reason"] == "user_not_found"


@pytest.mark.asyncio
async def test_assign_participants_batch_happy_path_enrolls_and_assigns(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8004)
    await _seed_assessment_package(test_db_session)
    await _seed_engagement(test_db_session)

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, age, phone, status) VALUES (5002, 30, '+919876543211', 'active')"
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/9001/assign-participants-batch",
        headers=_auth_header(8004),
        json={"rows": [{"metsights_record_id": "NEWREC2", "phone": "9876543211"}]},
    )
    assert response.status_code == 200
    row = response.json()["data"]["results"][0]
    assert row["status"] == "assigned"
    assert row["user_id"] == 5002
    assert row["newly_enrolled"] is True
    assert row["assessment_instance_id"] is not None

    participant = (
        await test_db_session.execute(
            text(
                "SELECT engagement_participant_id FROM engagement_participants "
                "WHERE engagement_id = 9001 AND user_id = 5002"
            )
        )
    ).first()
    assert participant is not None

    inst = (
        await test_db_session.execute(
            text(
                "SELECT metsights_record_id FROM assessment_instances "
                "WHERE assessment_instance_id = :aid"
            ),
            {"aid": row["assessment_instance_id"]},
        )
    ).first()
    assert inst.metsights_record_id == "NEWREC2"


@pytest.mark.asyncio
async def test_assign_participants_batch_already_enrolled_still_assigns(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8005)
    await _seed_assessment_package(test_db_session)
    await _seed_engagement(test_db_session)

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, age, phone, status) VALUES (5003, 30, '+919876543212', 'active')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_participants (engagement_id, user_id, engagement_date, slot_start_time) "
            "VALUES (9001, 5003, '2026-02-01', '10:00:00')"
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/9001/assign-participants-batch",
        headers=_auth_header(8005),
        json={"rows": [{"metsights_record_id": "NEWREC3", "phone": "+919876543212"}]},
    )
    assert response.status_code == 200
    row = response.json()["data"]["results"][0]
    assert row["status"] == "assigned"
    assert row["newly_enrolled"] is False
    assert row["reason"] == "already_enrolled"


@pytest.mark.asyncio
async def test_assign_participants_batch_requires_assessment_package(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8006)
    await _seed_assessment_package(test_db_session)
    await _seed_engagement(test_db_session, assessment_package_id=None)

    response = await async_client.post(
        "/engagements/9001/assign-participants-batch",
        headers=_auth_header(8006),
        json={"rows": [{"metsights_record_id": "RECX", "phone": "+919876543210"}]},
    )
    assert response.status_code == 422
