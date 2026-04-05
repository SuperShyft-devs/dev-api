"""Integration tests for assessments routes (user-facing)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.employee.models import Employee
from modules.engagements.models import Engagement
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    test_db_session.add(User(user_id=user_id, phone=f"{user_id}000000", age=30, status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_list_my_assessments_requires_auth(async_client):
    response = await async_client.get("/assessments/me")
    assert response.status_code == 401
    assert response.json() == {"error_code": "AUTH_FAILED", "message": "Authentication failed"}


@pytest.mark.asyncio
async def test_list_my_assessments_returns_only_my_rows(async_client, test_db_session):
    test_db_session.add(User(user_id=3001, age=30, phone="3001000000", status="active"))
    test_db_session.add(User(user_id=3002, age=30, phone="3002000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=99401, package_code="BASIC", display_name="Basic", status="active"))
    await test_db_session.flush()  # Ensure users and packages exist before adding engagements
    test_db_session.add(
        Engagement(
            engagement_id=501,
            engagement_name="E501",
            engagement_code="ENG501",
            engagement_type="healthcamp",
            assessment_package_id=99401,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=60,
            status="active",
            participant_count=0,
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=502,
            engagement_name="E502",
            engagement_code="ENG502",
            engagement_type="healthcamp",
            assessment_package_id=99401,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=60,
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.flush()  # Ensure engagements exist before adding instances

    now = datetime.now(timezone.utc)
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=91001,
            user_id=3001,
            package_id=99401,
            engagement_id=501,
            status="active",
            assigned_at=now,
            completed_at=None,
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=91002,
            user_id=3002,
            package_id=99401,
            engagement_id=502,
            status="active",
            assigned_at=now,
            completed_at=None,
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/assessments/me?page=1&limit=20", headers=_auth_header(3001))
    assert response.status_code == 200

    body = response.json()
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 20
    assert body["meta"]["total"] == 1

    assert len(body["data"]) == 1
    assert body["data"][0]["assessment_instance_id"] == 91001
    assert body["data"][0]["package_code"] == "BASIC"


@pytest.mark.asyncio
async def test_get_assessment_details_blocks_cross_user_access(async_client, test_db_session):
    test_db_session.add(User(user_id=3011, age=30, phone="3011000000", status="active"))
    test_db_session.add(User(user_id=3012, age=30, phone="3012000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=99402, package_code="P2", display_name="P2", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=601,
            engagement_name="E601",
            engagement_code="ENG601",
            engagement_type="healthcamp",
            assessment_package_id=99402,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=60,
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=92001,
            user_id=3012,
            package_id=99402,
            engagement_id=601,
            status="active",
            assigned_at=datetime.now(timezone.utc),
            completed_at=None,
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/assessments/92001", headers=_auth_header(3011))
    assert response.status_code == 404
    assert response.json() == {"error_code": "ASSESSMENT_NOT_FOUND", "message": "Assessment does not exist"}


@pytest.mark.asyncio
async def test_patch_assessment_status_allows_active_to_completed(async_client, test_db_session):
    test_db_session.add(User(user_id=3021, age=30, phone="3021000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=99403, package_code="P3", display_name="P3", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=701,
            engagement_name="E701",
            engagement_code="ENG701",
            engagement_type="healthcamp",
            assessment_package_id=99403,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=60,
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=93001,
            user_id=3021,
            package_id=99403,
            engagement_id=701,
            status="active",
            assigned_at=datetime.now(timezone.utc),
            completed_at=None,
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/assessments/93001/status",
        headers=_auth_header(3021),
        json={"status": "completed"},
    )

    assert response.status_code == 200
    body = response.json()["data"]
    assert body["assessment_instance_id"] == 93001
    assert (body["status"] or "").lower() == "completed"
    assert body["completed_at"] is not None

    updated = await test_db_session.get(AssessmentInstance, 93001)
    assert updated is not None
    assert (updated.status or "").lower() == "completed"
    assert updated.completed_at is not None


@pytest.mark.asyncio
async def test_patch_assessment_status_rejects_invalid_status(async_client, test_db_session):
    test_db_session.add(User(user_id=3031, age=30, phone="3031000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=99404, package_code="P4", display_name="P4", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=801,
            engagement_name="E801",
            engagement_code="ENG801",
            engagement_type="healthcamp",
            assessment_package_id=99404,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=60,
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=94001,
            user_id=3031,
            package_id=99404,
            engagement_id=801,
            status="active",
            assigned_at=datetime.now(timezone.utc),
            completed_at=None,
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/assessments/94001/status",
        headers=_auth_header(3031),
        json={"status": "archived"},
    )
    assert response.status_code == 400
    assert response.json() == {"error_code": "INVALID_INPUT", "message": "Invalid request"}


@pytest.mark.asyncio
async def test_patch_assessment_status_rejects_changes_after_completion(async_client, test_db_session):
    test_db_session.add(User(user_id=3041, age=30, phone="3041000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=99405, package_code="P5", display_name="P5", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=901,
            engagement_name="E901",
            engagement_code="ENG901",
            engagement_type="healthcamp",
            assessment_package_id=99405,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=60,
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=95001,
            user_id=3041,
            package_id=99405,
            engagement_id=901,
            status="completed",
            assigned_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/assessments/95001/status",
        headers=_auth_header(3041),
        json={"status": "active"},
    )

    assert response.status_code == 422
    assert response.json() == {"error_code": "INVALID_STATE", "message": "Assessment is already completed"}


@pytest.mark.asyncio
async def test_put_metsights_record_id_returns_404_for_missing_instance(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=3301, employee_id=3301)

    response = await async_client.put(
        "/assessments/999/metsights-record-id",
        headers=_auth_header(3301),
        json={"metsights_record_id": "0FF4776794C5"},
    )

    assert response.status_code == 404
    assert response.json() == {"error_code": "ASSESSMENT_NOT_FOUND", "message": "Assessment does not exist"}


@pytest.mark.asyncio
async def test_put_metsights_record_id_rejects_empty_string(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=3302, employee_id=3302)
    test_db_session.add(User(user_id=3303, phone="3303000000", age=30, status="active"))
    test_db_session.add(AssessmentPackage(package_id=99433, package_code="P33", display_name="P33", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=3303,
            engagement_name="E3303",
            engagement_code="ENG3303",
            engagement_type="healthcamp",
            assessment_package_id=99433,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=60,
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=3303001,
            user_id=3303,
            package_id=99433,
            engagement_id=3303,
            status="active",
            assigned_at=datetime.now(timezone.utc),
            completed_at=None,
        )
    )
    await test_db_session.commit()

    response = await async_client.put(
        "/assessments/3303001/metsights-record-id",
        headers=_auth_header(3302),
        json={"metsights_record_id": "   "},
    )

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_put_metsights_record_id_updates_and_get_returns_field(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=3304, employee_id=3304)
    test_db_session.add(User(user_id=3305, phone="3305000000", age=30, status="active"))
    test_db_session.add(AssessmentPackage(package_id=99434, package_code="P34", display_name="P34", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=3305,
            engagement_name="E3305",
            engagement_code="ENG3305",
            engagement_type="healthcamp",
            assessment_package_id=99434,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=60,
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=3305001,
            user_id=3305,
            package_id=99434,
            engagement_id=3305,
            status="active",
            assigned_at=datetime.now(timezone.utc),
            completed_at=None,
        )
    )
    await test_db_session.commit()

    update_response = await async_client.put(
        "/assessments/3305001/metsights-record-id",
        headers=_auth_header(3304),
        json={"metsights_record_id": "0FF4776794C5"},
    )

    assert update_response.status_code == 200
    assert update_response.json()["data"]["metsights_record_id"] == "0FF4776794C5"

    get_response = await async_client.get("/assessments/3305001", headers=_auth_header(3305))
    assert get_response.status_code == 200
    assert get_response.json()["data"]["metsights_record_id"] == "0FF4776794C5"
