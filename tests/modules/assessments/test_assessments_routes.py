"""Integration tests for assessments routes (user-facing)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.engagements.models import Engagement
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_list_my_assessments_requires_auth(async_client):
    response = await async_client.get("/assessments/me")
    assert response.status_code == 401
    assert response.json() == {"error_code": "AUTH_FAILED", "message": "Authentication failed"}


@pytest.mark.asyncio
async def test_list_my_assessments_returns_only_my_rows(async_client, test_db_session):
    test_db_session.add(User(user_id=3001, phone="3001000000", status="active"))
    test_db_session.add(User(user_id=3002, phone="3002000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=1, package_code="BASIC", display_name="Basic", status="active"))
    await test_db_session.flush()  # Ensure users and packages exist before adding engagements
    test_db_session.add(Engagement(engagement_id=501, engagement_code="ENG501", assessment_package_id=1))
    test_db_session.add(Engagement(engagement_id=502, engagement_code="ENG502", assessment_package_id=1))
    await test_db_session.flush()  # Ensure engagements exist before adding instances

    now = datetime.now(timezone.utc)
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=91001,
            user_id=3001,
            package_id=1,
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
            package_id=1,
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
    test_db_session.add(User(user_id=3011, phone="3011000000", status="active"))
    test_db_session.add(User(user_id=3012, phone="3012000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=2, package_code="P2", display_name="P2", status="active"))
    await test_db_session.flush()
    test_db_session.add(Engagement(engagement_id=601, engagement_code="ENG601", assessment_package_id=2))
    await test_db_session.flush()

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=92001,
            user_id=3012,
            package_id=2,
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
    test_db_session.add(User(user_id=3021, phone="3021000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=3, package_code="P3", display_name="P3", status="active"))
    await test_db_session.flush()
    test_db_session.add(Engagement(engagement_id=701, engagement_code="ENG701", assessment_package_id=3))
    await test_db_session.flush()

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=93001,
            user_id=3021,
            package_id=3,
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
    test_db_session.add(User(user_id=3031, phone="3031000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=4, package_code="P4", display_name="P4", status="active"))
    await test_db_session.flush()
    test_db_session.add(Engagement(engagement_id=801, engagement_code="ENG801", assessment_package_id=4))
    await test_db_session.flush()

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=94001,
            user_id=3031,
            package_id=4,
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
    test_db_session.add(User(user_id=3041, phone="3041000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=5, package_code="P5", display_name="P5", status="active"))
    await test_db_session.flush()
    test_db_session.add(Engagement(engagement_id=901, engagement_code="ENG901", assessment_package_id=5))
    await test_db_session.flush()

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=95001,
            user_id=3041,
            package_id=5,
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
