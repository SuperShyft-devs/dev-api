"""Tests for engagement booking-dates endpoint (integration sync logs)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.audit.models import IntegrationSyncLog
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.employee.models import Employee
from modules.engagements.models import Engagement
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
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_code=f"ENG{engagement_id}",
            engagement_type=type_id,
            assessment_package_id=1,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 9, 1),
            end_date=date(2026, 9, 30),
            status="running",
        )
    )
    await test_db_session.commit()


async def _seed_users(test_db_session, user_ids: list[int]):
    for user_id in user_ids:
        test_db_session.add(
            User(
                user_id=user_id,
                age=30,
                phone=f"{user_id:010d}",
                status="active",
            )
        )
    await test_db_session.flush()


def _booking_url() -> str:
    return "https://hbridge.healthians.com/api/toast4health/createBooking_v3"


@pytest.mark.asyncio
async def test_audit_service_groups_booking_dates_by_ist():
    repo = AsyncMock()
    repo.list_create_booking_dates_for_engagement.return_value = [
        (date(2026, 9, 1), 101),
        (date(2026, 9, 2), 102),
        (date(2026, 9, 2), 103),
    ]
    service = AuditService(repo)

    result = await service.list_create_booking_dates_for_engagement(
        db=AsyncMock(),
        engagement_id=1,
    )

    assert result["dates"] == ["2026-09-02", "2026-09-01"]
    assert result["user_ids_by_date"]["2026-09-01"] == [101]
    assert result["user_ids_by_date"]["2026-09-02"] == [102, 103]


@pytest.mark.asyncio
async def test_list_create_booking_dates_buckets_by_ist_and_filters_success_only(test_db_session):
    engagement_id = 78101
    other_engagement_id = 78199
    await _seed_engagement(test_db_session, engagement_id=engagement_id, type_code="booking_dates_repo")
    await _seed_engagement(
        test_db_session,
        engagement_id=other_engagement_id,
        type_code="booking_dates_repo_other",
    )
    await _seed_users(test_db_session, [78111, 78112, 78113, 78114, 78115])

    rows = [
        IntegrationSyncLog(
            engagement_id=engagement_id,
            user_id=78111,
            provider="healthians",
            api_endpoint_url=_booking_url(),
            status="success",
            created_at=datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc),
        ),
        IntegrationSyncLog(
            engagement_id=engagement_id,
            user_id=78112,
            provider="healthians",
            api_endpoint_url=_booking_url(),
            status="success",
            created_at=datetime(2026, 9, 1, 18, 30, tzinfo=timezone.utc),
        ),
        IntegrationSyncLog(
            engagement_id=engagement_id,
            user_id=78113,
            provider="healthians",
            api_endpoint_url=_booking_url(),
            status="failed",
            created_at=datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc),
        ),
        IntegrationSyncLog(
            engagement_id=engagement_id,
            user_id=78114,
            provider="healthians",
            api_endpoint_url="https://example.com/other-api",
            status="success",
            created_at=datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc),
        ),
        IntegrationSyncLog(
            engagement_id=other_engagement_id,
            user_id=78115,
            provider="healthians",
            api_endpoint_url=_booking_url(),
            status="success",
            created_at=datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc),
        ),
    ]
    test_db_session.add_all(rows)
    await test_db_session.commit()

    service = AuditService(AuditRepository())
    result = await service.list_create_booking_dates_for_engagement(
        test_db_session,
        engagement_id=engagement_id,
    )

    assert result["dates"] == ["2026-09-02", "2026-09-01"]
    assert result["user_ids_by_date"]["2026-09-01"] == [78111]
    assert result["user_ids_by_date"]["2026-09-02"] == [78112]


@pytest.mark.asyncio
async def test_get_engagement_booking_dates_route(async_client, test_db_session):
    engagement_id = 78102
    await _seed_employee(test_db_session, user_id=7810, employee_id=7810)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, type_code="booking_dates_route")
    await _seed_users(test_db_session, [78201, 78202])

    test_db_session.add(
        IntegrationSyncLog(
            engagement_id=engagement_id,
            user_id=78201,
            provider="healthians",
            api_endpoint_url=_booking_url(),
            status="success",
            created_at=datetime(2026, 9, 2, 4, 0, tzinfo=timezone.utc),
        )
    )
    test_db_session.add(
        IntegrationSyncLog(
            engagement_id=engagement_id,
            user_id=78202,
            provider="healthians",
            api_endpoint_url=_booking_url(),
            status="success",
            created_at=datetime(2026, 9, 2, 6, 0, tzinfo=timezone.utc),
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        f"/engagements/{engagement_id}/booking-dates",
        headers=_auth_header(7810),
    )
    assert response.status_code == 200, response.text
    body = response.json()["data"]
    assert body["dates"] == ["2026-09-02"]
    assert sorted(body["user_ids_by_date"]["2026-09-02"]) == [78201, 78202]


@pytest.mark.asyncio
async def test_get_engagement_booking_dates_not_found(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7811, employee_id=7811)

    response = await async_client.get(
        "/engagements/999999/booking-dates",
        headers=_auth_header(7811),
    )
    assert response.status_code == 404
