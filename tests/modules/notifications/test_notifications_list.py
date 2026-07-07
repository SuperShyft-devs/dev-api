"""Tests for GET /notifications admin list (filters and enrichment)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.engagements.models import Engagement
from modules.notifications.models import Notification
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token(
        {"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY
    )
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    test_db_session.add(
        User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active", first_name="Admin")
    )
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active")
    )
    await test_db_session.commit()


async def _seed_service(test_db_session, *, service_key: str, display_name: str, channel: str = "whatsapp"):
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_record_id, require_participant_detail) "
            "VALUES (:sk, :dn, :ch, 'test-webhook', true, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET display_name = EXCLUDED.display_name"
        ),
        {"sk": service_key, "dn": display_name, "ch": channel},
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_list_notifications_requires_employee(async_client, test_db_session):
    response = await async_client.get("/notifications")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_notifications_enriched_fields(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9601, employee_id=9601)
    await _seed_service(
        test_db_session,
        service_key="welcome-whatsapp-test",
        display_name="Welcome WhatsApp",
    )

    test_db_session.add(
        User(
            user_id=9602,
            age=30,
            phone="9602000000",
            status="active",
            first_name="Alex",
            last_name="Test",
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=9603,
            engagement_name="Summer Camp",
            engagement_code="ENG-LIST-9603",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 5, 1),
            end_date=date(2026, 5, 1),
            status="active",
        )
    )
    dispatched = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
    test_db_session.add(
        Notification(
            service_key="welcome-whatsapp-test",
            status="sent",
            channel="whatsapp",
            user={"user_ids": [9602]},
            engagement_id=9603,
            message="ok",
            dispatched_at=dispatched,
            completed_at=dispatched,
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/notifications?limit=5", headers=_auth_header(9601))
    assert response.status_code == 200
    body = response.json()
    row = next(
        (r for r in body["data"] if r.get("engagement_id") == 9603),
        None,
    )
    assert row is not None
    assert row["service_display_name"] == "Welcome WhatsApp"
    assert row["engagement_name"] == "Summer Camp"
    assert row["engagement_code"] == "ENG-LIST-9603"
    assert len(row["recipients"]) == 1
    assert row["recipients"][0]["user_id"] == 9602
    assert row["recipients"][0]["first_name"] == "Alex"


@pytest.mark.asyncio
async def test_list_notifications_multi_status_filter(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9604, employee_id=9604)
    await _seed_service(test_db_session, service_key="multi-status-svc", display_name="Multi")

    for st in ("pending", "sent", "failed"):
        test_db_session.add(
            Notification(
                service_key="multi-status-svc",
                status=st,
                channel="email",
                user={"user_ids": [9604]},
                message=st,
                dispatched_at=datetime.now(timezone.utc),
            )
        )
    await test_db_session.commit()

    response = await async_client.get(
        "/notifications?status=pending,failed&service_key=multi-status-svc&limit=50",
        headers=_auth_header(9604),
    )
    assert response.status_code == 200
    statuses = {r["status"] for r in response.json()["data"]}
    assert statuses <= {"pending", "failed"}
    assert "sent" not in statuses
    assert len(statuses) >= 1


@pytest.mark.asyncio
async def test_list_notifications_dispatched_at_range(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9605, employee_id=9605)
    await _seed_service(test_db_session, service_key="date-filter-svc", display_name="Date")

    old = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    recent = datetime(2026, 5, 20, 10, 0, 0, tzinfo=timezone.utc)
    test_db_session.add(
        Notification(
            service_key="date-filter-svc",
            status="sent",
            channel="whatsapp",
            user={"user_ids": [9605]},
            message="old",
            dispatched_at=old,
        )
    )
    test_db_session.add(
        Notification(
            service_key="date-filter-svc",
            status="sent",
            channel="whatsapp",
            user={"user_ids": [9605]},
            message="recent",
            dispatched_at=recent,
        )
    )
    await test_db_session.commit()

    from_iso = "2026-05-01T00:00:00Z"
    to_iso = "2026-05-31T23:59:59Z"
    response = await async_client.get(
        f"/notifications?service_key=date-filter-svc&dispatched_from={from_iso}&dispatched_to={to_iso}&limit=50",
        headers=_auth_header(9605),
    )
    assert response.status_code == 200
    messages = {r["message"] for r in response.json()["data"]}
    assert "recent" in messages
    assert "old" not in messages


@pytest.mark.asyncio
async def test_list_notifications_channel_filter(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9606, employee_id=9606)
    await _seed_service(
        test_db_session, service_key="email-svc-list", display_name="Email", channel="email"
    )
    await _seed_service(
        test_db_session, service_key="wa-svc-list", display_name="WA", channel="whatsapp"
    )

    test_db_session.add(
        Notification(
            service_key="email-svc-list",
            status="sent",
            channel="email",
            user={"user_ids": [9606]},
            dispatched_at=datetime.now(timezone.utc),
        )
    )
    test_db_session.add(
        Notification(
            service_key="wa-svc-list",
            status="sent",
            channel="whatsapp",
            user={"user_ids": [9606]},
            dispatched_at=datetime.now(timezone.utc),
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/notifications?channel=email&limit=50",
        headers=_auth_header(9606),
    )
    assert response.status_code == 200
    for row in response.json()["data"]:
        if row["service_key"] in ("email-svc-list", "wa-svc-list"):
            assert row["channel"] == "email"
