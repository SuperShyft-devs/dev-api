"""Tests for POST /notifications/bulk-delete."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
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
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail) "
            "VALUES (:sk, :dn, :ch, 'test-webhook', true, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET display_name = EXCLUDED.display_name"
        ),
        {"sk": service_key, "dn": display_name, "ch": channel},
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_bulk_delete_requires_employee(async_client):
    response = await async_client.post(
        "/notifications/bulk-delete",
        json={"notification_ids": [1]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_bulk_delete_rejects_empty_list(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9701, employee_id=9701)
    response = await async_client.post(
        "/notifications/bulk-delete",
        headers=_auth_header(9701),
        json={"notification_ids": []},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_bulk_delete_rejects_invalid_ids(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9702, employee_id=9702)
    response = await async_client.post(
        "/notifications/bulk-delete",
        headers=_auth_header(9702),
        json={"notification_ids": [0, -1]},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_bulk_delete_multiple_notifications(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9703, employee_id=9703)
    await _seed_service(test_db_session, service_key="bulk-del-svc", display_name="Bulk Del")

    now = datetime.now(timezone.utc)
    rows = [
        Notification(
            service_key="bulk-del-svc",
            status="sent",
            channel="whatsapp",
            user={"user_ids": [9703]},
            message=f"keep-{i}" if i == 2 else f"drop-{i}",
            dispatched_at=now,
        )
        for i in range(3)
    ]
    test_db_session.add_all(rows)
    await test_db_session.flush()
    drop_ids = [rows[0].notification_id, rows[1].notification_id]
    keep_id = rows[2].notification_id
    await test_db_session.commit()

    response = await async_client.post(
        "/notifications/bulk-delete",
        headers=_auth_header(9703),
        json={"notification_ids": drop_ids},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["deleted_count"] == 2
    assert sorted(body["deleted_ids"]) == sorted(drop_ids)
    assert body["not_found_ids"] == []

    remaining = (
        await test_db_session.execute(
            select(Notification.notification_id).where(
                Notification.service_key == "bulk-del-svc"
            )
        )
    ).scalars().all()
    assert keep_id in remaining
    assert drop_ids[0] not in remaining
    assert drop_ids[1] not in remaining


@pytest.mark.asyncio
async def test_bulk_delete_reports_missing_ids(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9704, employee_id=9704)
    await _seed_service(test_db_session, service_key="bulk-del-missing", display_name="Missing")

    row = Notification(
        service_key="bulk-del-missing",
        status="sent",
        channel="email",
        user={"user_ids": [9704]},
        message="exists",
        dispatched_at=datetime.now(timezone.utc),
    )
    test_db_session.add(row)
    await test_db_session.flush()
    existing_id = row.notification_id
    await test_db_session.commit()

    missing_id = existing_id + 999_999
    response = await async_client.post(
        "/notifications/bulk-delete",
        headers=_auth_header(9704),
        json={"notification_ids": [existing_id, missing_id]},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["deleted_count"] == 1
    assert body["deleted_ids"] == [existing_id]
    assert body["not_found_ids"] == [missing_id]


@pytest.mark.asyncio
async def test_bulk_delete_deduplicates_ids(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9705, employee_id=9705)
    await _seed_service(test_db_session, service_key="bulk-del-dup", display_name="Dup")

    row = Notification(
        service_key="bulk-del-dup",
        status="failed",
        channel="email",
        user={"user_ids": [9705]},
        message="dup",
        dispatched_at=datetime.now(timezone.utc),
    )
    test_db_session.add(row)
    await test_db_session.flush()
    nid = row.notification_id
    await test_db_session.commit()

    response = await async_client.post(
        "/notifications/bulk-delete",
        headers=_auth_header(9705),
        json={"notification_ids": [nid, nid]},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["deleted_count"] == 1
    assert body["deleted_ids"] == [nid]
    assert body["not_found_ids"] == []
