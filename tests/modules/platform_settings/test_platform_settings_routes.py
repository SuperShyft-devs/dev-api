"""Tests for platform settings (employee-only)."""

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


@pytest.mark.asyncio
async def test_get_b2c_defaults_requires_auth(async_client):
    response = await async_client.get("/platform-settings/b2c-onboarding")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_patch_b2c_defaults_requires_auth(async_client):
    response = await async_client.patch(
        "/platform-settings/b2c-onboarding",
        json={"b2c_default_assessment_package_id": 1, "b2c_default_diagnostic_package_id": 1},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_b2c_defaults_fallback_when_no_row(async_client, test_db_session):
    uid = 9101
    test_db_session.add(User(user_id=uid, age=30, phone="91010000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9101, user_id=uid, role="admin", status="active"))
    await test_db_session.execute(text("DELETE FROM platform_settings"))
    await test_db_session.commit()
    test_db_session.expire_all()

    response = await async_client.get("/platform-settings/b2c-onboarding", headers=_auth_header(uid))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["b2c_default_assessment_package_id"] == 1
    assert data["b2c_default_diagnostic_package_id"] == 1


@pytest.mark.asyncio
async def test_patch_b2c_defaults_persists_and_get_returns(async_client, test_db_session):
    uid = 9102
    test_db_session.add(User(user_id=uid, age=30, phone="91020000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9102, user_id=uid, role="admin", status="active"))

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES "
            "(1, 'P1', 'One', 'active'), (2, 'P2', 'Two', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET "
            "package_code = EXCLUDED.package_code, display_name = EXCLUDED.display_name, status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'R1', 'D1', 'p', 'active'), (2, 'R2', 'D2', 'p', 'active') "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status"
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/platform-settings/b2c-onboarding",
        headers=_auth_header(uid),
        json={"b2c_default_assessment_package_id": 2, "b2c_default_diagnostic_package_id": 2},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["b2c_default_assessment_package_id"] == 2
    assert data["b2c_default_diagnostic_package_id"] == 2

    response2 = await async_client.get("/platform-settings/b2c-onboarding", headers=_auth_header(uid))
    assert response2.status_code == 200
    assert response2.json()["data"] == data


@pytest.mark.asyncio
async def test_patch_b2c_defaults_rejects_inactive_package(async_client, test_db_session):
    uid = 9103
    test_db_session.add(User(user_id=uid, age=30, phone="91030000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9103, user_id=uid, role="admin", status="active"))

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES "
            "(1, 'P1', 'One', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES "
            "(99, 'PX', 'Inactive', 'inactive') "
            "ON CONFLICT (package_id) DO UPDATE SET "
            "package_code = EXCLUDED.package_code, display_name = EXCLUDED.display_name, status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'R1', 'D1', 'p', 'active') "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/platform-settings/b2c-onboarding",
        headers=_auth_header(uid),
        json={"b2c_default_assessment_package_id": 99, "b2c_default_diagnostic_package_id": 1},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_patch_engagement_notification_defaults_rejects_overlap(async_client, test_db_session):
    uid = 9104
    test_db_session.add(User(user_id=uid, age=30, phone="91040000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9104, user_id=uid, role="admin", status="active"))
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail) "
            "VALUES ('svc-a', 'A', 'whatsapp', 'a', true, false, false, false), "
            "('svc-b', 'B', 'whatsapp', 'b', true, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true"
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/platform-settings/engagement-notification-defaults",
        headers=_auth_header(uid),
        json={
            "default_questionnaire_reminder_1": "svc-a,svc-b",
            "default_questionnaire_reminder_2": "svc-b",
        },
    )
    assert response.status_code == 400
    assert "svc-b" in response.json()["message"]


@pytest.mark.asyncio
async def test_get_default_onboarding_assistants_requires_auth(async_client):
    response = await async_client.get("/platform-settings/default-onboarding-assistants")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_patch_default_onboarding_assistants_persists(async_client, test_db_session):
    uid = 9105
    test_db_session.add(User(user_id=uid, age=30, phone="91050000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9105, user_id=uid, role="admin", status="active"))

    aid = 9201
    oid = 9202
    test_db_session.add(User(user_id=aid, age=30, phone="92010000001", status="active", first_name="Ada", last_name="Admin"))
    await test_db_session.flush()
    test_db_session.add(User(user_id=oid, age=30, phone="92020000001", status="active", first_name="Omar", last_name="OA"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9201, user_id=aid, role="admin", status="active"))
    test_db_session.add(Employee(employee_id=9202, user_id=oid, role="onboarding_assistant", status="active"))

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES "
            "(1, 'P1', 'One', 'active') ON CONFLICT (package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'R1', 'D1', 'p', 'active') ON CONFLICT (diagnostic_package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/platform-settings/default-onboarding-assistants",
        headers=_auth_header(uid),
        json={"employee_ids": [9201, 9202, 9201]},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["employee_ids"] == [9201, 9202]
    assert len(data["assistants"]) == 2

    response2 = await async_client.get(
        "/platform-settings/default-onboarding-assistants",
        headers=_auth_header(uid),
    )
    assert response2.status_code == 200
    assert response2.json()["data"]["employee_ids"] == [9201, 9202]


@pytest.mark.asyncio
async def test_patch_default_onboarding_assistants_rejects_unknown_employee(async_client, test_db_session):
    uid = 9106
    test_db_session.add(User(user_id=uid, age=30, phone="91060000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9106, user_id=uid, role="admin", status="active"))
    await test_db_session.commit()

    response = await async_client.patch(
        "/platform-settings/default-onboarding-assistants",
        headers=_auth_header(uid),
        json={"employee_ids": [999999]},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_patch_default_onboarding_assistants_rejects_inactive_employee(async_client, test_db_session):
    uid = 9107
    test_db_session.add(User(user_id=uid, age=30, phone="91070000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9107, user_id=uid, role="admin", status="active"))

    inactive_uid = 9301
    test_db_session.add(User(user_id=inactive_uid, age=30, phone="93010000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=9301, user_id=inactive_uid, role="onboarding_assistant", status="inactive")
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/platform-settings/default-onboarding-assistants",
        headers=_auth_header(uid),
        json={"employee_ids": [9301]},
    )
    assert response.status_code == 422
