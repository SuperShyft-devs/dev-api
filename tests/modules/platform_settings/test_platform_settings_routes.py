"""Tests for platform settings (employee-only)."""

from __future__ import annotations

from datetime import timedelta

import pytest

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
    await test_db_session.commit()

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

    from modules.assessments.models import AssessmentPackage
    from modules.diagnostics.models import DiagnosticPackage

    test_db_session.add(
        AssessmentPackage(package_id=1, package_code="P1", display_name="One", status="active")
    )
    test_db_session.add(
        AssessmentPackage(package_id=2, package_code="P2", display_name="Two", status="active")
    )
    test_db_session.add(
        DiagnosticPackage(
            diagnostic_package_id=1,
            reference_id="R1",
            package_name="D1",
            diagnostic_provider="p",
            status="active",
        )
    )
    test_db_session.add(
        DiagnosticPackage(
            diagnostic_package_id=2,
            reference_id="R2",
            package_name="D2",
            diagnostic_provider="p",
            status="active",
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

    from modules.assessments.models import AssessmentPackage
    from modules.diagnostics.models import DiagnosticPackage

    test_db_session.add(
        AssessmentPackage(package_id=1, package_code="P1", display_name="One", status="active")
    )
    test_db_session.add(
        AssessmentPackage(package_id=99, package_code="PX", display_name="Inactive", status="inactive")
    )
    test_db_session.add(
        DiagnosticPackage(
            diagnostic_package_id=1,
            reference_id="R1",
            package_name="D1",
            diagnostic_provider="p",
            status="active",
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/platform-settings/b2c-onboarding",
        headers=_auth_header(uid),
        json={"b2c_default_assessment_package_id": 99, "b2c_default_diagnostic_package_id": 1},
    )
    assert response.status_code == 422
