"""Tests for Metsights CSV import on engagements."""

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
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_packages_and_engagement(
    test_db_session,
    *,
    engagement_id: int,
    engagement_code: str = "IMP8801",
    start_date: str | None = "2026-02-01",
    end_date: str | None = "2026-02-10",
):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    sd = "NULL" if start_date is None else f"'{start_date}'"
    ed = "NULL" if end_date is None else f"'{end_date}'"
    await test_db_session.execute(
        text(
            f"INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            f"assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count, organization_id) "
            f"VALUES ({engagement_id}, 'Import Camp', '{engagement_code}', 'healthcamp', 1, 1, 'BLR', 60, {sd}, {ed}, 'active', 0, NULL)"
        )
    )
    await test_db_session.commit()


def _csv_one_row(**kwargs) -> str:
    defaults = {
        "id": "MSREC001",
        "created": "2026-01-01",
        "first": "Ann",
        "last": "Bee",
        "phone": "9876543210",
        "email": "ann@example.com",
        "gender": "F",
        "age": "28",
    }
    defaults.update(kwargs)
    return (
        "id,Created Date,First Name,Last Name,Phone #,Email,Gender,Age\n"
        f"{defaults['id']},{defaults['created']},{defaults['first']},{defaults['last']},"
        f"{defaults['phone']},{defaults['email']},{defaults['gender']},{defaults['age']}\n"
    )


@pytest.mark.asyncio
async def test_import_metsights_csv_requires_auth(async_client):
    response = await async_client.post(
        "/engagements/1/import/metsights-csv",
        files={"file": ("a.csv", _csv_one_row().encode("utf-8"), "text/csv")},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_import_metsights_csv_rejects_missing_dates(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8801, employee_id=501)
    await _seed_packages_and_engagement(test_db_session, engagement_id=8801, start_date=None, end_date=None)

    csv_body = _csv_one_row().encode("utf-8")
    response = await async_client.post(
        "/engagements/8801/import/metsights-csv",
        headers=_auth_header(8801),
        files={"file": ("a.csv", csv_body, "text/csv")},
    )
    assert response.status_code == 400
    assert "start_date" in response.json().get("message", "").lower() or "end_date" in response.json().get(
        "message", ""
    ).lower()


@pytest.mark.asyncio
async def test_import_metsights_csv_uses_end_date_when_start_null(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8802, employee_id=502)
    await _seed_packages_and_engagement(
        test_db_session, engagement_id=8802, engagement_code="IMP8802", start_date=None, end_date="2026-04-20"
    )

    response = await async_client.post(
        "/engagements/8802/import/metsights-csv",
        headers=_auth_header(8802),
        files={"file": ("a.csv", _csv_one_row(id="MSR1", phone="9111111111").encode("utf-8"), "text/csv")},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["imported"] == 1
    assert data["failed"] == 0

    slot = (
        await test_db_session.execute(
            text(
                "SELECT engagement_date, slot_start_time::text FROM engagement_time_slots "
                "WHERE engagement_id = 8802 ORDER BY time_slot_id DESC LIMIT 1"
            )
        )
    ).first()
    assert slot is not None
    assert str(slot.engagement_date) == "2026-04-20"
    assert str(slot.slot_start_time)[:5] == "10:00"


@pytest.mark.asyncio
async def test_import_metsights_csv_happy_path_and_record_id(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8803, employee_id=503)
    await _seed_packages_and_engagement(test_db_session, engagement_id=8803, engagement_code="IMP8803")

    response = await async_client.post(
        "/engagements/8803/import/metsights-csv",
        headers=_auth_header(8803),
        files={"file": ("a.csv", _csv_one_row(id="MSHAPPY", phone="9222222222").encode("utf-8"), "text/csv")},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["imported"] == 1
    assert body["skipped"] == 0

    inst = (
        await test_db_session.execute(
            text(
                "SELECT metsights_record_id, package_id FROM assessment_instances "
                "WHERE engagement_id = 8803 AND metsights_record_id = 'MSHAPPY'"
            )
        )
    ).first()
    assert inst is not None
    assert inst.package_id == 1

    pc = (
        await test_db_session.execute(text("SELECT participant_count FROM engagements WHERE engagement_id = 8803"))
    ).first()
    assert int(pc.participant_count) == 1


@pytest.mark.asyncio
async def test_import_metsights_csv_invalid_age_row_fails(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8804, employee_id=504)
    await _seed_packages_and_engagement(test_db_session, engagement_id=8804, engagement_code="IMP8804")

    csv_bad_age = (
        "id,Created Date,First Name,Last Name,Phone #,Email,Gender,Age\n"
        "MSBAD,2026-01-01,X,Y,9333333333,x@y.com,M,\n"
    )
    response = await async_client.post(
        "/engagements/8804/import/metsights-csv",
        headers=_auth_header(8804),
        files={"file": ("a.csv", csv_bad_age.encode("utf-8"), "text/csv")},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["imported"] == 0
    assert data["failed"] == 1
    assert "age" in (data["rows"][0].get("reason") or "").lower()


@pytest.mark.asyncio
async def test_import_metsights_csv_idempotent_second_run(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8805, employee_id=505)
    await _seed_packages_and_engagement(test_db_session, engagement_id=8805, engagement_code="IMP8805")
    content = _csv_one_row(id="MSIDEM", phone="9444444444").encode("utf-8")

    r1 = await async_client.post(
        "/engagements/8805/import/metsights-csv",
        headers=_auth_header(8805),
        files={"file": ("a.csv", content, "text/csv")},
    )
    assert r1.json()["data"]["imported"] == 1

    r2 = await async_client.post(
        "/engagements/8805/import/metsights-csv",
        headers=_auth_header(8805),
        files={"file": ("a.csv", content, "text/csv")},
    )
    assert r2.status_code == 200
    assert r2.json()["data"]["imported"] == 0
    assert r2.json()["data"]["skipped"] >= 1


@pytest.mark.asyncio
async def test_import_metsights_csv_same_id_two_phones_conflicts(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8806, employee_id=506)
    await _seed_packages_and_engagement(test_db_session, engagement_id=8806, engagement_code="IMP8806")
    csv_two = (
        "id,Created Date,First Name,Last Name,Phone #,Email,Gender,Age\n"
        "MSDUP,2026-01-01,A,B,9555555555,a@b.com,M,30\n"
        "MSDUP,2026-01-01,C,D,9666666666,c@d.com,F,31\n"
    )
    response = await async_client.post(
        "/engagements/8806/import/metsights-csv",
        headers=_auth_header(8806),
        files={"file": ("a.csv", csv_two.encode("utf-8"), "text/csv")},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["imported"] == 1
    assert data["failed"] == 1
