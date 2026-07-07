"""Tests for POST /users/import-metsights-profiles."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.metsights.service import MetsightsService
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}0000000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_assessment_and_diagnostic_packages(test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (901, 'MB', 'Met Basic', '1', 'active'), (902, 'MP', 'Met Pro', '2', 'active') "
            "ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (17, 'MEN', 'Men Peak Performance', 'active'), (24, 'WMN', 'Women Peak Performance', 'active') "
            "ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.commit()


def _mock_metsights_multi(monkeypatch, *, profile_id: str, gender: str = "Female"):
    async def _get_profile(self, *, profile_id: str):
        return {
            "id": profile_id,
            "first_name": "Test",
            "last_name": "User",
            "email": "tu@example.com",
            "phone": "+919876543210",
            "gender": gender,
            "age": 35,
        }

    async def _list_records(self, *, profile_id: str, completed=None, code=None, search=None):
        return [
            {
                "id": "RECFP1",
                "date": "2026-05-07",
                "assessment_type": "FitPrint Full",
                "assessment_code": "MY_FITNESS_PRINT",
                "updated_at": "2026-05-07T15:00:00",
            },
            {
                "id": "RECNEW",
                "date": "2026-04-29",
                "assessment_type": "MetSights Pro",
                "assessment_code": "MET_PRO",
                "updated_at": "2026-05-07T17:00:00",
            },
            {
                "id": "RECOLD",
                "date": "2025-04-10",
                "assessment_type": "MetSights Basic",
                "assessment_code": "MET_BASIC",
                "updated_at": "2025-04-11T14:00:00",
            },
        ]

    async def _get_record(self, *, record_id: str):
        codes = {
            "RECOLD": "MET_BASIC",
            "RECNEW": "MET_PRO",
            "RECFP1": "MY_FITNESS_PRINT",
        }
        return {
            "id": record_id,
            "assessment_code": codes.get(record_id, "MET_PRO"),
            "is_complete": False,
            "profile": {"id": profile_id},
        }

    monkeypatch.setattr(MetsightsService, "get_profile_detail", _get_profile)
    monkeypatch.setattr(MetsightsService, "list_profile_records", _list_records)
    monkeypatch.setattr(MetsightsService, "get_record_detail", _get_record)


@pytest.mark.asyncio
async def test_import_metsights_profiles_requires_auth(async_client):
    response = await async_client.post(
        "/users/import-metsights-profiles",
        json={"metsights_profile_ids": ["prof-001"]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_import_metsights_profiles_order_fitprint_skip_and_female_diagnostic(
    async_client, test_db_session, monkeypatch
):
    pid = "prof-import-ord-01"
    _mock_metsights_multi(monkeypatch, profile_id=pid, gender="Female")
    await _seed_assessment_and_diagnostic_packages(test_db_session)
    await _seed_employee(test_db_session, user_id=9901, employee_id=601)

    response = await async_client.post(
        "/users/import-metsights-profiles",
        headers=_auth_header(9901),
        json={"metsights_profile_ids": [pid]},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert len(body["profiles"]) == 1
    prof = body["profiles"][0]
    assert prof["metsights_profile_id"] == pid
    assert prof["error"] is None
    assert prof["user_id"] is not None

    statuses = {r["metsights_record_id"]: r for r in prof["records"]}
    assert statuses["RECOLD"]["status"] == "imported"
    assert statuses["RECNEW"]["status"] == "imported"
    assert statuses["RECFP1"]["status"] == "skipped"
    assert statuses["RECFP1"]["reason"] == "fitprint"

    uid = prof["user_id"]
    user_row = (
        await test_db_session.execute(text("SELECT metsights_profile_id, gender FROM users WHERE user_id = :u"), {"u": uid})
    ).first()
    assert user_row.metsights_profile_id == pid
    assert (user_row.gender or "").lower() == "female"

    eng_rows = (
        await test_db_session.execute(
            text(
                "SELECT e.engagement_id, e.diagnostic_package_id, e.create_profile_on_metsights, e.enroll_for_fitprint_full, "
                "e.start_date, ai.metsights_record_id, ap.assessment_type_code "
                "FROM engagements e "
                "JOIN assessment_instances ai ON ai.engagement_id = e.engagement_id "
                "JOIN assessment_packages ap ON ap.package_id = ai.package_id "
                "WHERE ai.user_id = :u AND e.organization_id IS NULL "
                "ORDER BY e.start_date ASC, ai.metsights_record_id ASC"
            ),
            {"u": uid},
        )
    ).all()
    assert len(eng_rows) == 2
    assert eng_rows[0].metsights_record_id == "RECOLD"
    assert str(eng_rows[0].assessment_type_code) == "1"
    assert eng_rows[1].metsights_record_id == "RECNEW"
    assert str(eng_rows[1].assessment_type_code) == "2"
    for er in eng_rows:
        assert int(er.diagnostic_package_id) == 24
        assert er.create_profile_on_metsights is True
        assert er.enroll_for_fitprint_full is False

    part = (
        await test_db_session.execute(
            text(
                "SELECT is_profile_created_on_metsights, is_primary_record_id_synced "
                "FROM engagement_participants WHERE user_id = :u"
            ),
            {"u": uid},
        )
    ).all()
    assert len(part) == 2
    for p in part:
        assert p.is_profile_created_on_metsights is True
        assert p.is_primary_record_id_synced is True


@pytest.mark.asyncio
async def test_import_metsights_profiles_male_uses_diagnostic_17(async_client, test_db_session, monkeypatch):
    pid = "prof-import-male-01"

    async def _get_profile(self, *, profile_id: str):
        return {
            "id": profile_id,
            "first_name": "Bob",
            "last_name": "M",
            "email": "bob@example.com",
            "phone": "+919811122233",
            "gender": "Male",
            "age": 40,
        }

    async def _list_records(self, *, profile_id: str, completed=None, code=None, search=None):
        return [
            {
                "id": "RECPRO1",
                "date": "2026-01-10",
                "assessment_code": "MET_PRO",
                "assessment_type": "MetSights Pro",
            }
        ]

    async def _get_record(self, *, record_id: str):
        return {
            "id": record_id,
            "assessment_code": "MET_PRO",
            "is_complete": True,
            "profile": {"id": pid},
        }

    monkeypatch.setattr(MetsightsService, "get_profile_detail", _get_profile)
    monkeypatch.setattr(MetsightsService, "list_profile_records", _list_records)
    monkeypatch.setattr(MetsightsService, "get_record_detail", _get_record)

    await _seed_assessment_and_diagnostic_packages(test_db_session)
    await _seed_employee(test_db_session, user_id=9902, employee_id=602)

    response = await async_client.post(
        "/users/import-metsights-profiles",
        headers=_auth_header(9902),
        json={"metsights_profile_ids": [pid]},
    )
    assert response.status_code == 200
    prof = response.json()["data"]["profiles"][0]
    assert prof["records"][0]["status"] == "imported"
    assert prof["records"][0]["diagnostic_package_id"] == 17

    inst = (
        await test_db_session.execute(
            text("SELECT status FROM assessment_instances WHERE metsights_record_id = 'RECPRO1'")
        )
    ).first()
    assert inst.status == "completed"


@pytest.mark.asyncio
async def test_import_metsights_profiles_skips_existing_record_id(async_client, test_db_session, monkeypatch):
    pid = "prof-import-dup-01"

    async def _get_profile(self, *, profile_id: str):
        return {
            "id": profile_id,
            "first_name": "Ann",
            "last_name": "X",
            "email": "ann@example.com",
            "phone": "+919833344455",
            "gender": "Female",
            "age": 32,
        }

    async def _list_records(self, *, profile_id: str, completed=None, code=None, search=None):
        return [
            {"id": "RECDUP", "date": "2026-02-01", "assessment_code": "MET_PRO", "assessment_type": "MetSights Pro"},
        ]

    async def _get_record(self, *, record_id: str):
        return {
            "id": record_id,
            "assessment_code": "MET_PRO",
            "is_complete": False,
            "profile": {"id": pid},
        }

    monkeypatch.setattr(MetsightsService, "get_profile_detail", _get_profile)
    monkeypatch.setattr(MetsightsService, "list_profile_records", _list_records)
    monkeypatch.setattr(MetsightsService, "get_record_detail", _get_record)

    await _seed_assessment_and_diagnostic_packages(test_db_session)
    await _seed_employee(test_db_session, user_id=9903, employee_id=603)

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, last_name, age, phone, status, is_participant, relationship) "
            "VALUES (9904, 'Other', 'User', 30, '9904000000000', 'active', true, 'self') "
            "ON CONFLICT (user_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, organization_id) "
            "VALUES (99040, 'Prior', 'CODE99040', 'bio_ai', 902, 24, 'X', 20, '2026-02-01', '2026-02-01', 'active', 0, NULL) "
            "ON CONFLICT (engagement_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_instances (user_id, engagement_id, package_id, status, metsights_record_id, assigned_at) "
            "VALUES (9904, 99040, 902, 'active', 'RECDUP', NOW()) "
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/users/import-metsights-profiles",
        headers=_auth_header(9903),
        json={"metsights_profile_ids": [pid]},
    )
    assert response.status_code == 200
    prof = response.json()["data"]["profiles"][0]
    assert prof["records"][0]["status"] == "skipped"
    assert prof["records"][0]["reason"] == "already imported"
