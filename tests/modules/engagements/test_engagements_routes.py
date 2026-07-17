"""Integration tests for engagements routes."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1, role: str = "admin"):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role=role, status="active"))
    await test_db_session.commit()


async def _seed_organization(test_db_session, *, organization_id: int, name: str = "Test Org"):
    from modules.organizations.models import Organization
    test_db_session.add(
        Organization(
            organization_id=organization_id,
            name=name,
            organization_type="corporate",
            status="active",
        )
    )
    await test_db_session.commit()


async def _seed_assessment_package(test_db_session, *, package_id: int, package_code: str = "PKG1"):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (:pid, :pcode, :dname, 'active') ON CONFLICT (package_id) DO UPDATE SET "
            "package_code = EXCLUDED.package_code, display_name = EXCLUDED.display_name, status = EXCLUDED.status"
        ),
        {"pid": package_id, "pcode": package_code, "dname": f"Test Package {package_id}"},
    )
    await test_db_session.commit()


async def _seed_notification_service(
    test_db_session,
    service_key: str = "booking-alert-whatsapp",
    *,
    require_participant_detail: bool = False,
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail) "
            "VALUES (:sk, :dn, 'whatsapp', 'booking-alert', true, false, false, :rpd) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_participant_detail = :rpd"
        ),
        {"sk": service_key, "dn": service_key, "rpd": require_participant_detail},
    )
    await test_db_session.commit()


@pytest.fixture(autouse=True)
async def _seed_default_notification_service(test_db_session):
    await _seed_notification_service(test_db_session)


async def _seed_diagnostic_package(test_db_session, *, diagnostic_package_id: int):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, bookings_count) "
            "VALUES (:did, :ref, :pname, 'test_provider', 'active', 0) ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status"
        ),
        {
            "did": diagnostic_package_id,
            "ref": f"REF{diagnostic_package_id}",
            "pname": f"Test Diagnostic Package {diagnostic_package_id}",
        },
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_create_engagement_requires_auth(async_client):
    payload = {
        "engagement_name": "Camp",
        "organization_id": 1,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-01",
    }

    response = await async_client.post("/engagements", json=payload)
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_engagement_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=7001, age=30, phone="7001000000", status="active"))
    await test_db_session.commit()

    payload = {
        "engagement_name": "Camp",
        "organization_id": 1,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-01",
    }

    response = await async_client.post("/engagements", headers=_auth_header(7001), json=payload)
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_engagement_creates_row(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7002, employee_id=10)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization 1")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)
    await test_db_session.execute(text("DELETE FROM platform_settings"))
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "default_onboarding_assistant_employee_ids) "
            "VALUES (1, 1, 1, '10')"
        )
    )
    await test_db_session.commit()

    payload = {
        "engagement_name": "Camp",
        "organization_id": 1,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-02",
    }

    response = await async_client.post("/engagements", headers=_auth_header(7002), json=payload)
    assert response.status_code == 201

    engagement_id = response.json()["data"]["engagement_id"]
    assert isinstance(engagement_id, int)

    details = await async_client.get(f"/engagements/{engagement_id}", headers=_auth_header(7002))
    assert details.status_code == 200
    body = details.json()
    detail_data = body.get("data", body)
    assert detail_data["onboarding_notification"] == "booking-alert-whatsapp"
    assert detail_data["camp_no"] == 1010226

    assistants = await async_client.get(
        f"/engagements/{engagement_id}/onboarding-assistants",
        headers=_auth_header(7002),
    )
    assert assistants.status_code == 200
    assistant_ids = sorted(a["employee_id"] for a in assistants.json()["data"])
    assert assistant_ids == [10]


@pytest.mark.asyncio
async def test_create_engagement_persists_location_fields(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7012, employee_id=18)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization 1")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    payload = {
        "engagement_name": "Location Camp",
        "organization_id": 1,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "address": "Marol Naka, Andheri",
        "sub_locality": "Saki Naka",
        "landmark": "Marol Naka (Line 1)",
        "city": "Mumbai",
        "pincode": "400072",
        "state": "Maharashtra",
        "country": "India",
        "latitude": 19.1083663,
        "longitude": 72.8788727,
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-02",
    }

    response = await async_client.post("/engagements", headers=_auth_header(7012), json=payload)
    assert response.status_code == 201
    engagement_id = response.json()["data"]["engagement_id"]

    details = await async_client.get(f"/engagements/{engagement_id}", headers=_auth_header(7012))
    assert details.status_code == 200
    data = details.json()["data"]
    assert data["address"] == "Marol Naka, Andheri"
    assert data["sub_locality"] == "Saki Naka"
    assert data["landmark"] == "Marol Naka (Line 1)"
    assert data["city"] == "Mumbai"
    assert data["pincode"] == "400072"
    assert data["state"] == "Maharashtra"
    assert data["country"] == "India"
    assert data["latitude"] == pytest.approx(19.1083663)
    assert data["longitude"] == pytest.approx(72.8788727)

    listed = await async_client.get("/engagements", headers=_auth_header(7012))
    assert listed.status_code == 200
    row = next(item for item in listed.json()["data"] if item["engagement_id"] == engagement_id)
    assert row["sub_locality"] == "Saki Naka"
    assert row["state"] == "Maharashtra"
    assert row["latitude"] == pytest.approx(19.1083663)

    update_payload = {
        "engagement_name": "Location Camp",
        "engagement_code": data["engagement_code"],
        "organization_id": 1,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "address": "Updated Address",
        "sub_locality": "Andheri East",
        "landmark": "Metro Station",
        "city": "Mumbai",
        "pincode": "400093",
        "state": "Maharashtra",
        "country": "India",
        "latitude": 19.12,
        "longitude": 72.88,
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-02",
    }
    updated = await async_client.put(
        f"/engagements/{engagement_id}", headers=_auth_header(7012), json=update_payload
    )
    assert updated.status_code == 200

    details2 = await async_client.get(f"/engagements/{engagement_id}", headers=_auth_header(7012))
    data2 = details2.json()["data"]
    assert data2["address"] == "Updated Address"
    assert data2["sub_locality"] == "Andheri East"
    assert data2["landmark"] == "Metro Station"
    assert data2["pincode"] == "400093"
    assert data2["latitude"] == pytest.approx(19.12)
    assert data2["longitude"] == pytest.approx(72.88)


@pytest.mark.asyncio
async def test_create_engagement_sets_camp_no_for_org_8(async_client, test_db_session):

    await _seed_employee(test_db_session, user_id=7010, employee_id=16)
    await _seed_organization(test_db_session, organization_id=8, name="Org Eight")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    payload = {
        "engagement_name": "June Camp",
        "organization_id": 8,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-06-23",
        "end_date": "2026-06-24",
    }

    response = await async_client.post("/engagements", headers=_auth_header(7010), json=payload)
    assert response.status_code == 201

    engagement_id = response.json()["data"]["engagement_id"]
    details = await async_client.get(f"/engagements/{engagement_id}", headers=_auth_header(7010))
    detail_data = details.json()["data"]
    assert detail_data["camp_no"] == 8230626


@pytest.mark.asyncio
async def test_update_engagement_recalculates_camp_no(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7011, employee_id=17)
    await _seed_organization(test_db_session, organization_id=8, name="Org Eight")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=8110,
            engagement_name="Camp",
            organization_id=8,
            camp_no=8230626,
            engagement_code="UPD8",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 6, 23),
            end_date=date(2026, 6, 24),
            status="running",
        )
    )
    await test_db_session.commit()

    payload = {
        "engagement_name": "Camp",
        "engagement_code": "UPD8",
        "organization_id": 8,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-07-01",
        "end_date": "2026-07-02",
    }

    response = await async_client.put("/engagements/8110", headers=_auth_header(7011), json=payload)
    assert response.status_code == 200

    details = await async_client.get("/engagements/8110", headers=_auth_header(7011))
    assert details.json()["data"]["camp_no"] == 8010726


@pytest.mark.asyncio
async def test_create_engagement_allows_null_diagnostic_package(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7008, employee_id=15)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization 1")

    payload = {
        "engagement_name": "Camp",
        "organization_id": 1,
        "engagement_type": "doctor",
        "assessment_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-02",
    }

    response = await async_client.post("/engagements", headers=_auth_header(7008), json=payload)
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_list_engagements_paginates_and_filters(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7003, employee_id=11)
    await _seed_organization(test_db_session, organization_id=1, name="Org 1")
    await _seed_organization(test_db_session, organization_id=2, name="Org 2")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    # Create engagements
    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=8101,
            engagement_name="E1",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="CODE1",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=8102,
            engagement_name="E2",
            metsights_engagement_id=None,
            organization_id=2,
            engagement_code="CODE2",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="DEL",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 3),
            status="completed",
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/engagements?page=1&limit=10&org_id=1&status=running&city=BLR&date=2026-02-01",
        headers=_auth_header(7003),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 10
    assert body["meta"]["total"] >= 1

    assert len(body["data"]) == 1
    assert body["data"][0]["engagement_id"] == 8101
    assert body["data"][0]["onboarding_notification"] == "booking-alert-whatsapp"
    assert body["data"][0]["assessment_package_id"] == 1


@pytest.mark.asyncio
async def test_list_engagements_filters_by_multiple_statuses(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=70031, employee_id=31)
    await _seed_organization(test_db_session, organization_id=1, name="Org 1")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    from modules.engagements.models import Engagement

    for engagement_id, name, status in [
        (8111, "Scheduled Camp", "scheduled"),
        (8112, "Running Camp", "running"),
        (8113, "Completed Camp", "completed"),
    ]:
        test_db_session.add(
            Engagement(
                engagement_id=engagement_id,
                engagement_name=name,
                metsights_engagement_id=None,
                organization_id=1,
                engagement_code=f"CODE{engagement_id}",
                engagement_type="bio_ai",
                assessment_package_id=1,
                diagnostic_package_id=1,
                city="BLR",
                slot_duration=20,
                start_date=date(2026, 2, 1),
                end_date=date(2026, 2, 3),
                status=status,
            )
        )
    await test_db_session.commit()

    response = await async_client.get(
        "/engagements?page=1&limit=10&org_id=1&status=scheduled,running",
        headers=_auth_header(70031),
    )

    assert response.status_code == 200
    body = response.json()
    returned_ids = {row["engagement_id"] for row in body["data"]}
    assert returned_ids == {8111, 8112}
    assert 8113 not in returned_ids
    assert body["meta"]["total"] == 2

    invalid_response = await async_client.get(
        "/engagements?page=1&limit=10&status=running,invalid",
        headers=_auth_header(70031),
    )
    assert invalid_response.status_code == 400


@pytest.mark.asyncio
async def test_resolve_healthians_zone_for_non_healthians_package(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=70032, employee_id=32)
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    response = await async_client.post(
        "/engagements/resolve-healthians-zone",
        headers=_auth_header(70032),
        json={
            "diagnostic_package_id": 1,
            "latitude": 12.9716,
            "longitude": 77.5946,
            "pincode": "560001",
        },
    )

    assert response.status_code == 200
    body = response.json()["data"]
    assert body["serviceable"] is False
    assert body["zone_id"] is None
    assert "Healthians" in body["message"]


@pytest.mark.asyncio
async def test_resolve_healthians_zone_serviceable(async_client, test_db_session, monkeypatch):
    await _seed_employee(test_db_session, user_id=70033, employee_id=33)
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, bookings_count) "
            "VALUES (99, 'REF99', 'Healthians Package', 'healthians', 'active', 0) "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET diagnostic_provider = EXCLUDED.diagnostic_provider"
        )
    )
    await test_db_session.commit()

    async def _fake_token():
        return "token"

    async def _fake_serviceability(*_args, **_kwargs):
        return {"status": True, "data": {"zone_id": 42}, "message": "Serviceable"}

    monkeypatch.setattr(
        "modules.diagnostics.healthians.client.get_access_token",
        _fake_token,
    )
    monkeypatch.setattr(
        "modules.diagnostics.healthians.client.check_serviceability_by_location_v2",
        _fake_serviceability,
    )

    response = await async_client.post(
        "/engagements/resolve-healthians-zone",
        headers=_auth_header(70033),
        json={
            "diagnostic_package_id": 99,
            "latitude": 12.9716,
            "longitude": 77.5946,
            "pincode": "560001",
        },
    )

    assert response.status_code == 200
    body = response.json()["data"]
    assert body["serviceable"] is True
    assert body["zone_id"] == "42"
    assert "auto-filled" in body["message"].lower()


@pytest.mark.asyncio
async def test_list_engagements_filters_by_audience(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7004, employee_id=12)
    await _seed_organization(test_db_session, organization_id=1, name="Org 1")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=8201,
            engagement_name="B2B Camp",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="B2B1",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=8202,
            engagement_name="B2C Public",
            metsights_engagement_id=None,
            organization_id=None,
            engagement_code="B2C1",
            engagement_type="doctor",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="Mumbai",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    await test_db_session.commit()

    b2b_response = await async_client.get(
        "/engagements?page=1&limit=10&audience=b2b&search=B2B",
        headers=_auth_header(7004),
    )
    assert b2b_response.status_code == 200
    b2b_body = b2b_response.json()
    b2b_ids = {row["engagement_id"] for row in b2b_body["data"]}
    assert 8201 in b2b_ids
    assert 8202 not in b2b_ids
    assert all(row["organization_id"] is not None for row in b2b_body["data"])

    b2c_response = await async_client.get(
        "/engagements?page=1&limit=10&audience=b2c&search=B2C",
        headers=_auth_header(7004),
    )
    assert b2c_response.status_code == 200
    b2c_body = b2c_response.json()
    b2c_ids = {row["engagement_id"] for row in b2c_body["data"]}
    assert 8202 in b2c_ids
    assert 8201 not in b2c_ids
    assert all(row["organization_id"] is None for row in b2c_body["data"])

    invalid_response = await async_client.get(
        "/engagements?audience=invalid",
        headers=_auth_header(7004),
    )
    assert invalid_response.status_code == 400


@pytest.mark.asyncio
async def test_list_engagements_filters_by_camp_no(async_client, test_db_session):
    from modules.engagements.camp_no import compute_camp_no
    from modules.engagements.models import Engagement

    await _seed_employee(test_db_session, user_id=7005, employee_id=13)
    await _seed_organization(test_db_session, organization_id=1, name="Org 1")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    start_a = date(2026, 2, 1)
    start_b = date(2026, 3, 1)
    camp_a = compute_camp_no(1, start_a)
    camp_b = compute_camp_no(1, start_b)

    test_db_session.add(
        Engagement(
            engagement_id=8111,
            engagement_name="Camp A Eng",
            organization_id=1,
            camp_no=camp_a,
            engagement_code="CAMPA1",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=start_a,
            end_date=start_a,
            status="running",
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=8112,
            engagement_name="Camp B Eng",
            organization_id=1,
            camp_no=camp_b,
            engagement_code="CAMPB1",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=start_b,
            end_date=start_b,
            status="running",
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        f"/engagements?page=1&limit=10&camp_no={camp_a}",
        headers=_auth_header(7005),
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 1
    assert len(body["data"]) == 1
    assert body["data"][0]["engagement_id"] == 8111
    assert body["data"][0]["camp_no"] == camp_a


@pytest.mark.asyncio
async def test_get_engagement_details_returns_row(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7004, employee_id=12)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=8201,
            engagement_name="E1",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="CODE3",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/engagements/8201", headers=_auth_header(7004))
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["engagement_id"] == 8201
    assert body["onboarding_notification"] is None
    assert body["assessment_package_id"] == 1


@pytest.mark.asyncio
async def test_update_engagement_updates_fields(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7005, employee_id=13)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=8301,
            engagement_name="Old",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="CODE4",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    await test_db_session.commit()

    payload = {
        "engagement_name": "New",
        "engagement_code": "CODE4-NEW",
        "organization_id": 1,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "Pune",
        "slot_duration": 30,
        "start_date": "2026-02-01",
        "end_date": "2026-02-02",
        "metsights_engagement_id": "MS1",
    }

    response = await async_client.put("/engagements/8301", headers=_auth_header(7005), json=payload)
    assert response.status_code == 200

    updated = await test_db_session.get(Engagement, 8301)
    assert updated is not None
    assert updated.engagement_name == "New"
    assert updated.engagement_code == "CODE4-NEW"
    assert updated.city == "Pune"
    assert updated.slot_duration == 30
    assert updated.metsights_engagement_id == "MS1"


@pytest.mark.asyncio
async def test_update_b2c_engagement_without_organization(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7005, employee_id=13)
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=2)

    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=8305,
            engagement_name="B2C Camp",
            metsights_engagement_id=None,
            organization_id=None,
            engagement_code="B2C-CODE",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 5, 21),
            end_date=date(2026, 5, 21),
            status="running",
        )
    )
    await test_db_session.commit()

    payload = {
        "engagement_name": "B2C Camp",
        "engagement_code": "B2C-CODE",
        "organization_id": None,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 2,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-05-21",
        "end_date": "2026-05-21",
        "metsights_engagement_id": None,
    }

    response = await async_client.put("/engagements/8305", headers=_auth_header(7005), json=payload)
    assert response.status_code == 200

    updated = await test_db_session.get(Engagement, 8305)
    assert updated is not None
    assert updated.organization_id is None
    assert updated.diagnostic_package_id == 2


@pytest.mark.asyncio
async def test_update_engagement_rejects_duplicate_engagement_code(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7005, employee_id=13)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    from modules.engagements.models import Engagement

    test_db_session.add_all(
        [
            Engagement(
                engagement_id=8302,
                engagement_name="A",
                organization_id=1,
                engagement_code="TAKEN",
                engagement_type="bio_ai",
                assessment_package_id=1,
                diagnostic_package_id=1,
                city="BLR",
                slot_duration=20,
                start_date=date(2026, 2, 1),
                end_date=date(2026, 2, 1),
                status="running",
            ),
            Engagement(
                engagement_id=8303,
                engagement_name="B",
                organization_id=1,
                engagement_code="MINE",
                engagement_type="bio_ai",
                assessment_package_id=1,
                diagnostic_package_id=1,
                city="BLR",
                slot_duration=20,
                start_date=date(2026, 2, 1),
                end_date=date(2026, 2, 1),
                status="running",
            ),
        ]
    )
    await test_db_session.commit()

    payload = {
        "engagement_name": "B",
        "engagement_code": "TAKEN",
        "organization_id": 1,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-01",
    }

    response = await async_client.put("/engagements/8303", headers=_auth_header(7005), json=payload)
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_get_occupied_slots_by_engagement_code_is_public(async_client):
    # No auth header required
    response = await async_client.get("/engagements/code/ENG1/occupied-slots")
    assert response.status_code in (200, 404)


@pytest.mark.asyncio
async def test_get_occupied_slots_by_engagement_code_returns_grouped_slots(async_client, test_db_session):
    from modules.engagements.models import Engagement, EngagementParticipant
    from modules.users.models import User
    from datetime import time

    await _seed_organization(test_db_session, organization_id=1, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    # Create users for time slots
    test_db_session.add(User(user_id=1001, age=30, phone="1001000000", status="active"))
    test_db_session.add(User(user_id=1002, age=30, phone="1002000000", status="active"))
    test_db_session.add(User(user_id=1003, age=30, phone="1003000000", status="active"))
    await test_db_session.flush()

    test_db_session.add(
        Engagement(
            engagement_id=9101,
            engagement_name="E",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="ENG9101",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 2),
            status="running",
        )
    )
    await test_db_session.commit()

    test_db_session.add_all(
        [
            EngagementParticipant(
                engagement_participant_id=1,
                engagement_id=9101,
                user_id=1001,
                engagement_date=date(2026, 2, 1),
                slot_start_time=time(10, 0),
            ),
            EngagementParticipant(
                engagement_participant_id=2,
                engagement_id=9101,
                user_id=1002,
                engagement_date=date(2026, 2, 1),
                slot_start_time=time(10, 20),
            ),
            EngagementParticipant(
                engagement_participant_id=3,
                engagement_id=9101,
                user_id=1003,
                engagement_date=date(2026, 2, 2),
                slot_start_time=time(11, 0),
            ),
        ]
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/engagements/code/ENG9101/occupied-slots",
    )

    assert response.status_code == 200
    occupied = response.json()["data"]["occupied_slots"]
    assert occupied == {
        "2026-02-01": ["10:00:00", "10:20:00"],
        "2026-02-02": ["11:00:00"],
    }


@pytest.mark.asyncio
async def test_get_public_occupied_slots_is_public(async_client, test_db_session):
    # No auth header required
    response = await async_client.get("/engagements/public/occupied-slots")
    assert response.status_code == 200
    assert response.json()["data"]["occupied_slots"] == {}


@pytest.mark.asyncio
async def test_get_public_occupied_slots_returns_only_active_b2c(async_client, test_db_session):
    from modules.engagements.models import Engagement, EngagementParticipant
    from modules.users.models import User
    from datetime import time

    await _seed_organization(test_db_session, organization_id=99, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    # Create user for time slots
    test_db_session.add(User(user_id=1001, age=30, phone="1001000000", status="active"))
    await test_db_session.flush()

    # Active B2C (organization_id is None)
    test_db_session.add(
        Engagement(
            engagement_id=9201,
            engagement_name="B2C1",
            metsights_engagement_id=None,
            organization_id=None,
            engagement_code="B2C1",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )

    # Active B2B (must be excluded)
    test_db_session.add(
        Engagement(
            engagement_id=9202,
            engagement_name="B2B1",
            metsights_engagement_id=None,
            organization_id=99,
            engagement_code="B2B1",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )

    # Inactive B2C (must be excluded)
    test_db_session.add(
        Engagement(
            engagement_id=9203,
            engagement_name="B2C2",
            metsights_engagement_id=None,
            organization_id=None,
            engagement_code="B2C2",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="completed",
        )
    )

    await test_db_session.commit()

    test_db_session.add_all(
        [
            EngagementParticipant(
                engagement_participant_id=10,
                engagement_id=9201,
                user_id=1001,
                engagement_date=date(2026, 2, 1),
                slot_start_time=time(9, 0),
            ),
            EngagementParticipant(
                engagement_participant_id=11,
                engagement_id=9202,
                user_id=1001,
                engagement_date=date(2026, 2, 1),
                slot_start_time=time(9, 20),
            ),
            EngagementParticipant(
                engagement_participant_id=12,
                engagement_id=9203,
                user_id=1001,
                engagement_date=date(2026, 2, 1),
                slot_start_time=time(9, 40),
            ),
        ]
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/engagements/public/occupied-slots",
    )

    assert response.status_code == 200
    occupied = response.json()["data"]["occupied_slots"]
    assert occupied == {"2026-02-01": ["09:00:00"]}


@pytest.mark.asyncio
async def test_patch_engagement_status_changes_status(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7006, employee_id=14)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=8401,
            engagement_name="E",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="CODE5",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/engagements/8401/status",
        headers=_auth_header(7006),
        json={"status": "completed"},
    )
    assert response.status_code == 200

    updated = await test_db_session.get(Engagement, 8401)
    assert updated is not None
    assert (updated.status or "").lower() == "completed"


@pytest.mark.asyncio
async def test_delete_engagement_requires_auth(async_client):
    response = await async_client.delete("/engagements/1")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_delete_engagement_removes_scoped_data_but_not_users(async_client, test_db_session):
    from datetime import time

    from modules.assessments.models import AssessmentInstance
    from modules.engagements.models import Engagement, EngagementParticipant
    from modules.reports.models import IndividualHealthReport
    from modules.users.models import User

    await _seed_employee(test_db_session, user_id=7010, employee_id=20)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    test_db_session.add(User(user_id=1010, age=30, phone="1010000000", status="active"))
    await test_db_session.flush()

    test_db_session.add(
        Engagement(
            engagement_id=8501,
            engagement_name="Delete Me",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="DEL8501",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=85001,
            engagement_id=8501,
            user_id=1010,
            engagement_date=date(2026, 2, 1),
            slot_start_time=time(10, 0),
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=85001,
            user_id=1010,
            engagement_id=8501,
            package_id=1,
            status="assigned",
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        IndividualHealthReport(
            report_id=85001,
            user_id=1010,
            assessment_instance_id=85001,
            engagement_id=8501,
            reports={},
            blood_parameters={},
        )
    )
    await test_db_session.commit()

    response = await async_client.delete("/engagements/8501", headers=_auth_header(7010))
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["engagement_id"] == 8501
    assert body["deleted_engagement_participants"] >= 1
    assert body["deleted_assessment_instances"] >= 1

    assert await test_db_session.get(Engagement, 8501) is None
    assert await test_db_session.get(User, 1010) is not None

    participants = (
        await test_db_session.execute(
            text("SELECT COUNT(*) FROM engagement_participants WHERE engagement_id = 8501")
        )
    ).scalar_one()
    assert participants == 0

    instances = (
        await test_db_session.execute(
            text("SELECT COUNT(*) FROM assessment_instances WHERE engagement_id = 8501")
        )
    ).scalar_one()
    assert instances == 0


@pytest.mark.asyncio
async def test_delete_participant_clears_notification_refs_before_instance_delete(
    async_client, test_db_session
):
    """Participant purge must detach notifications that reference assessment instances."""

    from datetime import time

    from modules.assessments.models import AssessmentInstance
    from modules.engagements.models import Engagement, EngagementParticipant
    from modules.notifications.models import Notification

    await _seed_employee(test_db_session, user_id=7020, employee_id=21)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    test_db_session.add(User(user_id=1020, age=30, phone="1020000000", status="active"))
    await test_db_session.flush()

    test_db_session.add(
        Engagement(
            engagement_id=8601,
            engagement_name="Participant Delete",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="PD8601",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=86001,
            engagement_id=8601,
            user_id=1020,
            engagement_date=date(2026, 2, 1),
            slot_start_time=time(10, 0),
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=86001,
            user_id=1020,
            engagement_id=8601,
            package_id=1,
            status="assigned",
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        Notification(
            service_key="booking-alert-whatsapp",
            status="sent",
            channel="whatsapp",
            engagement_id=8601,
            assessment_instance_id=86001,
        )
    )
    await test_db_session.commit()

    response = await async_client.delete(
        "/engagements/8601/participants/1020",
        headers=_auth_header(7020),
    )
    assert response.status_code == 200, response.text
    body = response.json()["data"]
    assert body["engagement_id"] == 8601
    assert body["user_id"] == 1020
    assert body["deleted_assessment_instances"] >= 1

    participants = (
        await test_db_session.execute(
            text("SELECT COUNT(*) FROM engagement_participants WHERE engagement_id = 8601")
        )
    ).scalar_one()
    assert participants == 0

    instances = (
        await test_db_session.execute(
            text("SELECT COUNT(*) FROM assessment_instances WHERE engagement_id = 8601")
        )
    ).scalar_one()
    assert instances == 0

    notif_assessment_id = (
        await test_db_session.execute(
            text("SELECT assessment_instance_id FROM notifications WHERE engagement_id = 8601")
        )
    ).scalar_one()
    assert notif_assessment_id is None


@pytest.mark.asyncio
async def test_patch_participant_department_updates_slug(async_client, test_db_session):
    from datetime import time

    from modules.engagements.models import Engagement, EngagementParticipant
    from modules.organizations.models import Organization

    await _seed_employee(test_db_session, user_id=7030, employee_id=31)
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    test_db_session.add(
        Organization(
            organization_id=8701,
            name="Patch Dept Org",
            status="active",
            departments=[
                {"department": "Sales", "slug": "sales"},
                {"department": "Marketing", "slug": "marketing"},
            ],
        )
    )
    test_db_session.add(User(user_id=1030, age=30, phone="1030000000", status="active"))
    await test_db_session.flush()

    test_db_session.add(
        Engagement(
            engagement_id=8701,
            engagement_name="Patch Dept Engagement",
            organization_id=8701,
            engagement_code="PD8701",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=87001,
            engagement_id=8701,
            user_id=1030,
            engagement_date=date(2026, 2, 1),
            slot_start_time=time(10, 0),
            participant_department=None,
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/engagements/8701/participants/1030",
        headers=_auth_header(7030),
        json={"participant_department": "marketing"},
    )
    assert response.status_code == 200
    assert response.json()["data"]["participant_department"] == "marketing"

    row = (
        await test_db_session.execute(
            text(
                "SELECT participant_department FROM engagement_participants "
                "WHERE engagement_id = 8701 AND user_id = 1030"
            )
        )
    ).first()
    assert row.participant_department == "marketing"


async def _seed_engagement_participant_for_patch(
    test_db_session,
    *,
    engagement_id: int = 8702,
    user_id: int = 1031,
    employee_user_id: int = 7031,
    employee_id: int = 32,
):
    from datetime import time

    from modules.engagements.models import Engagement, EngagementParticipant
    from modules.experts.models import ConsultationBooking
    from modules.organizations.models import Organization

    await _seed_employee(test_db_session, user_id=employee_user_id, employee_id=employee_id)
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    test_db_session.add(
        Organization(
            organization_id=engagement_id,
            name="Patch Consult Org",
            status="active",
        )
    )
    test_db_session.add(User(user_id=user_id, age=30, phone="1031000000", status="active"))
    await test_db_session.flush()

    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Patch Consult Engagement",
            organization_id=engagement_id,
            engagement_code=f"PC{engagement_id}",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=engagement_id * 10 + 1,
            engagement_id=engagement_id,
            user_id=user_id,
            engagement_date=date(2026, 2, 1),
            slot_start_time=time(10, 0),
            consultation_booking_ids=[87021, 87022],
        )
    )
    await test_db_session.flush()
    test_db_session.add_all(
        [
            ConsultationBooking(
                consultation_id=87021,
                engagement_participant_id=engagement_id * 10 + 1,
                expert_type="doctor",
                want=False,
            ),
            ConsultationBooking(
                consultation_id=87022,
                engagement_participant_id=engagement_id * 10 + 1,
                expert_type="nutritionist",
                want=True,
            ),
        ]
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_patch_participant_consultation_fields(async_client, test_db_session):
    await _seed_engagement_participant_for_patch(test_db_session)

    response = await async_client.patch(
        "/engagements/8702/participants/1031",
        headers=_auth_header(7031),
        json={"consultations": {"doctor": {"want": True}}},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["consultations"]["doctor"]["want"] is True

    row = (
        await test_db_session.execute(
            text(
                "SELECT want FROM consultation_bookings "
                "WHERE engagement_participant_id = 87021 AND expert_type = 'doctor'"
            )
        )
    ).first()
    assert row.want is True


@pytest.mark.asyncio
async def test_patch_participant_consultation_null(async_client, test_db_session):
    await _seed_engagement_participant_for_patch(test_db_session)

    response = await async_client.patch(
        "/engagements/8702/participants/1031",
        headers=_auth_header(7031),
        json={"consultations": {"nutritionist": {"want": False}}},
    )
    assert response.status_code == 200
    assert response.json()["data"]["consultations"]["nutritionist"]["want"] is False

    row = (
        await test_db_session.execute(
            text(
                "SELECT want FROM consultation_bookings "
                "WHERE engagement_participant_id = 87021 AND expert_type = 'nutritionist'"
            )
        )
    ).first()
    assert row.want is False


@pytest.mark.asyncio
async def test_patch_participant_partial_update(async_client, test_db_session):
    await _seed_engagement_participant_for_patch(test_db_session)

    response = await async_client.patch(
        "/engagements/8702/participants/1031",
        headers=_auth_header(7031),
        json={"consultations": {"nutritionist": {"want": True}}},
    )
    assert response.status_code == 200
    assert response.json()["data"]["consultations"]["nutritionist"]["want"] is True

    row = (
        await test_db_session.execute(
            text(
                "SELECT expert_type, want FROM consultation_bookings "
                "WHERE engagement_participant_id = 87021 ORDER BY expert_type"
            )
        )
    ).all()
    assert len(row) == 2
    assert row[0].expert_type == "doctor"
    assert row[0].want is False
    assert row[1].expert_type == "nutritionist"
    assert row[1].want is True


@pytest.mark.asyncio
async def test_update_consultation_consent_for_participant(async_client, test_db_session):
    from datetime import time

    from modules.engagements.models import Engagement, EngagementParticipant
    from modules.experts.models import ConsultationBooking

    user_id = 1035
    engagement_id = 8705
    participant_id = 87051
    consultation_id = 870511

    test_db_session.add(User(user_id=user_id, age=30, phone="1035000000", status="active"))
    test_db_session.add(User(user_id=1036, age=30, phone="1036000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Consent Engagement",
            engagement_code="CONS8705",
            engagement_type="bio_ai_with_consultation",
            consultations={"doctor": True},
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=participant_id,
            engagement_id=engagement_id,
            user_id=user_id,
            engagement_date=date(2026, 2, 1),
            slot_start_time=time(10, 0),
            consultation_booking_ids=[consultation_id],
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        ConsultationBooking(
            consultation_id=consultation_id,
            engagement_participant_id=participant_id,
            expert_type="doctor",
            want=True,
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        f"/engagements/{engagement_id}/consultation/{consultation_id}/consent",
        headers=_auth_header(user_id),
        json={"bio_ai": True, "questionnaire": True},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["consultation_id"] == consultation_id
    assert data["consent"]["bio_ai"] is True
    assert data["consent"]["blood_report"] is False
    assert data["consent"]["questionnaire"] is True

    forbidden = await async_client.post(
        f"/engagements/{engagement_id}/consultation/{consultation_id}/consent",
        headers=_auth_header(1036),
        json={"bio_ai": True},
    )
    assert forbidden.status_code == 403


@pytest.mark.asyncio
async def test_get_engagement_consultations_for_user(async_client, test_db_session):
    from datetime import time

    from modules.engagements.models import Engagement, EngagementParticipant
    from modules.experts.models import ConsultationBooking

    user_id = 1037
    engagement_id = 8706
    participant_id = 87061
    consultation_id = 870611

    test_db_session.add(User(user_id=user_id, age=30, phone="1037000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="User Consultations Engagement",
            engagement_code="UCON8706",
            engagement_type="consultation",
            consultations={"doctor": True, "nutritionist": True},
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=participant_id,
            engagement_id=engagement_id,
            user_id=user_id,
            engagement_date=date(2026, 2, 1),
            slot_start_time=time(10, 0),
            consultation_booking_ids=[consultation_id, consultation_id + 1],
        )
    )
    await test_db_session.flush()
    test_db_session.add_all(
        [
            ConsultationBooking(
                consultation_id=consultation_id,
                engagement_participant_id=participant_id,
                expert_type="doctor",
                want=True,
                consultation_date=date(2026, 2, 1),
                consultation_slot="10:00",
                consultation_summary="Doctor notes",
                attachments=["http://example.com/a.pdf"],
                done=True,
            ),
            ConsultationBooking(
                consultation_id=consultation_id + 1,
                engagement_participant_id=participant_id,
                expert_type="nutritionist",
                want=True,
                consultation_date=date(2026, 2, 2),
                consultation_slot="11:30",
                done=False,
            ),
        ]
    )
    await test_db_session.commit()

    response = await async_client.get(
        f"/engagements/{engagement_id}/consultation",
        headers=_auth_header(user_id),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["engagement_id"] == engagement_id
    assert data["user_id"] == user_id
    assert len(data["consultations"]) == 2
    by_type = {item["expert_type"]: item for item in data["consultations"]}
    assert by_type["doctor"]["consultation_summary"] == "Doctor notes"
    assert by_type["doctor"]["attachments"] == ["http://example.com/a.pdf"]
    assert by_type["doctor"]["date"] == "2026-02-01"
    assert by_type["doctor"]["slot"] == "10:00"
    assert by_type["nutritionist"]["consultation_summary"] is None
    assert by_type["nutritionist"]["done"] is False


@pytest.mark.asyncio
async def test_get_engagement_consultations_forbidden_for_non_participant(async_client, test_db_session):
    from modules.engagements.models import Engagement

    test_db_session.add(User(user_id=1038, age=30, phone="1038000000", status="active"))
    test_db_session.add(
        Engagement(
            engagement_id=8707,
            engagement_name="No Access Engagement",
            engagement_code="NOAC8707",
            engagement_type="consultation",
            consultations={"doctor": True},
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/engagements/8707/consultation",
        headers=_auth_header(1038),
    )
    assert response.status_code == 403
async def test_list_engagements_onboarding_assistant_403(async_client, test_db_session):
    await _seed_employee(
        test_db_session, user_id=7040, employee_id=40, role="onboarding_assistant"
    )
    response = await async_client.get("/engagements", headers=_auth_header(7040))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_update_engagement_onboarding_assistant_403(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7041, employee_id=41, role="admin")
    await _seed_employee(
        test_db_session, user_id=7042, employee_id=42, role="onboarding_assistant"
    )
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=8801,
            engagement_name="OA Blocked Update",
            organization_id=1,
            engagement_code="OABLOCK1",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
        )
    )
    await test_db_session.commit()

    payload = {
        "engagement_name": "Hacked",
        "engagement_code": "OABLOCK1",
        "organization_id": 1,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-01",
    }
    response = await async_client.put(
        "/engagements/8801",
        headers=_auth_header(7042),
        json=payload,
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_engagement_rejects_overlapping_questionnaire_reminders(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7050, employee_id=50)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization 1")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)
    await _seed_notification_service(test_db_session, service_key="service-a")
    await _seed_notification_service(test_db_session, service_key="service-b")

    payload = {
        "engagement_name": "Overlap Camp",
        "organization_id": 1,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-02",
        "questionnaire_reminder_1": "service-a,service-b",
        "questionnaire_reminder_2": "service-b",
    }

    response = await async_client.post("/engagements", headers=_auth_header(7050), json=payload)
    assert response.status_code == 400
    assert "service-b" in response.json()["message"]
