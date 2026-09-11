"""Integration tests for experts routes (public list/detail; employee mutations)."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pytest

from modules.engagements.models import Engagement, EngagementParticipant
from modules.experts.models import ConsultationBooking, Expert
from modules.partners.models import Partner
from modules.users.models import User
from tests.helpers.auth import (
    employee_auth_header,
    partner_auth_header,
    seed_employee,
    seed_partner,
    user_auth_header,
)


async def _seed_expert(
    test_db_session,
    *,
    partner_id: int,
    expert_type: str = "doctor",
    specialization: str = "General medicine",
) -> tuple[Partner, Expert]:
    partner = await seed_partner(
        test_db_session,
        partner_id=partner_id,
        role="expert",
        phone=f"785{partner_id:07d}"[:15],
        email=f"expert{partner_id}@test.example",
        commit=False,
    )
    expert = Expert(
        partner_id=partner_id,
        expert_type=expert_type,
        specialization=specialization,
        status="active",
    )
    test_db_session.add(expert)
    await test_db_session.commit()
    return partner, expert


@pytest.mark.asyncio
async def test_list_experts_public_returns_specialization_key(async_client, test_db_session):
    """Public GET uses API field name specialization (not display_name)."""
    await _seed_expert(
        test_db_session,
        partner_id=78501,
        expert_type="doctor",
        specialization="Cardiology",
    )

    response = await async_client.get("/experts?page=1&limit=20")
    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    rows = [r for r in body["data"] if r.get("expert_id")]
    assert any(r.get("specialization") == "Cardiology" for r in rows)
    for r in rows:
        assert "specialization" in r
        assert "display_name" not in r


@pytest.mark.asyncio
async def test_get_expert_detail_public_includes_specialization(async_client, test_db_session):
    _, expert = await _seed_expert(
        test_db_session,
        partner_id=78502,
        expert_type="nutritionist",
        specialization="Sports nutrition",
    )
    expert_id = expert.expert_id

    response = await async_client.get(f"/experts/{expert_id}")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["specialization"] == "Sports nutrition"
    assert "display_name" not in data


@pytest.mark.asyncio
async def test_create_expert_requires_employee(async_client, test_db_session):
    await seed_partner(
        test_db_session,
        partner_id=78503,
        role="expert",
        phone="785030000000",
        email="expert78503@test.example",
    )
    test_db_session.add(User(user_id=78503, age=30, phone="785031000000", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/experts",
        headers=user_auth_header(78503),
        json={
            "partner_id": 78503,
            "expert_type": "doctor",
            "specialization": "General medicine",
        },
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_expert_persists_specialization(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=401, role="admin")
    await seed_partner(
        test_db_session,
        partner_id=78505,
        role="expert",
        phone="785050000000",
        email="expert78505@test.example",
    )

    payload = {
        "partner_id": 78505,
        "expert_type": "doctor",
        "specialization": "Pediatrics",
    }
    response = await async_client.post(
        "/experts",
        headers=employee_auth_header(401),
        json=payload,
    )
    assert response.status_code == 201
    expert_id = response.json()["data"]["expert_id"]

    row = await test_db_session.get(Expert, expert_id)
    assert row is not None
    assert row.specialization == "Pediatrics"
    assert row.expert_type == "doctor"
    assert row.partner_id == 78505

    partner = await test_db_session.get(Partner, 78505)
    assert partner is not None
    assert partner.role == "expert"
    assert partner.status == "active"


@pytest.mark.asyncio
async def test_experts_portal_me_returns_own_expert(async_client, test_db_session):
    partner, expert = await _seed_expert(
        test_db_session,
        partner_id=78520,
        expert_type="doctor",
        specialization="Dermatology",
    )

    response = await async_client.get(
        "/experts/portal/me",
        headers=partner_auth_header(partner.partner_id),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["expert_id"] == expert.expert_id
    assert data["specialization"] == "Dermatology"
    assert "expertise_tags" in data


@pytest.mark.asyncio
async def test_experts_portal_me_forbidden_for_onboarding_assistant(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=421, role="onboarding_assistant")

    response = await async_client.get(
        "/experts/portal/me",
        headers=employee_auth_header(421),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_update_expert_specialization(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=402, role="admin")
    await seed_partner(
        test_db_session,
        partner_id=78507,
        role="expert",
        phone="785070000000",
        email="expert78507@test.example",
    )
    expert = Expert(
        partner_id=78507,
        expert_type="doctor",
        specialization="Old spec",
        status="active",
    )
    test_db_session.add(expert)
    await test_db_session.flush()
    await test_db_session.commit()
    expert_id = expert.expert_id

    response = await async_client.put(
        f"/experts/{expert_id}",
        headers=employee_auth_header(402),
        json={
            "partner_id": 78507,
            "expert_type": "nutritionist",
            "specialization": "Updated specialization",
        },
    )
    assert response.status_code == 200
    await test_db_session.refresh(expert)
    assert expert.specialization == "Updated specialization"
    assert expert.expert_type == "nutritionist"


@pytest.mark.asyncio
async def test_mark_consultation_done_requires_meet_link(async_client, test_db_session):
    participant_user_id = 78531
    engagement_id = 7853
    participant_id = 78531
    consultation_id = 785301

    partner, expert = await _seed_expert(
        test_db_session,
        partner_id=78530,
        expert_type="doctor",
        specialization="General medicine",
    )
    participant_user = User(
        user_id=participant_user_id,
        age=30,
        phone="785310000000",
        status="active",
    )
    test_db_session.add(participant_user)
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Done Consult Engagement",
            engagement_code="DONE7853",
            engagement_type=1,
            consultations={"doctor": True},
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date.today(),
            end_date=date.today() + timedelta(days=7),
            status="running",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=participant_id,
            engagement_id=engagement_id,
            user_id=participant_user_id,
            engagement_date=date.today() + timedelta(days=1),
            slot_start_time=time(10, 0),
            consultation_booking_ids=[consultation_id],
        )
    )
    await test_db_session.flush()
    past_slot = datetime.now() - timedelta(hours=1)
    test_db_session.add(
        ConsultationBooking(
            consultation_id=consultation_id,
            engagement_participant_id=participant_id,
            expert_type="doctor",
            expert_id=expert.expert_id,
            want=True,
            consultation_date=past_slot.date(),
            consultation_slot=past_slot.strftime("%H:%M"),
            done=False,
        )
    )
    await test_db_session.commit()

    expert_headers = partner_auth_header(partner.partner_id)

    missing_link = await async_client.post(
        "/experts/portal/consultations/done",
        headers=expert_headers,
        json={
            "user_id": participant_user_id,
            "engagement_id": engagement_id,
            "expert_type": "doctor",
        },
    )
    assert missing_link.status_code in (400, 422)

    response = await async_client.post(
        "/experts/portal/consultations/done",
        headers=expert_headers,
        json={
            "user_id": participant_user_id,
            "engagement_id": engagement_id,
            "expert_type": "doctor",
            "meet_link": "https://meet.google.com/abc-defg-hij",
        },
    )
    assert response.status_code == 200
    assert response.json()["data"]["done"] is True

    booking = await test_db_session.get(ConsultationBooking, consultation_id)
    assert booking is not None
    assert booking.done is True
    assert booking.meet_link == "https://meet.google.com/abc-defg-hij"


@pytest.mark.asyncio
async def test_consultation_manage_done_outside_engagement_window_rejected(async_client, test_db_session):
    participant_user_id = 78541
    engagement_id = 7854
    participant_id = 78541
    consultation_id = 785401

    partner, expert = await _seed_expert(
        test_db_session,
        partner_id=78540,
        expert_type="doctor",
        specialization="General medicine",
    )
    test_db_session.add(
        User(user_id=participant_user_id, age=30, phone="785410000000", status="active")
    )
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Future Consult",
            engagement_code="FUT7854",
            engagement_type=1,
            consultations={"doctor": True},
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date.today() + timedelta(days=1),
            end_date=date.today() + timedelta(days=7),
            status="scheduled",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=participant_id,
            engagement_id=engagement_id,
            user_id=participant_user_id,
            engagement_date=date.today() + timedelta(days=1),
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
            expert_id=expert.expert_id,
            want=True,
            consultation_date=date.today() + timedelta(days=1),
            consultation_slot="10:00",
            done=False,
            consent={"bio_ai": True, "blood_report": False, "questionnaire": False},
        )
    )
    await test_db_session.commit()

    expert_headers = partner_auth_header(partner.partner_id)

    detail = await async_client.get(
        f"/experts/portal/consultations/{consultation_id}",
        headers=expert_headers,
    )
    assert detail.status_code == 200
    assert detail.json()["data"]["within_engagement_window"] is False

    before = await async_client.post(
        f"/experts/portal/consultations/{consultation_id}/done",
        headers=expert_headers,
    )
    assert before.status_code == 400
    assert "before the engagement start date" in before.json()["message"]


@pytest.mark.asyncio
async def test_consultation_manage_done_after_engagement_end_rejected(async_client, test_db_session):
    participant_user_id = 78543
    engagement_id = 7856
    participant_id = 78543
    consultation_id = 785601

    partner, expert = await _seed_expert(
        test_db_session,
        partner_id=78542,
        expert_type="doctor",
        specialization="General medicine",
    )
    test_db_session.add(
        User(user_id=participant_user_id, age=30, phone="785430000000", status="active")
    )
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Past Consult",
            engagement_code="PAST7856",
            engagement_type=1,
            consultations={"doctor": True},
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date.today() - timedelta(days=14),
            end_date=date.today() - timedelta(days=1),
            status="completed",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=participant_id,
            engagement_id=engagement_id,
            user_id=participant_user_id,
            engagement_date=date.today() - timedelta(days=7),
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
            expert_id=expert.expert_id,
            want=True,
            consultation_date=date.today() - timedelta(days=7),
            consultation_slot="10:00",
            done=False,
        )
    )
    await test_db_session.commit()

    expert_headers = partner_auth_header(partner.partner_id)

    detail = await async_client.get(
        f"/experts/portal/consultations/{consultation_id}",
        headers=expert_headers,
    )
    assert detail.status_code == 200
    assert detail.json()["data"]["within_engagement_window"] is False

    after = await async_client.post(
        f"/experts/portal/consultations/{consultation_id}/done",
        headers=expert_headers,
    )
    assert after.status_code == 400
    assert "after the engagement end date" in after.json()["message"]


@pytest.mark.asyncio
async def test_consultation_manage_detail_patch_and_done(async_client, test_db_session, tmp_path, monkeypatch):
    from core.config import settings
    from modules.reports.models import IndividualHealthReport

    media_root = tmp_path / "media"
    media_root.mkdir()
    monkeypatch.setattr(settings, "MEDIA_ROOT", str(media_root))
    monkeypatch.setattr(settings, "MEDIA_BASE_URL", "http://testserver/media")

    pdf_dir = media_root / "bio-ai"
    pdf_dir.mkdir()
    pdf_path = pdf_dir / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 sample content")

    participant_user_id = 78551
    engagement_id = 7855
    participant_id = 78551
    consultation_id = 785501

    partner, expert = await _seed_expert(
        test_db_session,
        partner_id=78550,
        expert_type="doctor",
        specialization="General medicine",
    )
    test_db_session.add(
        User(
            user_id=participant_user_id,
            age=30,
            phone="785510000000",
            first_name="Pat",
            last_name="Ient",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Manage Consult",
            engagement_code="MNG7855",
            engagement_type=1,
            consultations={"doctor": True},
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date.today(),
            end_date=date.today() + timedelta(days=7),
            status="running",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=participant_id,
            engagement_id=engagement_id,
            user_id=participant_user_id,
            engagement_date=date.today(),
            slot_start_time=time(9, 0),
            consultation_booking_ids=[consultation_id],
        )
    )
    await test_db_session.flush()
    past_slot = datetime.now() - timedelta(hours=2)
    test_db_session.add(
        ConsultationBooking(
            consultation_id=consultation_id,
            engagement_participant_id=participant_id,
            expert_type="doctor",
            expert_id=expert.expert_id,
            want=True,
            consultation_date=past_slot.date(),
            consultation_slot=past_slot.strftime("%H:%M"),
            done=False,
            consent={"bio_ai": True, "blood_report": False, "questionnaire": False},
        )
    )
    test_db_session.add(
        IndividualHealthReport(
            user_id=participant_user_id,
            engagement_id=engagement_id,
            report_url="http://testserver/media/bio-ai/sample.pdf",
        )
    )
    await test_db_session.commit()

    expert_headers = partner_auth_header(partner.partner_id)

    detail = await async_client.get(
        f"/experts/portal/consultations/{consultation_id}",
        headers=expert_headers,
    )
    assert detail.status_code == 200
    body = detail.json()["data"]
    assert body["user_id"] == participant_user_id
    assert body["shared_resources"]["bio_ai"]["consent"] is True
    assert body["shared_resources"]["bio_ai"]["available"] is True
    assert body["shared_resources"]["blood_report"]["consent"] is False
    assert body["within_engagement_window"] is True
    assert body["start_date"] == date.today().isoformat()

    patched = await async_client.patch(
        f"/experts/portal/consultations/{consultation_id}",
        headers=expert_headers,
        json={
            "consultation_summary": "Discussed labs",
            "attachments": ["http://testserver/media/consultation-attachments/a.pdf"],
            "meet_link": "https://meet.google.com/xyz",
        },
    )
    assert patched.status_code == 200
    assert patched.json()["data"]["consultation_summary"] == "Discussed labs"
    assert patched.json()["data"]["attachments"] == [
        "http://testserver/media/consultation-attachments/a.pdf"
    ]

    pdf_ok = await async_client.get(
        f"/experts/portal/consultations/{consultation_id}/bio-ai/pdf",
        headers=expert_headers,
    )
    assert pdf_ok.status_code == 200
    assert pdf_ok.headers["content-type"].startswith("application/pdf")
    assert pdf_ok.content.startswith(b"%PDF")

    pdf_forbidden = await async_client.get(
        f"/experts/portal/consultations/{consultation_id}/blood-report/pdf",
        headers=expert_headers,
    )
    assert pdf_forbidden.status_code == 403

    done = await async_client.post(
        f"/experts/portal/consultations/{consultation_id}/done",
        headers=expert_headers,
    )
    assert done.status_code == 200
    assert done.json()["data"]["done"] is True

    booking = await test_db_session.get(ConsultationBooking, consultation_id)
    assert booking is not None
    assert booking.done is True
    assert booking.consultation_summary == "Discussed labs"


@pytest.mark.asyncio
async def test_upload_consultation_attachments(async_client, test_db_session, tmp_path, monkeypatch):
    from core.config import settings

    media_root = tmp_path / "media"
    media_root.mkdir()
    monkeypatch.setattr(settings, "MEDIA_ROOT", str(media_root))
    monkeypatch.setattr(settings, "MEDIA_BASE_URL", "http://testserver/media")

    partner, _ = await _seed_expert(
        test_db_session,
        partner_id=78560,
        expert_type="doctor",
        specialization="General medicine",
    )

    files = [
        ("files", ("note.txt", b"hello notes", "text/plain")),
        ("files", ("scan.pdf", b"%PDF-1.4 hello", "application/pdf")),
    ]
    response = await async_client.post(
        "/uploads/consultation-attachments",
        headers=partner_auth_header(partner.partner_id),
        files=files,
    )
    assert response.status_code == 200
    urls = response.json()["data"]["urls"]
    assert len(urls) == 2
    assert all("/consultation-attachments/" in url for url in urls)
