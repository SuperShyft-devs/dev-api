"""Integration tests for experts routes (public list/detail; employee mutations)."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest

from modules.engagements.models import Engagement, EngagementParticipant
from modules.experts.models import ConsultationBooking, Expert, ExpertAvailabilityModel, ExpertReview
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
    consultation_modes: list[str] | None = None,
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
        consultation_modes=consultation_modes if consultation_modes is not None else ["online"],
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
        assert "name" in r
        assert "discount_percent" in r
        assert "availability" in r


@pytest.mark.asyncio
async def test_list_experts_public_includes_card_fields(async_client, test_db_session):
    await seed_partner(
        test_db_session,
        partner_id=78550,
        role="expert",
        name="Dr. Sarah Jenkins",
        phone="785500000001",
        email="expert78550@test.example",
    )
    expert = Expert(
        partner_id=78550,
        expert_type="doctor",
        specialization="General Physician",
        status="active",
        appointment_fee_paise=49900,
        original_fee_paise=89800,
        experience_years=12,
        session_duration_mins=20,
        consultation_modes=["online"],
    )
    test_db_session.add(expert)
    await test_db_session.flush()
    for day in range(1, 6):
        test_db_session.add(
            ExpertAvailabilityModel(
                expert_id=expert.expert_id,
                day_of_week=day,
                start_time=time(9, 0),
                end_time=time(17, 0),
                slot_duration=20,
                buffer_time=5,
            )
        )
    await test_db_session.commit()

    response = await async_client.get("/experts?expert_type=doctor&page=1&limit=100")
    assert response.status_code == 200
    row = next(r for r in response.json()["data"] if r["expert_id"] == expert.expert_id)
    assert row["name"] == "Dr. Sarah Jenkins"
    assert row["discount_percent"] == 44
    assert row["availability"]["day_label"] == "Mon-Fri"
    assert row["availability"]["days"] == [1, 2, 3, 4, 5]
    assert row["availability"]["start_time"] == "09:00"
    assert row["availability"]["end_time"] == "17:00"
    assert row["availability"]["hours_label"] == "9-5 PM"
    assert "display_name" not in row

    search_response = await async_client.get("/experts?search=Sarah+Jenkins&page=1&limit=100")
    assert search_response.status_code == 200
    search_rows = search_response.json()["data"]
    assert any(r["expert_id"] == expert.expert_id for r in search_rows)

    sort_name_response = await async_client.get("/experts?sort_by=name&sort_dir=asc&page=1&limit=100")
    assert sort_name_response.status_code == 200

    sort_rating_response = await async_client.get("/experts?sort_by=rating&sort_dir=desc&page=1&limit=100")
    assert sort_rating_response.status_code == 200

    detail_response = await async_client.get(f"/experts/{expert.expert_id}")
    assert detail_response.status_code == 200
    detail = detail_response.json()["data"]
    assert detail["name"] == "Dr. Sarah Jenkins"
    assert detail["discount_percent"] == 44
    assert detail["availability"]["day_label"] == "Mon-Fri"


@pytest.mark.asyncio
async def test_list_experts_card_fields_null_without_fees_or_availability(async_client, test_db_session):
    partner, expert = await _seed_expert(
        test_db_session,
        partner_id=78551,
        expert_type="doctor",
        specialization="MBBS",
    )

    response = await async_client.get(f"/experts?expert_type=doctor&page=1&limit=100")
    assert response.status_code == 200
    row = next(r for r in response.json()["data"] if r["expert_id"] == expert.expert_id)
    assert row["name"] == partner.name
    assert row["discount_percent"] is None
    assert row["availability"] is None


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
    assert "name" in data
    assert "discount_percent" in data
    assert "availability" in data
    assert "reviews" in data
    assert "reviews_total" in data
    assert data["reviews"] == []
    assert data["reviews_total"] == 0


@pytest.mark.asyncio
async def test_get_expert_detail_embeds_reviews(async_client, test_db_session):
    _, expert = await _seed_expert(
        test_db_session,
        partner_id=78560,
        expert_type="doctor",
        specialization="Cardiology",
    )
    test_db_session.add(User(user_id=78560, age=30, phone="785600000001", status="active"))
    test_db_session.add(User(user_id=78561, age=30, phone="785600000002", status="active"))
    await test_db_session.flush()
    for idx, user_id in enumerate((78560, 78561), start=1):
        test_db_session.add(
            ExpertReview(
                expert_id=expert.expert_id,
                user_id=user_id,
                rating=Decimal("4.5"),
                review_text=f"Review text {idx}",
            )
        )
    await test_db_session.commit()

    response = await async_client.get(f"/experts/{expert.expert_id}?reviews_limit=1")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["reviews_total"] == 2
    assert len(data["reviews"]) == 1
    assert data["reviews"][0]["review_text"] in ("Review text 1", "Review text 2")
    assert data["reviews"][0]["rating"] == 4.5


@pytest.mark.asyncio
async def test_get_expert_detail_reviews_limit_zero(async_client, test_db_session):
    _, expert = await _seed_expert(
        test_db_session,
        partner_id=78562,
        expert_type="doctor",
        specialization="General medicine",
    )
    test_db_session.add(User(user_id=78562, age=30, phone="785620000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        ExpertReview(
            expert_id=expert.expert_id,
            user_id=78562,
            rating=Decimal("5.0"),
            review_text="Only review",
        )
    )
    await test_db_session.commit()

    response = await async_client.get(f"/experts/{expert.expert_id}?reviews_limit=0")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["reviews"] == []
    assert data["reviews_total"] == 1


@pytest.mark.asyncio
async def test_list_experts_does_not_include_reviews(async_client, test_db_session):
    await _seed_expert(
        test_db_session,
        partner_id=78563,
        expert_type="doctor",
        specialization="Dermatology",
    )

    response = await async_client.get("/experts?page=1&limit=20")
    assert response.status_code == 200
    for row in response.json()["data"]:
        assert "reviews" not in row
        assert "reviews_total" not in row


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
async def test_list_experts_excludes_non_expert_partner_role(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=402, role="admin")
    partner = await seed_partner(
        test_db_session,
        partner_id=78599,
        role="organization_manager",
        name="Org Manager Expert Orphan",
        phone="785990000001",
        email="expert78599@test.example",
    )
    expert = Expert(
        partner_id=partner.partner_id,
        expert_type="doctor",
        specialization="MBBS",
        status="active",
    )
    test_db_session.add(expert)
    await test_db_session.commit()

    response = await async_client.get(
        "/experts",
        headers=employee_auth_header(402),
        params={"page": 1, "limit": 100, "consultation_mode": "all"},
    )
    assert response.status_code == 200
    ids = {row["expert_id"] for row in response.json()["data"]}
    assert expert.expert_id not in ids


@pytest.mark.asyncio
async def test_delete_expert_removes_row(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=403, role="admin")
    partner, expert = await _seed_expert(
        test_db_session,
        partner_id=78598,
        specialization="To delete",
    )
    response = await async_client.delete(
        f"/experts/{expert.expert_id}",
        headers=employee_auth_header(403),
    )
    assert response.status_code == 200
    row = await test_db_session.get(Expert, expert.expert_id)
    assert row is None
    assert partner.partner_id is not None


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


@pytest.mark.asyncio
async def test_list_experts_defaults_to_online_consultation_mode(async_client, test_db_session):
    _, online_expert = await _seed_expert(
        test_db_session,
        partner_id=78570,
        specialization="Online only",
        consultation_modes=["online"],
    )
    _, offline_expert = await _seed_expert(
        test_db_session,
        partner_id=78571,
        specialization="Offline only",
        consultation_modes=["offline"],
    )

    response = await async_client.get("/experts?page=1&limit=100")
    assert response.status_code == 200
    ids = {row["expert_id"] for row in response.json()["data"]}
    assert online_expert.expert_id in ids
    assert offline_expert.expert_id not in ids


@pytest.mark.asyncio
async def test_list_experts_consultation_mode_offline_and_all(async_client, test_db_session):
    _, online_expert = await _seed_expert(
        test_db_session,
        partner_id=78572,
        specialization="Online doc",
        consultation_modes=["online"],
    )
    _, offline_expert = await _seed_expert(
        test_db_session,
        partner_id=78573,
        specialization="Offline doc",
        consultation_modes=["offline"],
    )

    offline_response = await async_client.get(
        "/experts",
        params={"page": 1, "limit": 100, "consultation_mode": "offline"},
    )
    assert offline_response.status_code == 200
    offline_ids = {row["expert_id"] for row in offline_response.json()["data"]}
    assert offline_expert.expert_id in offline_ids
    assert online_expert.expert_id not in offline_ids

    all_response = await async_client.get(
        "/experts",
        params={"page": 1, "limit": 100, "consultation_mode": "all"},
    )
    assert all_response.status_code == 200
    all_ids = {row["expert_id"] for row in all_response.json()["data"]}
    assert online_expert.expert_id in all_ids
    assert offline_expert.expert_id in all_ids


@pytest.mark.asyncio
async def test_create_expert_rejects_legacy_consultation_modes(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=404, role="admin")
    await seed_partner(
        test_db_session,
        partner_id=78574,
        role="expert",
        phone="785740000000",
        email="expert78574@test.example",
    )
    response = await async_client.post(
        "/experts",
        headers=employee_auth_header(404),
        json={
            "partner_id": 78574,
            "expert_type": "doctor",
            "specialization": "General medicine",
            "consultation_modes": ["video"],
        },
    )
    assert response.status_code == 400
