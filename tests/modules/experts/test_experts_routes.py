"""Integration tests for experts routes (public list/detail; employee mutations)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from sqlalchemy import select

from modules.employee.models import Employee, EmployeeRole
from modules.engagements.models import Engagement, EngagementParticipant
from modules.experts.models import ConsultationBooking, Expert
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    user = User(user_id=user_id, age=30, phone=f"{user_id}0000000000"[:15], status="active")
    test_db_session.add(user)
    await test_db_session.flush()
    employee = Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active")
    test_db_session.add(employee)
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_list_experts_public_returns_specialization_key(async_client, test_db_session):
    """Public GET uses API field name specialization (not display_name)."""
    expert_user = User(
        user_id=78501,
        age=35,
        phone="785010000000",
        first_name="Test",
        last_name="Expert",
        status="active",
    )
    test_db_session.add(expert_user)
    await test_db_session.flush()
    test_db_session.add(
        Expert(
            user_id=78501,
            expert_type="doctor",
            specialization="Cardiology",
            status="active",
        )
    )
    await test_db_session.commit()

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
    expert_user = User(user_id=78502, age=40, phone="785020000000", status="active")
    test_db_session.add(expert_user)
    await test_db_session.flush()
    expert = Expert(
        user_id=78502,
        expert_type="nutritionist",
        specialization="Sports nutrition",
        status="active",
    )
    test_db_session.add(expert)
    await test_db_session.flush()
    await test_db_session.commit()
    expert_id = expert.expert_id

    response = await async_client.get(f"/experts/{expert_id}")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["specialization"] == "Sports nutrition"
    assert "display_name" not in data


@pytest.mark.asyncio
async def test_create_expert_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=78503, age=30, phone="785030000000", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/experts",
        headers=_auth_header(78503),
        json={
            "user_id": 78503,
            "expert_type": "doctor",
            "specialization": "General medicine",
        },
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_expert_persists_specialization(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=78504, employee_id=401)
    test_db_session.add(User(user_id=78505, age=28, phone="785050000000", status="active"))
    await test_db_session.commit()

    payload = {
        "user_id": 78505,
        "expert_type": "doctor",
        "specialization": "Pediatrics",
    }
    response = await async_client.post("/experts", headers=_auth_header(78504), json=payload)
    assert response.status_code == 201
    expert_id = response.json()["data"]["expert_id"]

    row = await test_db_session.get(Expert, expert_id)
    assert row is not None
    assert row.specialization == "Pediatrics"
    assert row.expert_type == "doctor"
    assert row.user_id == 78505

    emp = (
        await test_db_session.execute(select(Employee).where(Employee.user_id == 78505))
    ).scalar_one_or_none()
    assert emp is not None
    assert emp.role == EmployeeRole.expert
    assert emp.status == "active"


@pytest.mark.asyncio
async def test_experts_portal_me_returns_own_expert(async_client, test_db_session):
    expert_user = User(user_id=78520, age=33, phone="785200000000", status="active")
    test_db_session.add(expert_user)
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=420, user_id=78520, role="expert", status="active")
    )
    expert = Expert(
        user_id=78520,
        expert_type="doctor",
        specialization="Dermatology",
        status="active",
    )
    test_db_session.add(expert)
    await test_db_session.flush()
    await test_db_session.commit()

    response = await async_client.get("/experts/portal/me", headers=_auth_header(78520))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["expert_id"] == expert.expert_id
    assert data["specialization"] == "Dermatology"
    assert "expertise_tags" in data


@pytest.mark.asyncio
async def test_experts_portal_me_forbidden_for_onboarding_assistant(async_client, test_db_session):
    user = User(user_id=78521, age=30, phone="785210000000", status="active")
    test_db_session.add(user)
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=421, user_id=78521, role="onboarding_assistant", status="active")
    )
    await test_db_session.commit()

    response = await async_client.get("/experts/portal/me", headers=_auth_header(78521))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_update_expert_specialization(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=78506, employee_id=402)
    test_db_session.add(User(user_id=78507, age=32, phone="785070000000", status="active"))
    await test_db_session.flush()
    expert = Expert(
        user_id=78507,
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
        headers=_auth_header(78506),
        json={
            "user_id": 78507,
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
    from datetime import date, datetime, time, timedelta

    expert_user_id = 78530
    participant_user_id = 78531
    engagement_id = 7853
    participant_id = 78531
    consultation_id = 785301

    expert_user = User(user_id=expert_user_id, age=35, phone="785300000000", status="active")
    participant_user = User(
        user_id=participant_user_id,
        age=30,
        phone="785310000000",
        status="active",
    )
    test_db_session.add_all([expert_user, participant_user])
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=430, user_id=expert_user_id, role="expert", status="active")
    )
    expert = Expert(
        user_id=expert_user_id,
        expert_type="doctor",
        specialization="General medicine",
        status="active",
    )
    test_db_session.add(expert)
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Done Consult Engagement",
            engagement_code="DONE7853",
            engagement_type="consultation",
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

    missing_link = await async_client.post(
        "/experts/portal/consultations/done",
        headers=_auth_header(expert_user_id),
        json={
            "user_id": participant_user_id,
            "engagement_id": engagement_id,
            "expert_type": "doctor",
        },
    )
    assert missing_link.status_code in (400, 422)

    response = await async_client.post(
        "/experts/portal/consultations/done",
        headers=_auth_header(expert_user_id),
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
async def test_consultation_manage_done_before_slot_rejected(async_client, test_db_session):
    from datetime import date, time, timedelta

    expert_user_id = 78540
    participant_user_id = 78541
    engagement_id = 7854
    participant_id = 78541
    consultation_id = 785401

    test_db_session.add_all(
        [
            User(user_id=expert_user_id, age=35, phone="785400000000", status="active"),
            User(user_id=participant_user_id, age=30, phone="785410000000", status="active"),
        ]
    )
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=440, user_id=expert_user_id, role="expert", status="active"))
    expert = Expert(
        user_id=expert_user_id,
        expert_type="doctor",
        specialization="General medicine",
        status="active",
    )
    test_db_session.add(expert)
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Future Consult",
            engagement_code="FUT7854",
            engagement_type="consultation",
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

    before = await async_client.post(
        f"/experts/portal/consultations/{consultation_id}/done",
        headers=_auth_header(expert_user_id),
    )
    assert before.status_code == 400
    assert "before the scheduled slot" in before.json()["message"]


@pytest.mark.asyncio
async def test_consultation_manage_detail_patch_and_done(async_client, test_db_session, tmp_path, monkeypatch):
    from datetime import date, datetime, time, timedelta

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

    expert_user_id = 78550
    participant_user_id = 78551
    engagement_id = 7855
    participant_id = 78551
    consultation_id = 785501

    test_db_session.add_all(
        [
            User(
                user_id=expert_user_id,
                age=35,
                phone="785500000000",
                first_name="Doc",
                last_name="Expert",
                status="active",
            ),
            User(
                user_id=participant_user_id,
                age=30,
                phone="785510000000",
                first_name="Pat",
                last_name="Ient",
                status="active",
            ),
        ]
    )
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=450, user_id=expert_user_id, role="expert", status="active"))
    expert = Expert(
        user_id=expert_user_id,
        expert_type="doctor",
        specialization="General medicine",
        status="active",
    )
    test_db_session.add(expert)
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Manage Consult",
            engagement_code="MNG7855",
            engagement_type="consultation",
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

    detail = await async_client.get(
        f"/experts/portal/consultations/{consultation_id}",
        headers=_auth_header(expert_user_id),
    )
    assert detail.status_code == 200
    body = detail.json()["data"]
    assert body["user_id"] == participant_user_id
    assert body["shared_resources"]["bio_ai"]["consent"] is True
    assert body["shared_resources"]["bio_ai"]["available"] is True
    assert body["shared_resources"]["blood_report"]["consent"] is False
    assert body["slot_reached"] is True

    patched = await async_client.patch(
        f"/experts/portal/consultations/{consultation_id}",
        headers=_auth_header(expert_user_id),
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
        headers=_auth_header(expert_user_id),
    )
    assert pdf_ok.status_code == 200
    assert pdf_ok.headers["content-type"].startswith("application/pdf")
    assert pdf_ok.content.startswith(b"%PDF")

    pdf_forbidden = await async_client.get(
        f"/experts/portal/consultations/{consultation_id}/blood-report/pdf",
        headers=_auth_header(expert_user_id),
    )
    assert pdf_forbidden.status_code == 403

    done = await async_client.post(
        f"/experts/portal/consultations/{consultation_id}/done",
        headers=_auth_header(expert_user_id),
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

    expert_user_id = 78560
    test_db_session.add(User(user_id=expert_user_id, age=35, phone="785600000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=460, user_id=expert_user_id, role="expert", status="active"))
    await test_db_session.commit()

    files = [
        ("files", ("note.txt", b"hello notes", "text/plain")),
        ("files", ("scan.pdf", b"%PDF-1.4 hello", "application/pdf")),
    ]
    response = await async_client.post(
        "/uploads/consultation-attachments",
        headers=_auth_header(expert_user_id),
        files=files,
    )
    assert response.status_code == 200
    urls = response.json()["data"]["urls"]
    assert len(urls) == 2
    assert all("/consultation-attachments/" in url for url in urls)
