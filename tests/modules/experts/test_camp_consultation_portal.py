"""Integration tests for expert portal camp consultation routes."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pytest

from modules.engagements.enums import ConsultationMode
from modules.engagements.models import Engagement, EngagementParticipant, OnboardingAssistantAssignment
from modules.experts.models import ConsultationBooking, Expert
from modules.organizations.models import Organization
from modules.users.models import User
from tests.helpers.auth import partner_auth_header, seed_partner


async def _seed_camp_setup(
    test_db_session,
    *,
    engagement_id: int,
    expert_partner_id: int,
    participant_user_id: int,
    participant_id: int,
    nutritionist_consultation_id: int,
    doctor_consultation_id: int,
    assign_expert_as_oa: bool = True,
    expert_type: str = "nutritionist",
) -> Expert:
    test_db_session.add(
        Organization(
            organization_id=8860,
            name="Camp Org",
            organization_type="corporate",
            status="active",
        )
    )
    test_db_session.add(
        User(
            user_id=participant_user_id,
            age=30,
            phone="886000000002",
            email="participant.camp@example.com",
            first_name="Camp",
            last_name="Patient",
            status="active",
        )
    )
    await test_db_session.flush()
    await seed_partner(
        test_db_session,
        partner_id=expert_partner_id,
        role="expert",
        name="Nutri Expert",
        phone="886000000001",
        email="expert.camp@example.com",
        commit=False,
    )
    expert = Expert(
        partner_id=expert_partner_id,
        expert_type=expert_type,
        specialization="Nutrition",
        status="active",
    )
    test_db_session.add(expert)
    await test_db_session.flush()

    past_slot = datetime.now() - timedelta(hours=2)
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Camp Consultation",
            engagement_code=f"CAMP{engagement_id}",
            engagement_type=1,
            organization_id=8860,
            camp_no=886001,
            consultations={"doctor": True, "nutritionist": True},
            consultation_mode=ConsultationMode.offline,
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date.today(),
            end_date=date.today() + timedelta(days=1),
            status="running",
        )
    )
    await test_db_session.flush()
    if assign_expert_as_oa:
        test_db_session.add(
            OnboardingAssistantAssignment(
                onboarding_assistant_id=886001,
                partner_id=expert_partner_id,
                engagement_id=engagement_id,
            )
        )
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=participant_id,
            engagement_id=engagement_id,
            user_id=participant_user_id,
            engagement_date=date.today(),
            slot_start_time=time(9, 0),
            consultation_booking_ids=[nutritionist_consultation_id, doctor_consultation_id],
        )
    )
    await test_db_session.flush()
    test_db_session.add_all(
        [
            ConsultationBooking(
                consultation_id=nutritionist_consultation_id,
                engagement_participant_id=participant_id,
                expert_type="nutritionist",
                want=True,
                consultation_date=past_slot.date(),
                consultation_slot=past_slot.strftime("%H:%M"),
                consultation_cabin="C1",
                done=False,
                consent={"bio_ai": True, "blood_report": False, "questionnaire": True},
            ),
            ConsultationBooking(
                consultation_id=doctor_consultation_id,
                engagement_participant_id=participant_id,
                expert_type="doctor",
                want=True,
                consultation_date=past_slot.date(),
                consultation_slot=past_slot.strftime("%H:%M"),
                consultation_cabin="C2",
                done=False,
            ),
        ]
    )
    await test_db_session.commit()
    return expert


@pytest.mark.asyncio
async def test_camp_consultation_engagements_only_for_assigned_offline(async_client, test_db_session):
    engagement_id = 88601
    expert_partner_id = 88601
    other_engagement_id = 88602

    await _seed_camp_setup(
        test_db_session,
        engagement_id=engagement_id,
        expert_partner_id=expert_partner_id,
        participant_user_id=88611,
        participant_id=88611,
        nutritionist_consultation_id=886101,
        doctor_consultation_id=886102,
        assign_expert_as_oa=True,
    )

    test_db_session.add(
        Engagement(
            engagement_id=other_engagement_id,
            engagement_name="Other Offline Camp",
            engagement_code="CAMP88602",
            engagement_type=1,
            organization_id=8860,
            camp_no=886002,
            consultations={"nutritionist": True},
            consultation_mode=ConsultationMode.offline,
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date.today(),
            end_date=date.today() + timedelta(days=1),
            status="running",
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/experts/portal/camp-consultations/engagements",
        headers=partner_auth_header(expert_partner_id),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    engagement_ids = {item["engagement_id"] for item in data}
    assert engagement_id in engagement_ids
    assert other_engagement_id not in engagement_ids
    assigned = next(item for item in data if item["engagement_id"] == engagement_id)
    assert assigned["consultation_pending_count"] == 1


@pytest.mark.asyncio
async def test_camp_consultation_participants_filter_by_expert_type_and_mask_pii(
    async_client, test_db_session
):
    engagement_id = 88603
    expert_partner_id = 88603

    await _seed_camp_setup(
        test_db_session,
        engagement_id=engagement_id,
        expert_partner_id=expert_partner_id,
        participant_user_id=88613,
        participant_id=88613,
        nutritionist_consultation_id=886301,
        doctor_consultation_id=886302,
    )

    response = await async_client.get(
        f"/experts/portal/camp-consultations/engagements/{engagement_id}/participants",
        headers=partner_auth_header(expert_partner_id),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) == 1
    item = data[0]
    assert item["expert_type"] == "nutritionist"
    assert item["consultation_id"] == 886301
    assert item["phone"] == "********0002"
    assert item["email"].endswith("@example.com")
    assert item["email"].startswith("*")
    assert item["cabin"] == "C1"


@pytest.mark.asyncio
async def test_camp_consultation_manage_without_expert_id(async_client, test_db_session):
    engagement_id = 88604
    expert_partner_id = 88604
    consultation_id = 886401

    await _seed_camp_setup(
        test_db_session,
        engagement_id=engagement_id,
        expert_partner_id=expert_partner_id,
        participant_user_id=88614,
        participant_id=88614,
        nutritionist_consultation_id=consultation_id,
        doctor_consultation_id=886402,
    )

    expert_headers = partner_auth_header(expert_partner_id)

    detail = await async_client.get(
        f"/experts/portal/consultations/{consultation_id}",
        headers=expert_headers,
    )
    assert detail.status_code == 200
    body = detail.json()["data"]
    assert body["consultation_id"] == consultation_id
    assert body["cabin"] == "C1"
    assert body["phone"] == "********0002"
    assert body["email"].startswith("*")
    assert body["within_engagement_window"] is True

    patch = await async_client.patch(
        f"/experts/portal/consultations/{consultation_id}",
        headers=expert_headers,
        json={"consultation_summary": "Camp consult complete"},
    )
    assert patch.status_code == 200
    assert patch.json()["data"]["consultation_summary"] == "Camp consult complete"

    done = await async_client.post(
        f"/experts/portal/consultations/{consultation_id}/done",
        headers=expert_headers,
    )
    assert done.status_code == 200
    assert done.json()["data"]["done"] is True


@pytest.mark.asyncio
async def test_camp_consultation_forbidden_without_oa_assignment(async_client, test_db_session):
    engagement_id = 88605
    expert_partner_id = 88605
    consultation_id = 886501

    await _seed_camp_setup(
        test_db_session,
        engagement_id=engagement_id,
        expert_partner_id=expert_partner_id,
        participant_user_id=88615,
        participant_id=88615,
        nutritionist_consultation_id=consultation_id,
        doctor_consultation_id=886502,
        assign_expert_as_oa=False,
    )

    expert_headers = partner_auth_header(expert_partner_id)

    response = await async_client.get(
        f"/experts/portal/camp-consultations/engagements/{engagement_id}/participants",
        headers=expert_headers,
    )
    assert response.status_code == 403

    manage = await async_client.get(
        f"/experts/portal/consultations/{consultation_id}",
        headers=expert_headers,
    )
    assert manage.status_code == 403


@pytest.mark.asyncio
async def test_camp_consultation_manage_rejects_wrong_expert_type(async_client, test_db_session):
    engagement_id = 88606
    expert_partner_id = 88606
    doctor_consultation_id = 886602

    await _seed_camp_setup(
        test_db_session,
        engagement_id=engagement_id,
        expert_partner_id=expert_partner_id,
        participant_user_id=88616,
        participant_id=88616,
        nutritionist_consultation_id=886601,
        doctor_consultation_id=doctor_consultation_id,
    )

    response = await async_client.get(
        f"/experts/portal/consultations/{doctor_consultation_id}",
        headers=partner_auth_header(expert_partner_id),
    )
    assert response.status_code == 403
