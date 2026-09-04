"""Tests for live Healthians blood_test_complete participant enrichment."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text

from modules.employee.models import Employee
from modules.engagements.models import Engagement, EngagementParticipant
from modules.engagements.service import EngagementsService
from modules.users.models import User


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id:010d}", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _engagement_type_id(test_db_session, code: str) -> int:
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES (:code, :dn, true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        ),
        {"code": code, "dn": code},
    )
    await test_db_session.commit()
    row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = :code"),
            {"code": code},
        )
    ).one()
    return int(row[0])


async def _seed_engagement(test_db_session, *, engagement_id: int, type_code: str):
    type_id = await _engagement_type_id(test_db_session, type_code)
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (:pid, :pcode, :dname, 'active') ON CONFLICT (package_id) DO NOTHING"
        ),
        {"pid": engagement_id, "pcode": f"PKG{engagement_id}", "dname": f"Package {engagement_id}"},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, bookings_count) "
            "VALUES (:did, :ref, :pname, 'test_provider', 'active', 0) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        ),
        {"did": engagement_id, "ref": f"REF{engagement_id}", "pname": f"Diag {engagement_id}"},
    )
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_code=f"ENG{engagement_id}",
            engagement_type=type_id,
            assessment_package_id=engagement_id,
            diagnostic_package_id=engagement_id,
            slot_duration=20,
            start_date=date(2026, 9, 1),
            end_date=date(2026, 9, 30),
            status="running",
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_participant_list_skips_live_healthians_enrichment(test_db_session):
    engagement_id = 988150
    user_id = 988150
    await _seed_engagement(test_db_session, engagement_id=engagement_id, type_code="blood_complete_live")
    test_db_session.add(
        User(
            user_id=user_id,
            age=25,
            first_name="Neelima",
            last_name="M",
            phone="7902555237",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=988150,
            engagement_id=engagement_id,
            user_id=user_id,
            booking_id="20014806389",
        )
    )
    await test_db_session.commit()

    service = EngagementsService(repository=__import__(
        "modules.engagements.repository", fromlist=["EngagementsRepository"]
    ).EngagementsRepository())
    participants = await service._repository.list_participants_by_engagement_id(
        test_db_session,
        engagement_id=engagement_id,
        page=1,
        limit=20,
    )
    result = await service._participant_rows_to_dicts(test_db_session, participants)

    assert len(result) == 1
    assert result[0]["blood_test_complete"] is None


@pytest.mark.asyncio
@patch("modules.engagements.blood_test_complete_resolver.healthians_client.get_booking_report", new_callable=AsyncMock)
@patch("modules.engagements.blood_test_complete_resolver.healthians_client.get_access_token", new_callable=AsyncMock)
async def test_participant_stats_calls_healthians_live(
    mock_get_access_token,
    mock_get_booking_report,
    test_db_session,
):
    mock_get_access_token.return_value = "token"
    mock_get_booking_report.return_value = {
        "status": True,
        "data": [{"cust_name": "NEELIMA M", "full_report": 1}],
    }

    engagement_id = 988151
    user_id = 988151
    await _seed_employee(test_db_session, user_id=988152, employee_id=988152)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, type_code="blood_complete_stats")
    test_db_session.add(
        User(
            user_id=user_id,
            age=25,
            first_name="Neelima",
            last_name="M",
            phone="7902555238",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=988151,
            engagement_id=engagement_id,
            user_id=user_id,
            booking_id="20014806389",
        )
    )
    await test_db_session.commit()

    service = EngagementsService(repository=__import__(
        "modules.engagements.repository", fromlist=["EngagementsRepository"]
    ).EngagementsRepository())
    from modules.employee.service import EmployeeContext

    employee = EmployeeContext(employee_id=988152, user_id=988152, role="admin")
    stats = await service.participant_stats_for_engagement_id(
        test_db_session,
        employee=employee,
        engagement_id=engagement_id,
        filters=None,
    )

    assert stats["filtered_total"] == 1
    assert stats["filtered_blood_test_complete"] == 1
    mock_get_booking_report.assert_awaited_once()
