"""Tests for reports_ready participant list filter."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.engagements.models import Engagement, EngagementParticipant
from modules.reports.models import IndividualHealthReport
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token(
        {"sub": str(user_id)},
        timedelta(minutes=5),
        secret_key=settings.JWT_SECRET_KEY,
    )
    return {"Authorization": f"Bearer {token}"}


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


async def _ids(async_client, *, engagement_id: int, admin_id: int, reports_ready: str) -> list[int]:
    response = await async_client.get(
        f"/engagements/{engagement_id}/participants/ids?reports_ready={reports_ready}",
        headers=_auth_header(admin_id),
    )
    assert response.status_code == 200
    return sorted(response.json()["data"]["user_ids"])


@pytest.mark.asyncio
async def test_reports_ready_filter_modes(async_client, test_db_session):
    engagement_id = 88601
    admin_id = 88602
    await _seed_employee(test_db_session, user_id=admin_id, employee_id=admin_id)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, type_code="reports_ready_filter_v2")

    both_ready = 886101
    bio_only = 886102
    blood_only = 886103
    neither = 886104
    # Bio AI + blood URLs on separate IHR rows — still counts as both ready.
    split_rows = 886105

    for user_id in (both_ready, bio_only, blood_only, neither, split_rows):
        test_db_session.add(
            User(
                user_id=user_id,
                first_name=f"User{user_id}",
                age=30,
                phone=f"{user_id:010d}",
                status="active",
            )
        )
        await test_db_session.flush()
        test_db_session.add(
            EngagementParticipant(engagement_id=engagement_id, user_id=user_id)
        )

    test_db_session.add_all(
        [
            IndividualHealthReport(
                user_id=both_ready,
                engagement_id=engagement_id,
                report_url="https://bio-ai-reports.example/r/both",
                diagnostic_report_url="https://blood.example/both.pdf",
            ),
            IndividualHealthReport(
                user_id=bio_only,
                engagement_id=engagement_id,
                report_url="https://bio-ai-reports.example/r/bio",
                diagnostic_report_url=None,
            ),
            IndividualHealthReport(
                user_id=blood_only,
                engagement_id=engagement_id,
                report_url=None,
                diagnostic_report_url="https://blood.example/blood.pdf",
            ),
            IndividualHealthReport(
                user_id=neither,
                engagement_id=engagement_id,
                report_url="",
                diagnostic_report_url="   ",
            ),
            IndividualHealthReport(
                user_id=split_rows,
                engagement_id=engagement_id,
                report_url="https://bio-ai-reports.example/r/split",
                diagnostic_report_url=None,
            ),
            IndividualHealthReport(
                user_id=split_rows,
                engagement_id=engagement_id,
                report_url=None,
                diagnostic_report_url="https://blood.example/split.pdf",
            ),
        ]
    )
    await test_db_session.commit()

    assert await _ids(async_client, engagement_id=engagement_id, admin_id=admin_id, reports_ready="bio_ai") == [
        both_ready,
        bio_only,
        split_rows,
    ]
    assert await _ids(async_client, engagement_id=engagement_id, admin_id=admin_id, reports_ready="blood") == [
        both_ready,
        blood_only,
        split_rows,
    ]
    assert await _ids(async_client, engagement_id=engagement_id, admin_id=admin_id, reports_ready="both") == [
        both_ready,
        split_rows,
    ]
    assert await _ids(async_client, engagement_id=engagement_id, admin_id=admin_id, reports_ready="missing") == [
        bio_only,
        blood_only,
        neither,
    ]
    # Legacy aliases still work.
    assert await _ids(async_client, engagement_id=engagement_id, admin_id=admin_id, reports_ready="yes") == [
        both_ready,
        split_rows,
    ]
    assert await _ids(async_client, engagement_id=engagement_id, admin_id=admin_id, reports_ready="no") == [
        bio_only,
        blood_only,
        neither,
    ]

    listed = await async_client.get(
        f"/engagements/{engagement_id}/participants?reports_ready=both&page=1&limit=50",
        headers=_auth_header(admin_id),
    )
    assert listed.status_code == 200
    listed_body = listed.json()
    assert listed_body["meta"]["total"] == 2
    assert listed_body["meta"]["filters"]["reports_ready"] == "both"
    assert sorted(p["user_id"] for p in listed_body["data"]) == [both_ready, split_rows]
