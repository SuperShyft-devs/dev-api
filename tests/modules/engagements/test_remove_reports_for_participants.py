"""Tests for remove-reports participant endpoint."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import select, text

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


async def _seed_participant(test_db_session, *, engagement_id: int, user_id: int):
    test_db_session.add(
        User(
            user_id=user_id,
            age=30,
            phone=f"{user_id:010d}",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_id=engagement_id,
            user_id=user_id,
        )
    )
    await test_db_session.flush()


@pytest.mark.asyncio
async def test_remove_reports_route_requires_auth(async_client):
    response = await async_client.post(
        "/engagements/1/participants/remove-reports",
        json={"user_ids": [1]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_remove_reports_route_rejects_unknown_participants(async_client, test_db_session):
    engagement_id = 1998301
    admin_id = 1998311
    await _seed_employee(test_db_session, user_id=admin_id, employee_id=admin_id)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, type_code="remove_reports_reject")

    response = await async_client.post(
        f"/engagements/{engagement_id}/participants/remove-reports",
        headers=_auth_header(admin_id),
        json={"user_ids": [9991998301]},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_remove_reports_deletes_all_ihr_rows_for_selected_users(async_client, test_db_session):
    engagement_id = 1998302
    other_engagement_id = 1998303
    admin_id = 1998312
    multi_report_user = 19983201
    single_report_user = 19983202
    no_report_user = 19983203
    untouched_user = 19983204

    await _seed_employee(test_db_session, user_id=admin_id, employee_id=admin_id)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, type_code="remove_reports_ok")
    await _seed_engagement(test_db_session, engagement_id=other_engagement_id, type_code="remove_reports_other")

    await test_db_session.execute(
        text(
            "DELETE FROM individual_health_report "
            "WHERE engagement_id IN (:engagement_id, :other_engagement_id)"
        ),
        {
            "engagement_id": engagement_id,
            "other_engagement_id": other_engagement_id,
        },
    )
    await test_db_session.commit()

    for user_id in (multi_report_user, single_report_user, no_report_user, untouched_user):
        await _seed_participant(test_db_session, engagement_id=engagement_id, user_id=user_id)
    test_db_session.add(
        EngagementParticipant(
            engagement_id=other_engagement_id,
            user_id=untouched_user,
        )
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            IndividualHealthReport(
                user_id=multi_report_user,
                engagement_id=engagement_id,
                report_url="https://bio-ai-reports.example/r/1",
            ),
            IndividualHealthReport(
                user_id=multi_report_user,
                engagement_id=engagement_id,
                diagnostic_report_url="https://blood.example/1.pdf",
            ),
            IndividualHealthReport(
                user_id=single_report_user,
                engagement_id=engagement_id,
                report_url="https://bio-ai-reports.example/r/2",
            ),
            IndividualHealthReport(
                user_id=untouched_user,
                engagement_id=other_engagement_id,
                report_url="https://bio-ai-reports.example/r/other",
            ),
        ]
    )
    await test_db_session.commit()

    response = await async_client.post(
        f"/engagements/{engagement_id}/participants/remove-reports",
        headers=_auth_header(admin_id),
        json={"user_ids": [multi_report_user, single_report_user, no_report_user]},
    )

    assert response.status_code == 200
    body = response.json()["data"]
    assert body == {
        "removed": 3,
        "users_processed": 3,
        "users_with_no_reports": 1,
    }

    remaining = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(
                IndividualHealthReport.engagement_id == engagement_id,
                IndividualHealthReport.user_id.in_(
                    [multi_report_user, single_report_user, no_report_user]
                ),
            )
        )
    ).scalars().all()
    assert remaining == []

    untouched = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(
                IndividualHealthReport.user_id == untouched_user,
                IndividualHealthReport.engagement_id == other_engagement_id,
            )
        )
    ).scalar_one()
    assert untouched.report_url == "https://bio-ai-reports.example/r/other"
