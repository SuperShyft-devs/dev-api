from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import EngagementsService


async def _seed_engagement_dependencies(test_db_session) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PKG1', 'Test Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_record_id, require_participant_detail) "
            "VALUES ('booking-alert-whatsapp', 'Booking Alert', 'whatsapp', 'booking-alert', true, false, false) "
            "ON CONFLICT (service_key) DO NOTHING"
        )
    )
    await test_db_session.commit()


async def _insert_engagement(
    test_db_session,
    *,
    engagement_id: int,
    engagement_code: str,
    end_date: str,
    status: str,
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO engagements "
            "(engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, "
            "diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count, "
            "organization_id, notification_service_key) "
            f"VALUES ({engagement_id}, 'Camp {engagement_id}', '{engagement_code}', 'bio_ai', 1, 1, 'BLR', 20, "
            f"'2026-01-01', '{end_date}', '{status}', 0, NULL, 'booking-alert-whatsapp')"
        )
    )


def _service() -> EngagementsService:
    return EngagementsService(
        EngagementsRepository(),
        audit_service=AuditService(AuditRepository()),
    )


@pytest.mark.asyncio
async def test_complete_expired_engagements_marks_past_running_as_completed(test_db_session):
    await _seed_engagement_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9101,
        engagement_code="ENG9101",
        end_date="2026-05-28",
        status="running",
    )
    await _insert_engagement(
        test_db_session,
        engagement_id=9102,
        engagement_code="ENG9102",
        end_date="2026-05-30",
        status="running",
    )
    await _insert_engagement(
        test_db_session,
        engagement_id=9103,
        engagement_code="ENG9103",
        end_date="2026-06-01",
        status="running",
    )
    await _insert_engagement(
        test_db_session,
        engagement_id=9104,
        engagement_code="ENG9104",
        end_date="2026-05-20",
        status="completed",
    )
    await test_db_session.commit()

    service = _service()
    result = await service.complete_expired_engagements(test_db_session, as_of=date(2026, 5, 30))
    await test_db_session.commit()

    assert result["as_of"] == "2026-05-30"
    assert result["dry_run"] is False
    assert result["completed_count"] >= 1

    statuses = {
        row.engagement_id: row.status
        for row in (
            await test_db_session.execute(
                text("SELECT engagement_id, status FROM engagements WHERE engagement_id IN (9101, 9102, 9103, 9104)")
            )
        ).all()
    }
    assert statuses[9101] == "completed"
    assert statuses[9102] == "running"
    assert statuses[9103] == "running"
    assert statuses[9104] == "completed"


@pytest.mark.asyncio
async def test_complete_expired_engagements_is_idempotent(test_db_session):
    await _seed_engagement_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9201,
        engagement_code="ENG9201",
        end_date="2026-05-28",
        status="running",
    )
    await test_db_session.commit()

    service = _service()
    first = await service.complete_expired_engagements(test_db_session, as_of=date(2026, 5, 30))
    await test_db_session.commit()
    second = await service.complete_expired_engagements(test_db_session, as_of=date(2026, 5, 30))
    await test_db_session.commit()

    assert first["completed_count"] == 1
    assert second["completed_count"] == 0


@pytest.mark.asyncio
async def test_complete_expired_engagements_dry_run_does_not_update(test_db_session):
    await _seed_engagement_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9301,
        engagement_code="ENG9301",
        end_date="2026-05-28",
        status="running",
    )
    await test_db_session.commit()

    service = _service()
    result = await service.complete_expired_engagements(
        test_db_session,
        as_of=date(2026, 5, 30),
        dry_run=True,
    )
    await test_db_session.commit()

    assert result["completed_count"] == 1
    assert result["dry_run"] is True

    status = (
        await test_db_session.execute(text("SELECT status FROM engagements WHERE engagement_id = 9301"))
    ).scalar_one()
    assert status == "running"


@pytest.mark.asyncio
async def test_complete_expired_engagements_writes_audit_log_when_updates_occur(test_db_session):
    await _seed_engagement_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9401,
        engagement_code="ENG9401",
        end_date="2026-05-28",
        status="running",
    )
    await test_db_session.commit()

    service = _service()
    await service.complete_expired_engagements(test_db_session, as_of=date(2026, 5, 30))
    await test_db_session.commit()

    audit_action = (
        await test_db_session.execute(
            text(
                "SELECT action FROM data_audit_logs "
                "WHERE action = 'SYSTEM_COMPLETE_EXPIRED_ENGAGEMENTS' "
                "ORDER BY audit_id DESC LIMIT 1"
            )
        )
    ).scalar_one_or_none()
    assert audit_action == "SYSTEM_COMPLETE_EXPIRED_ENGAGEMENTS"
