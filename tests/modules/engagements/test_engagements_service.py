from __future__ import annotations

from datetime import date, time

import pytest
from sqlalchemy import text

from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import EngagementsService


@pytest.mark.asyncio
async def test_enroll_user_in_engagement_does_not_increment_participant_count_by_default(test_db_session):
    # Create required assessment and diagnostic packages
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PKG1', 'Test Package', 'active')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active')"
        )
    )
    await test_db_session.commit()
    
    # Create engagement directly without organization_id (B2C engagement)
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count, organization_id) "
            "VALUES (9001, 'Camp', 'ENG9001', 'healthcamp', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'active', 0, NULL)"
        )
    )
    await test_db_session.commit()

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, phone, status) VALUES (1001, '9999999999', 'active')"
        )
    )
    await test_db_session.commit()

    service = EngagementsService(EngagementsRepository())
    engagement = await service.get_by_code(test_db_session, "ENG9001")
    assert engagement is not None

    slot = await service.enroll_user_in_engagement(
        test_db_session,
        engagement=engagement,
        user_id=1001,
        engagement_date=date(2026, 2, 1),
        slot_start_time=time(10, 0),
    )

    row = (
        await test_db_session.execute(text("SELECT participant_count FROM engagements WHERE engagement_id = 9001"))
    ).first()
    assert row.participant_count == 0

    assert slot.engagement_id == 9001
    assert slot.user_id == 1001
