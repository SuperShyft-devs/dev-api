"""Tests for onboarding assistant assignment functionality (phlebo partners)."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text

from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import EngagementsService
from tests.helpers.auth import seed_partner


async def _seed_packages_and_engagement(
    test_db_session,
    *,
    engagement_id: int,
    engagement_code: str,
) -> None:
    from tests.helpers.engagement_types import engagement_type_id

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PKG1', 'Package 1', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'REF1', 'Diag 1', 'Provider', 'active') "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    type_id = await engagement_type_id(test_db_session, "bio_ai")
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status) "
            "VALUES (:eid, 'Camp', :ecode, :etype, 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'running')"
        ),
        {"eid": engagement_id, "ecode": engagement_code, "etype": type_id},
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_add_onboarding_assistant_creates_assignment(test_db_session):
    """Adding a phlebo partner creates a new assignment record."""
    await seed_partner(
        test_db_session,
        partner_id=101,
        role="phlebo",
        name="Phlebo One",
        phone="5001000000",
        commit=False,
    )
    await _seed_packages_and_engagement(test_db_session, engagement_id=6001, engagement_code="ENG6001")

    repository = EngagementsRepository()
    assignment = await repository.add_onboarding_assistant(
        test_db_session,
        engagement_id=6001,
        partner_id=101,
    )

    assert assignment.engagement_id == 6001
    assert assignment.partner_id == 101
    assert assignment.onboarding_assistant_id is not None

    result = await test_db_session.execute(
        text(
            "SELECT partner_id, engagement_id FROM onboarding_assistant_assignment "
            "WHERE engagement_id = 6001"
        )
    )
    row = result.first()
    assert row is not None
    assert row.partner_id == 101


@pytest.mark.asyncio
async def test_add_multiple_onboarding_assistants_to_one_engagement(test_db_session):
    """Multiple phlebo partners can be assigned to one engagement."""
    await seed_partner(
        test_db_session,
        partner_id=102,
        role="phlebo",
        name="Phlebo Two",
        phone="5002000000",
        commit=False,
    )
    await seed_partner(
        test_db_session,
        partner_id=103,
        role="phlebo",
        name="Phlebo Three",
        phone="5003000000",
        commit=False,
    )
    await _seed_packages_and_engagement(test_db_session, engagement_id=6002, engagement_code="ENG6002")

    repository = EngagementsRepository()
    assignment1 = await repository.add_onboarding_assistant(test_db_session, engagement_id=6002, partner_id=102)
    assignment2 = await repository.add_onboarding_assistant(test_db_session, engagement_id=6002, partner_id=103)

    assert assignment1.partner_id == 102
    assert assignment2.partner_id == 103

    result = await test_db_session.execute(
        text("SELECT COUNT(*) as cnt FROM onboarding_assistant_assignment WHERE engagement_id = 6002")
    )
    assert result.scalar() == 2


@pytest.mark.asyncio
async def test_duplicate_assignment_prevented_by_unique_constraint(test_db_session):
    """The unique constraint prevents duplicate partner assignments."""
    await seed_partner(
        test_db_session,
        partner_id=104,
        role="phlebo",
        name="Phlebo Four",
        phone="5004000000",
        commit=False,
    )
    await _seed_packages_and_engagement(test_db_session, engagement_id=6003, engagement_code="ENG6003")

    repository = EngagementsRepository()
    await repository.add_onboarding_assistant(test_db_session, engagement_id=6003, partner_id=104)
    await test_db_session.commit()

    with pytest.raises(Exception):
        await repository.add_onboarding_assistant(test_db_session, engagement_id=6003, partner_id=104)
        await test_db_session.commit()


@pytest.mark.asyncio
async def test_list_onboarding_assistants_returns_all_assignments(test_db_session):
    """Listing returns all partner assignments for an engagement."""
    await seed_partner(
        test_db_session,
        partner_id=105,
        role="phlebo",
        name="Phlebo Five",
        phone="5005000000",
        commit=False,
    )
    await seed_partner(
        test_db_session,
        partner_id=106,
        role="phlebo",
        name="Phlebo Six",
        phone="5006000000",
        commit=False,
    )
    await _seed_packages_and_engagement(test_db_session, engagement_id=6004, engagement_code="ENG6004")

    repository = EngagementsRepository()
    await repository.add_onboarding_assistant(test_db_session, engagement_id=6004, partner_id=105)
    await repository.add_onboarding_assistant(test_db_session, engagement_id=6004, partner_id=106)
    await test_db_session.commit()

    assignments = await repository.list_onboarding_assistants(test_db_session, engagement_id=6004)

    assert len(assignments) == 2
    assert {a.partner_id for a in assignments} == {105, 106}


@pytest.mark.asyncio
async def test_remove_onboarding_assistant_deletes_assignment(test_db_session):
    """Removing a phlebo partner deletes the assignment."""
    await seed_partner(
        test_db_session,
        partner_id=107,
        role="phlebo",
        name="Phlebo Seven",
        phone="5007000000",
        commit=False,
    )
    await _seed_packages_and_engagement(test_db_session, engagement_id=6005, engagement_code="ENG6005")

    repository = EngagementsRepository()
    await repository.add_onboarding_assistant(test_db_session, engagement_id=6005, partner_id=107)
    await test_db_session.commit()

    assignments = await repository.list_onboarding_assistants(test_db_session, engagement_id=6005)
    assert len(assignments) == 1

    result = await repository.remove_onboarding_assistant(
        test_db_session, engagement_id=6005, partner_id=107
    )
    await test_db_session.commit()

    assert result is True
    assignments = await repository.list_onboarding_assistants(test_db_session, engagement_id=6005)
    assert len(assignments) == 0


@pytest.mark.asyncio
async def test_remove_nonexistent_assignment_returns_false(test_db_session):
    """Removing a non-existent assignment returns False."""
    await _seed_packages_and_engagement(test_db_session, engagement_id=6006, engagement_code="ENG6006")

    repository = EngagementsRepository()
    result = await repository.remove_onboarding_assistant(
        test_db_session, engagement_id=6006, partner_id=999
    )

    assert result is False


@pytest.mark.asyncio
async def test_get_onboarding_assistant_assignment_returns_specific_assignment(test_db_session):
    """Getting a specific partner assignment returns the row."""
    await seed_partner(
        test_db_session,
        partner_id=108,
        role="phlebo",
        name="Phlebo Eight",
        phone="5008000000",
        commit=False,
    )
    await _seed_packages_and_engagement(test_db_session, engagement_id=6007, engagement_code="ENG6007")

    repository = EngagementsRepository()
    await repository.add_onboarding_assistant(test_db_session, engagement_id=6007, partner_id=108)
    await test_db_session.commit()

    assignment = await repository.get_onboarding_assistant_assignment(
        test_db_session,
        engagement_id=6007,
        partner_id=108,
    )

    assert assignment is not None
    assert assignment.engagement_id == 6007
    assert assignment.partner_id == 108


@pytest.mark.asyncio
async def test_get_nonexistent_assignment_returns_none(test_db_session):
    """Getting a non-existent assignment returns None."""
    repository = EngagementsRepository()

    assignment = await repository.get_onboarding_assistant_assignment(
        test_db_session,
        engagement_id=9999,
        partner_id=9999,
    )

    assert assignment is None


@pytest.mark.asyncio
async def test_b2c_engagement_assigns_no_assistants_when_defaults_empty(test_db_session):
    """B2C engagements assign nothing when platform defaults are empty."""
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PKG1', 'Package 1', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'REF1', 'Diag 1', 'Provider', 'active') "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(text("DELETE FROM platform_settings"))
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "default_onboarding_assistant_employee_ids) "
            "VALUES (1, 1, 1, NULL)"
        )
    )
    await test_db_session.commit()

    service = EngagementsService(EngagementsRepository())
    engagement = await service.create_b2c_engagement(
        test_db_session,
        user_first_name="Test",
        engagement_date=date(2026, 2, 1),
        city="Mumbai",
        assessment_package_id=1,
        diagnostic_package_id=1,
    )
    await test_db_session.commit()

    repository = EngagementsRepository()
    assignments = await repository.list_onboarding_assistants(
        test_db_session, engagement_id=engagement.engagement_id
    )
    assert len(assignments) == 0


@pytest.mark.asyncio
async def test_b2c_engagement_assigns_default_assistants_from_platform_settings(test_db_session):
    """B2C engagements auto-assign phlebo partners configured in platform settings."""
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PKG1', 'Package 1', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'REF1', 'Diag 1', 'Provider', 'active') "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    await seed_partner(
        test_db_session,
        partner_id=110,
        role="phlebo",
        name="Default Phlebo A",
        phone="5010000000",
        commit=False,
    )
    await seed_partner(
        test_db_session,
        partner_id=111,
        role="phlebo",
        name="Default Phlebo B",
        phone="5011000000",
        commit=False,
    )
    await test_db_session.execute(text("DELETE FROM platform_settings"))
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "default_onboarding_assistant_employee_ids) "
            "VALUES (1, 1, 1, '110,111')"
        )
    )
    await test_db_session.commit()

    service = EngagementsService(EngagementsRepository())
    engagement = await service.create_b2c_engagement(
        test_db_session,
        user_first_name="Test",
        engagement_date=date(2026, 2, 1),
        city="Mumbai",
        assessment_package_id=1,
        diagnostic_package_id=1,
    )
    await test_db_session.commit()

    repository = EngagementsRepository()
    assignments = await repository.list_onboarding_assistants(
        test_db_session, engagement_id=engagement.engagement_id
    )
    assigned_ids = sorted(a.partner_id for a in assignments)
    assert assigned_ids == [110, 111]
