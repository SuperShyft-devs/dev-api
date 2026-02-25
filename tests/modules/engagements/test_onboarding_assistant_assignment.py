"""Tests for onboarding assistant assignment functionality."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text

from modules.engagements.models import Engagement, OnboardingAssistantAssignment
from modules.engagements.repository import EngagementsRepository


@pytest.mark.asyncio
async def test_add_onboarding_assistant_creates_assignment(test_db_session):
    """Test that adding an onboarding assistant creates a new assignment record."""
    # Seed required packages
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PKG1', 'Package 1', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) VALUES (1, 'REF1', 'Diag 1', 'Provider', 'active')")
    )
    
    # Seed employee
    await test_db_session.execute(
        text("INSERT INTO users (user_id, phone, status) VALUES (5001, '5001000000', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO employee (employee_id, user_id, role, status) VALUES (101, 5001, 'onboarding_assistant', 'active')")
    )
    
    # Seed engagement
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (6001, 'Camp', 'ENG6001', 'healthcamp', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'active', 0)"
        )
    )
    await test_db_session.commit()

    # Add onboarding assistant
    repository = EngagementsRepository()
    assignment = await repository.add_onboarding_assistant(
        test_db_session,
        engagement_id=6001,
        employee_id=101,
    )

    assert assignment.engagement_id == 6001
    assert assignment.employee_id == 101
    assert assignment.onboarding_assistant_id is not None

    # Verify in database
    result = await test_db_session.execute(
        text("SELECT employee_id, engagement_id FROM onboarding_assistant_assignment WHERE engagement_id = 6001")
    )
    row = result.first()
    assert row is not None
    assert row.employee_id == 101


@pytest.mark.asyncio
async def test_add_multiple_onboarding_assistants_to_one_engagement(test_db_session):
    """Test that multiple employees can be assigned to one engagement."""
    # Seed required packages
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PKG1', 'Package 1', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) VALUES (1, 'REF1', 'Diag 1', 'Provider', 'active')")
    )
    
    # Seed employees
    await test_db_session.execute(
        text("INSERT INTO users (user_id, phone, status) VALUES (5002, '5002000000', 'active'), (5003, '5003000000', 'active')")
    )
    await test_db_session.execute(
        text(
            "INSERT INTO employee (employee_id, user_id, role, status) VALUES "
            "(102, 5002, 'onboarding_assistant', 'active'), (103, 5003, 'onboarding_assistant', 'active')"
        )
    )
    
    # Seed engagement
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (6002, 'Camp2', 'ENG6002', 'healthcamp', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'active', 0)"
        )
    )
    await test_db_session.commit()

    # Add multiple assistants
    repository = EngagementsRepository()
    assignment1 = await repository.add_onboarding_assistant(test_db_session, engagement_id=6002, employee_id=102)
    assignment2 = await repository.add_onboarding_assistant(test_db_session, engagement_id=6002, employee_id=103)

    assert assignment1.employee_id == 102
    assert assignment2.employee_id == 103

    # Verify both exist
    result = await test_db_session.execute(
        text("SELECT COUNT(*) as cnt FROM onboarding_assistant_assignment WHERE engagement_id = 6002")
    )
    count = result.scalar()
    assert count == 2


@pytest.mark.asyncio
async def test_duplicate_assignment_prevented_by_unique_constraint(test_db_session):
    """Test that the unique constraint prevents duplicate assignments."""
    # Seed required packages
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PKG1', 'Package 1', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) VALUES (1, 'REF1', 'Diag 1', 'Provider', 'active')")
    )
    
    # Seed employee and engagement
    await test_db_session.execute(
        text("INSERT INTO users (user_id, phone, status) VALUES (5004, '5004000000', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO employee (employee_id, user_id, role, status) VALUES (104, 5004, 'onboarding_assistant', 'active')")
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (6003, 'Camp3', 'ENG6003', 'healthcamp', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'active', 0)"
        )
    )
    await test_db_session.commit()

    repository = EngagementsRepository()
    
    # First assignment should succeed
    await repository.add_onboarding_assistant(test_db_session, engagement_id=6003, employee_id=104)
    await test_db_session.commit()

    # Second assignment with same engagement_id and employee_id should fail
    with pytest.raises(Exception):  # Database integrity error
        await repository.add_onboarding_assistant(test_db_session, engagement_id=6003, employee_id=104)
        await test_db_session.commit()


@pytest.mark.asyncio
async def test_list_onboarding_assistants_returns_all_assignments(test_db_session):
    """Test listing all onboarding assistants for an engagement."""
    # Seed required packages
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PKG1', 'Package 1', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) VALUES (1, 'REF1', 'Diag 1', 'Provider', 'active')")
    )
    
    # Seed employees
    await test_db_session.execute(
        text("INSERT INTO users (user_id, phone, status) VALUES (5005, '5005000000', 'active'), (5006, '5006000000', 'active')")
    )
    await test_db_session.execute(
        text(
            "INSERT INTO employee (employee_id, user_id, role, status) VALUES "
            "(105, 5005, 'onboarding_assistant', 'active'), (106, 5006, 'onboarding_assistant', 'active')"
        )
    )
    
    # Seed engagement
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (6004, 'Camp4', 'ENG6004', 'healthcamp', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'active', 0)"
        )
    )
    await test_db_session.commit()

    # Add assignments
    repository = EngagementsRepository()
    await repository.add_onboarding_assistant(test_db_session, engagement_id=6004, employee_id=105)
    await repository.add_onboarding_assistant(test_db_session, engagement_id=6004, employee_id=106)
    await test_db_session.commit()

    # List assignments
    assignments = await repository.list_onboarding_assistants(test_db_session, engagement_id=6004)
    
    assert len(assignments) == 2
    employee_ids = {a.employee_id for a in assignments}
    assert employee_ids == {105, 106}


@pytest.mark.asyncio
async def test_remove_onboarding_assistant_deletes_assignment(test_db_session):
    """Test removing an onboarding assistant from an engagement."""
    # Seed required packages
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PKG1', 'Package 1', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) VALUES (1, 'REF1', 'Diag 1', 'Provider', 'active')")
    )
    
    # Seed employee and engagement
    await test_db_session.execute(
        text("INSERT INTO users (user_id, phone, status) VALUES (5007, '5007000000', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO employee (employee_id, user_id, role, status) VALUES (107, 5007, 'onboarding_assistant', 'active')")
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (6005, 'Camp5', 'ENG6005', 'healthcamp', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'active', 0)"
        )
    )
    await test_db_session.commit()

    repository = EngagementsRepository()
    
    # Add assignment
    await repository.add_onboarding_assistant(test_db_session, engagement_id=6005, employee_id=107)
    await test_db_session.commit()

    # Verify it exists
    assignments = await repository.list_onboarding_assistants(test_db_session, engagement_id=6005)
    assert len(assignments) == 1

    # Remove assignment
    result = await repository.remove_onboarding_assistant(test_db_session, engagement_id=6005, employee_id=107)
    await test_db_session.commit()

    assert result is True

    # Verify it's gone
    assignments = await repository.list_onboarding_assistants(test_db_session, engagement_id=6005)
    assert len(assignments) == 0


@pytest.mark.asyncio
async def test_remove_nonexistent_assignment_returns_false(test_db_session):
    """Test that removing a non-existent assignment returns False."""
    # Seed required packages
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PKG1', 'Package 1', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) VALUES (1, 'REF1', 'Diag 1', 'Provider', 'active')")
    )
    
    # Seed engagement
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (6006, 'Camp6', 'ENG6006', 'healthcamp', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'active', 0)"
        )
    )
    await test_db_session.commit()

    repository = EngagementsRepository()
    
    # Try to remove non-existent assignment
    result = await repository.remove_onboarding_assistant(test_db_session, engagement_id=6006, employee_id=999)
    
    assert result is False


@pytest.mark.asyncio
async def test_get_onboarding_assistant_assignment_returns_specific_assignment(test_db_session):
    """Test getting a specific assignment."""
    # Seed required packages
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PKG1', 'Package 1', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) VALUES (1, 'REF1', 'Diag 1', 'Provider', 'active')")
    )
    
    # Seed employee and engagement
    await test_db_session.execute(
        text("INSERT INTO users (user_id, phone, status) VALUES (5008, '5008000000', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO employee (employee_id, user_id, role, status) VALUES (108, 5008, 'onboarding_assistant', 'active')")
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (6007, 'Camp7', 'ENG6007', 'healthcamp', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'active', 0)"
        )
    )
    await test_db_session.commit()

    repository = EngagementsRepository()
    
    # Add assignment
    await repository.add_onboarding_assistant(test_db_session, engagement_id=6007, employee_id=108)
    await test_db_session.commit()

    # Get specific assignment
    assignment = await repository.get_onboarding_assistant_assignment(
        test_db_session,
        engagement_id=6007,
        employee_id=108,
    )
    
    assert assignment is not None
    assert assignment.engagement_id == 6007
    assert assignment.employee_id == 108


@pytest.mark.asyncio
async def test_get_nonexistent_assignment_returns_none(test_db_session):
    """Test that getting a non-existent assignment returns None."""
    repository = EngagementsRepository()
    
    assignment = await repository.get_onboarding_assistant_assignment(
        test_db_session,
        engagement_id=9999,
        employee_id=9999,
    )
    
    assert assignment is None


@pytest.mark.asyncio
async def test_b2c_engagement_created_without_onboarding_assistants(test_db_session):
    """Test that B2C engagements are created without any onboarding assistants by default."""
    # Seed required packages
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PKG1', 'Package 1', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) VALUES (1, 'REF1', 'Diag 1', 'Provider', 'active')")
    )
    
    # Seed users table for foreign key
    await test_db_session.execute(
        text("INSERT INTO users (user_id, phone, status) VALUES (5009, '5009000000', 'active')")
    )
    await test_db_session.commit()
    
    # Create a B2C engagement directly
    test_db_session.add(
        Engagement(
            engagement_id=6008,
            engagement_name="B2C-Test",
            engagement_code="B2C6008",
            engagement_type="healthcamp",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="Mumbai",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="active",
            participant_count=0,
            organization_id=None,  # B2C has no organization
        )
    )
    await test_db_session.commit()

    # Verify no onboarding assistants are assigned
    repository = EngagementsRepository()
    assignments = await repository.list_onboarding_assistants(test_db_session, engagement_id=6008)
    
    assert len(assignments) == 0
