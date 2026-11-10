"""RecipientResolver department recipient types."""

from __future__ import annotations

import pytest

from modules.notifications.recipient_resolver import RecipientResolver
from tests.helpers.auth import seed_employee
from tests.helpers.employee_departments import seed_employee_departments


@pytest.mark.asyncio
async def test_resolve_departments_ops_filters_active_ops(test_db_session):
    await seed_employee(test_db_session, employee_id=801, role="admin", phone="8010000001", commit=False)
    await seed_employee(test_db_session, employee_id=802, role="inferior_admin", phone="8020000001", commit=False)
    await seed_employee(test_db_session, employee_id=803, role="admin", status="inactive", phone="8030000001", commit=False)
    await seed_employee_departments(test_db_session, employee_id=801, departments=["ops"], commit=False)
    await seed_employee_departments(test_db_session, employee_id=802, departments=["tech"], commit=False)
    await seed_employee_departments(test_db_session, employee_id=803, departments=["ops"], commit=False)
    await test_db_session.commit()

    resolver = RecipientResolver()
    target = await resolver.resolve(test_db_session, recipient_type="departments.ops", context={})
    phones = sorted(c.get("phone") for c in target.contacts if c.get("phone"))
    assert phones == ["8010000001"]


@pytest.mark.asyncio
async def test_resolve_departments_any_active_with_assignment(test_db_session):
    await seed_employee(test_db_session, employee_id=811, role="admin", phone="8110000001", commit=False)
    await seed_employee(test_db_session, employee_id=812, role="admin", phone="8120000001", commit=False)
    await seed_employee_departments(test_db_session, employee_id=811, departments=["design"], commit=False)
    await seed_employee_departments(test_db_session, employee_id=812, departments=["tech"], commit=False)
    await test_db_session.commit()

    resolver = RecipientResolver()
    target = await resolver.resolve(test_db_session, recipient_type="departments", context={})
    phones = sorted(c.get("phone") for c in target.contacts if c.get("phone"))
    assert phones == ["8110000001", "8120000001"]
