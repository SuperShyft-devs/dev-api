"""Unit tests for organizations service."""

from __future__ import annotations

import pytest

from core.exceptions import AppError
from modules.employee.service import EmployeeContext
from modules.organizations.models import Organization
from modules.organizations.repository import OrganizationsRepository
from modules.organizations.schemas import OrganizationCreateRequest
from modules.organizations.service import OrganizationsService


@pytest.mark.asyncio
async def test_create_organization_rejects_duplicates(test_db_session):
    repo = OrganizationsRepository()
    service = OrganizationsService(repository=repo)

    test_db_session.add(Organization(organization_id=1, name="Acme", status="active"))
    await test_db_session.commit()

    with pytest.raises(AppError) as exc:
        await service.create_organization_for_employee(
            test_db_session,
            employee=EmployeeContext(employee_id=1, user_id=1, role="admin"),
            payload=OrganizationCreateRequest(name="Acme"),
            ip_address="1.1.1.1",
            user_agent="pytest",
            endpoint="/organizations",
        )

    assert exc.value.status_code == 409
