"""Shared employee role and organization-scoped access checks."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.employee.models import EmployeeRole
from modules.employee.service import EmployeeContext
from modules.organizations.models import Organization
from modules.organizations.repository import OrganizationsRepository

INTERNAL_ROLES = frozenset({EmployeeRole.admin, EmployeeRole.onboarding_assistant})


def is_internal_employee(role: EmployeeRole) -> bool:
    return role in INTERNAL_ROLES


def ensure_employee_present(employee: EmployeeContext | None) -> None:
    if employee is None:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


def ensure_internal_employee(employee: EmployeeContext | None) -> None:
    ensure_employee_present(employee)
    if not is_internal_employee(employee.role):
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


async def ensure_org_access(
    db: AsyncSession,
    employee: EmployeeContext | None,
    organization_id: int,
    *,
    repository: OrganizationsRepository | None = None,
) -> None:
    ensure_employee_present(employee)
    if is_internal_employee(employee.role):
        return

    if employee.role != EmployeeRole.organization_manager:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )

    organization = await _load_organization(db, organization_id, repository=repository)
    if organization is None:
        raise AppError(
            status_code=404,
            error_code="ORGANIZATION_NOT_FOUND",
            message="Organization does not exist",
        )

    if organization.contact_person_user_id != employee.user_id:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


async def ensure_camp_access(
    db: AsyncSession,
    employee: EmployeeContext | None,
    organization_id: int,
    *,
    repository: OrganizationsRepository | None = None,
) -> None:
    await ensure_org_access(
        db,
        employee,
        organization_id,
        repository=repository,
    )


async def _load_organization(
    db: AsyncSession,
    organization_id: int,
    *,
    repository: OrganizationsRepository | None,
) -> Organization | None:
    if repository is not None:
        return await repository.get_by_id(db, organization_id)
    return await db.get(Organization, organization_id)
