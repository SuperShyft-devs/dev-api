"""Shared employee role and organization-scoped access checks."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.employee.models import EmployeeRole
from modules.employee.service import EmployeeContext
from modules.engagements.repository import EngagementsRepository
from modules.organizations.models import Organization
from modules.organizations.repository import OrganizationsRepository

INTERNAL_ROLES = frozenset({EmployeeRole.admin, EmployeeRole.onboarding_assistant})

ONBOARDING_ASSISTANT_ASSIGNEE_ROLES = frozenset(
    {EmployeeRole.admin, EmployeeRole.onboarding_assistant, EmployeeRole.organization_manager}
)


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


def ensure_admin(employee: EmployeeContext | None) -> None:
    ensure_employee_present(employee)
    if employee.role != EmployeeRole.admin:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


def ensure_valid_onboarding_assistant_assignee_role(role: EmployeeRole) -> None:
    """Only admin, onboarding_assistant, and organization_manager may be assigned."""
    if role not in ONBOARDING_ASSISTANT_ASSIGNEE_ROLES:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Employee role cannot be assigned as an onboarding assistant",
        )


def ensure_engagement_running(engagement) -> None:
    if (getattr(engagement, "status", None) or "").lower() != "running":
        raise AppError(
            status_code=422,
            error_code="ENGAGEMENT_NOT_RUNNING",
            message="This engagement is not running",
        )


async def ensure_console_access(
    db: AsyncSession,
    employee: EmployeeContext | None,
    engagement_id: int,
    *,
    repository: EngagementsRepository,
) -> None:
    """Admins: any engagement. Org managers: assigned + org contact person. OAs: assigned + running."""
    ensure_employee_present(employee)
    if employee.role == EmployeeRole.admin:
        return

    if employee.role == EmployeeRole.organization_manager:
        assignment = await repository.get_onboarding_assistant_assignment(
            db, engagement_id=engagement_id, employee_id=employee.employee_id
        )
        if assignment is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        engagement = await repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )
        if engagement.organization_id is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )
        await ensure_org_access(db, employee, engagement.organization_id)
        return

    if employee.role != EmployeeRole.onboarding_assistant:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )

    assignment = await repository.get_onboarding_assistant_assignment(
        db, engagement_id=engagement_id, employee_id=employee.employee_id
    )
    if assignment is None:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )

    engagement = await repository.get_engagement_by_id(db, engagement_id)
    if engagement is None:
        raise AppError(
            status_code=404,
            error_code="ENGAGEMENT_NOT_FOUND",
            message="Engagement does not exist",
        )
    ensure_engagement_running(engagement)


async def ensure_org_manager_assignable_to_engagement(
    db: AsyncSession,
    *,
    assignee_user_id: int,
    assignee_role: EmployeeRole,
    engagement_id: int,
    repository: EngagementsRepository,
    organizations_repository: OrganizationsRepository | None = None,
) -> None:
    """Organization managers may only be assigned to engagements for orgs they manage."""
    if assignee_role != EmployeeRole.organization_manager:
        return

    engagement = await repository.get_engagement_by_id(db, engagement_id)
    if engagement is None:
        raise AppError(
            status_code=404,
            error_code="ENGAGEMENT_NOT_FOUND",
            message="Engagement does not exist",
        )
    if engagement.organization_id is None:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Organization manager can only be assigned to organization engagements",
        )

    organization = await _load_organization(
        db,
        engagement.organization_id,
        repository=organizations_repository,
    )
    if organization is None:
        raise AppError(
            status_code=404,
            error_code="ORGANIZATION_NOT_FOUND",
            message="Organization does not exist",
        )
    if organization.contact_person_user_id != assignee_user_id:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Organization manager must be the contact person for the engagement organization",
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


async def ensure_camp_access_admin_or_org_manager(
    db: AsyncSession,
    employee: EmployeeContext | None,
    organization_id: int,
    *,
    repository: OrganizationsRepository | None = None,
) -> None:
    """Allow admin (all camps) or organization_manager (own org only)."""
    ensure_employee_present(employee)
    if employee.role == EmployeeRole.admin:
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


async def _load_organization(
    db: AsyncSession,
    organization_id: int,
    *,
    repository: OrganizationsRepository | None,
) -> Organization | None:
    if repository is not None:
        return await repository.get_by_id(db, organization_id)
    return await db.get(Organization, organization_id)
