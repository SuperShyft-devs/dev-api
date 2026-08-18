"""Shared employee role and organization-scoped access checks."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.employee.models import EmployeeRole
from modules.employee.service import EmployeeContext
from modules.engagements.repository import EngagementsRepository
from modules.organizations.contact_person import (
    OrgManagerScope,
    normalize_city_key,
    resolve_org_manager_scope,
    user_has_any_org_contact_role,
)
from modules.organizations.models import Organization
from modules.organizations.repository import OrganizationsRepository

INTERNAL_ROLES = frozenset({EmployeeRole.admin, EmployeeRole.onboarding_assistant})

ONBOARDING_ASSISTANT_ASSIGNEE_ROLES = frozenset(
    {
        EmployeeRole.admin,
        EmployeeRole.onboarding_assistant,
        EmployeeRole.organization_manager,
        EmployeeRole.expert,
    }
)

EXPERT_PORTAL_ROLES = frozenset({EmployeeRole.admin, EmployeeRole.expert})


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


def ensure_not_expert_employee(employee: EmployeeContext | None) -> None:
    """Expert-role employees may not use admin expert CRUD endpoints."""
    ensure_employee_present(employee)
    if employee.role == EmployeeRole.expert:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


def ensure_expert_portal_access(employee: EmployeeContext | None) -> None:
    """Only admin and expert roles may access /experts/portal/*."""
    ensure_employee_present(employee)
    if employee.role not in EXPERT_PORTAL_ROLES:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


def ensure_expert_portal_owns(
    employee: EmployeeContext,
    *,
    resource_expert_id: int,
    caller_expert_id: int | None,
) -> None:
    """Admins may access any expert; experts only their own expert_id."""
    ensure_expert_portal_access(employee)
    if employee.role == EmployeeRole.admin:
        return
    if caller_expert_id is None or caller_expert_id != resource_expert_id:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


def ensure_valid_onboarding_assistant_assignee_role(role: EmployeeRole) -> None:
    """Only admin, onboarding_assistant, organization_manager, and expert may be assigned."""
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


def resolve_org_manager_scope_for_organization(
    organization: Organization,
    user_id: int,
) -> OrgManagerScope | None:
    return resolve_org_manager_scope(organization.contact_person_user_ids, user_id)


def ensure_org_manager_has_contact_role(organization: Organization, user_id: int) -> OrgManagerScope:
    scope = resolve_org_manager_scope_for_organization(organization, user_id)
    if scope is None:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )
    return scope


def ensure_engagement_city_access(scope: OrgManagerScope, engagement_city: str | None) -> None:
    if not scope.can_access_engagement_city(engagement_city):
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


def ensure_camp_report_scope(
    scope: OrgManagerScope,
    *,
    city: str | None,
    department: str | None,
) -> None:
    if not scope.can_access_camp_report(city, department):
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


def ensure_participant_department_access(
    scope: OrgManagerScope,
    *,
    engagement_city: str | None,
    participant_department: str | None,
) -> None:
    allowed_slugs = scope.participant_department_slugs_for_city(engagement_city)
    if allowed_slugs is None:
        return
    dept = (participant_department or "").strip()
    if dept not in allowed_slugs:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
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

        organization = await _load_organization(db, engagement.organization_id)
        if organization is None:
            raise AppError(
                status_code=404,
                error_code="ORGANIZATION_NOT_FOUND",
                message="Organization does not exist",
            )
        scope = ensure_org_manager_has_contact_role(organization, employee.user_id)
        ensure_engagement_city_access(scope, engagement.city)
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

    scope = resolve_org_manager_scope_for_organization(organization, assignee_user_id)
    if scope is None:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Organization manager must be assigned in the organization contact persons",
        )
    ensure_engagement_city_access(scope, engagement.city)


async def ensure_org_access(
    db: AsyncSession,
    employee: EmployeeContext | None,
    organization_id: int,
    *,
    repository: OrganizationsRepository | None = None,
) -> OrgManagerScope | None:
    ensure_employee_present(employee)
    if is_internal_employee(employee.role):
        return None

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

    return ensure_org_manager_has_contact_role(organization, employee.user_id)


async def ensure_camp_access(
    db: AsyncSession,
    employee: EmployeeContext | None,
    organization_id: int,
    *,
    repository: OrganizationsRepository | None = None,
    city: str | None = None,
    department: str | None = None,
) -> None:
    scope = await ensure_org_access(
        db,
        employee,
        organization_id,
        repository=repository,
    )
    if scope is None:
        return
    ensure_camp_report_scope(scope, city=city, department=department)


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

    if not user_has_any_org_contact_role(organization.contact_person_user_ids, employee.user_id):
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


async def ensure_camp_report_access_for_employee(
    db: AsyncSession,
    employee: EmployeeContext | None,
    organization_id: int,
    *,
    city: str | None,
    department: str | None,
    repository: OrganizationsRepository | None = None,
) -> None:
    scope = await ensure_org_access(
        db,
        employee,
        organization_id,
        repository=repository,
    )
    if scope is None:
        return
    ensure_camp_report_scope(scope, city=city, department=department)


async def get_org_manager_scope_for_employee(
    db: AsyncSession,
    employee: EmployeeContext,
    organization_id: int,
    *,
    repository: OrganizationsRepository | None = None,
) -> OrgManagerScope | None:
    if employee.role == EmployeeRole.admin:
        return OrgManagerScope(is_org_manager=True)
    if employee.role != EmployeeRole.organization_manager:
        return None
    organization = await _load_organization(db, organization_id, repository=repository)
    if organization is None:
        return None
    return resolve_org_manager_scope_for_organization(organization, employee.user_id)


async def _load_organization(
    db: AsyncSession,
    organization_id: int,
    *,
    repository: OrganizationsRepository | None = None,
) -> Organization | None:
    if repository is not None:
        return await repository.get_by_id(db, organization_id)
    return await db.get(Organization, organization_id)
