"""Shared employee role and organization-scoped access checks."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.employee.models import EmployeeRole
from modules.employee.permissions import context_has_capability
from modules.employee.service import EmployeeContext
from modules.engagements.repository import EngagementsRepository
from modules.organizations.contact_person import (
    OrgManagerScope,
    resolve_org_manager_scope,
    user_has_any_org_contact_role,
)
from modules.organizations.models import Organization
from modules.organizations.repository import OrganizationsRepository
from modules.partners.models import Partner, PartnerRole

INTERNAL_ROLES = frozenset({EmployeeRole.admin})

# Staff employees that can be assigned alongside phlebo/expert partners.
ONBOARDING_ASSISTANT_ASSIGNEE_EMPLOYEE_ROLES = frozenset(
    {EmployeeRole.admin, EmployeeRole.inferior_admin}
)

# Deprecated alias — partners use PartnerRole; kept for older imports.
ONBOARDING_ASSISTANT_ASSIGNEE_ROLES = ONBOARDING_ASSISTANT_ASSIGNEE_EMPLOYEE_ROLES

# Portal access for employees is admin-only; expert partners use partner JWT.
EXPERT_PORTAL_ROLES = frozenset({EmployeeRole.admin})


def ensure_valid_onboarding_assistant_assignee_role(role: str | PartnerRole) -> None:
    """Partners with role phlebo or expert may be assigned as onboarding assistants."""
    value = role.value if isinstance(role, PartnerRole) else str(role or "")
    if value not in {PartnerRole.phlebo.value, PartnerRole.expert.value}:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Partner role cannot be assigned as an onboarding assistant",
        )


def ensure_valid_onboarding_assistant_assignee_employee_role(role: EmployeeRole | str) -> None:
    """Admin / inferior_admin employees may be assigned to engagements."""
    value = role if isinstance(role, EmployeeRole) else EmployeeRole(str(role))
    if value not in ONBOARDING_ASSISTANT_ASSIGNEE_EMPLOYEE_ROLES:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Employee role cannot be assigned as an onboarding assistant",
        )


def is_internal_employee(role: EmployeeRole) -> bool:
    return role in INTERNAL_ROLES


def has_route_admin_scope(employee: EmployeeContext) -> bool:
    """True for full admins or an authorized route-bound Inferior Admin."""
    if employee.role == EmployeeRole.admin:
        return True
    capability = employee.capability
    return bool(
        employee.role == EmployeeRole.inferior_admin
        and capability is not None
        and context_has_capability(employee, capability.category, capability.action)
    )


def ensure_employee_present(employee: EmployeeContext | None) -> None:
    if employee is None:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


def ensure_internal_employee(employee: EmployeeContext | None) -> None:
    ensure_employee_present(employee)
    if has_route_admin_scope(employee):
        return
    if not is_internal_employee(employee.role):
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


def ensure_admin(employee: EmployeeContext | None) -> None:
    ensure_employee_present(employee)
    if not has_route_admin_scope(employee):
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


def ensure_not_expert_employee(employee: EmployeeContext | None) -> None:
    """Experts are partners, not employees — admin CRUD only needs an employee present."""
    ensure_employee_present(employee)


def ensure_expert_portal_access(
    employee: EmployeeContext | None = None,
    *,
    partner: Partner | None = None,
) -> None:
    """Allow admin employees or active expert partners on /experts/portal/*."""
    if partner is not None:
        role = partner.role.value if isinstance(partner.role, PartnerRole) else str(partner.role or "")
        if role == PartnerRole.expert.value and (partner.status or "").lower() == "active":
            return
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )
    ensure_employee_present(employee)
    if employee.role not in EXPERT_PORTAL_ROLES and not has_route_admin_scope(employee):
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )


def ensure_expert_portal_owns(
    employee: EmployeeContext | None,
    *,
    resource_expert_id: int,
    caller_expert_id: int | None,
    partner: Partner | None = None,
) -> None:
    """Admins may access any expert; expert partners only their own expert_id."""
    ensure_expert_portal_access(employee, partner=partner)
    if employee is not None and has_route_admin_scope(employee):
        return
    if caller_expert_id is None or caller_expert_id != resource_expert_id:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
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
    contact_id: int,
) -> OrgManagerScope | None:
    """Resolve org-manager scope; contact JSON stores partner_ids (organization_manager)."""
    return resolve_org_manager_scope(organization.contact_person_user_ids, contact_id)


def is_organization_manager_partner(partner: Partner | None) -> bool:
    if partner is None:
        return False
    role = partner.role.value if isinstance(partner.role, PartnerRole) else str(partner.role or "")
    return role == PartnerRole.organization_manager.value and (partner.status or "").lower() == "active"


def ensure_org_manager_has_contact_role(organization: Organization, contact_id: int) -> OrgManagerScope:
    scope = resolve_org_manager_scope_for_organization(organization, contact_id)
    if scope is None:
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )
    return scope


def org_manager_contact_id(
    *,
    employee: EmployeeContext | None = None,
    partner: Partner | None = None,
) -> int | None:
    """Return contact JSON id for an org manager employee (legacy) or partner."""
    if partner is not None and is_organization_manager_partner(partner):
        return int(partner.partner_id)
    if employee is not None and employee.role == EmployeeRole.organization_manager:
        return int(employee.employee_id)
    return None


# Backward-compatible private alias.
_org_manager_contact_id = org_manager_contact_id


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
    engagement_id: int,
    *,
    repository: EngagementsRepository,
    employee: EmployeeContext | None = None,
    partner: Partner | None = None,
) -> None:
    """Admins: any engagement. Org managers: org contact + city. Phlebo partners: assignment + running."""
    if employee is not None and has_route_admin_scope(employee):
        return

    contact_id = _org_manager_contact_id(employee=employee, partner=partner)
    if contact_id is not None:
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
        scope = ensure_org_manager_has_contact_role(organization, contact_id)
        ensure_engagement_city_access(scope, engagement.city)
        return

    if partner is not None:
        role = partner.role.value if isinstance(partner.role, PartnerRole) else str(partner.role or "")
        if role != PartnerRole.phlebo.value:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )
        assignment = await repository.get_onboarding_assistant_assignment(
            db, engagement_id=engagement_id, partner_id=partner.partner_id
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
        return

    raise AppError(
        status_code=403,
        error_code="FORBIDDEN",
        message="You do not have permission to perform this action",
    )


async def ensure_org_manager_assignable_to_engagement(
    db: AsyncSession,
    *,
    assignee_employee_id: int,
    assignee_role: EmployeeRole,
    engagement_id: int,
    repository: EngagementsRepository,
    organizations_repository: OrganizationsRepository | None = None,
) -> None:
    """Organization managers may only be linked to engagements for orgs they manage."""
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

    scope = resolve_org_manager_scope_for_organization(organization, assignee_employee_id)
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
    partner: Partner | None = None,
    repository: OrganizationsRepository | None = None,
) -> OrgManagerScope | None:
    if employee is not None and (is_internal_employee(employee.role) or has_route_admin_scope(employee)):
        return None

    contact_id = _org_manager_contact_id(employee=employee, partner=partner)
    if contact_id is None:
        if employee is None and partner is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )
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

    return ensure_org_manager_has_contact_role(organization, contact_id)


async def ensure_camp_access(
    db: AsyncSession,
    employee: EmployeeContext | None,
    organization_id: int,
    *,
    partner: Partner | None = None,
    repository: OrganizationsRepository | None = None,
    city: str | None = None,
    department: str | None = None,
) -> None:
    scope = await ensure_org_access(
        db,
        employee,
        organization_id,
        partner=partner,
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
    partner: Partner | None = None,
    repository: OrganizationsRepository | None = None,
) -> None:
    """Allow admin (all camps) or organization_manager (own org only)."""
    if employee is not None and has_route_admin_scope(employee):
        return

    contact_id = _org_manager_contact_id(employee=employee, partner=partner)
    if contact_id is None:
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

    if not user_has_any_org_contact_role(organization.contact_person_user_ids, contact_id):
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
    partner: Partner | None = None,
    repository: OrganizationsRepository | None = None,
) -> None:
    scope = await ensure_org_access(
        db,
        employee,
        organization_id,
        partner=partner,
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
    if has_route_admin_scope(employee):
        return OrgManagerScope(is_org_manager=True)
    if employee.role != EmployeeRole.organization_manager:
        return None
    organization = await _load_organization(db, organization_id, repository=repository)
    if organization is None:
        return None
    return resolve_org_manager_scope_for_organization(organization, employee.employee_id)


async def get_org_manager_scope_for_actor(
    db: AsyncSession,
    *,
    employee: EmployeeContext | None = None,
    partner: Partner | None = None,
    organization_id: int,
    repository: OrganizationsRepository | None = None,
) -> OrgManagerScope | None:
    if employee is not None and has_route_admin_scope(employee):
        return OrgManagerScope(is_org_manager=True)
    contact_id = _org_manager_contact_id(employee=employee, partner=partner)
    if contact_id is None:
        return None
    organization = await _load_organization(db, organization_id, repository=repository)
    if organization is None:
        return None
    return resolve_org_manager_scope_for_organization(organization, contact_id)


async def _load_organization(
    db: AsyncSession,
    organization_id: int,
    *,
    repository: OrganizationsRepository | None = None,
) -> Organization | None:
    if repository is not None:
        return await repository.get_by_id(db, organization_id)
    return await db.get(Organization, organization_id)
