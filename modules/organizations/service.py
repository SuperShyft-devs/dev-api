"""Organizations service.

These endpoints are employee-only.

Business rules:
- Validate input rules that are not covered by Pydantic.
- Enforce employee access.
- Enforce allowed status values.
- Create audit logs for all mutations.
"""

from __future__ import annotations

from core.exceptions import AppError
from common.slug import slugify_department
from modules.audit.service import AuditService
from modules.employee.access_control import (
    ensure_admin,
    ensure_camp_access_admin_or_org_manager,
    ensure_employee_present,
    ensure_internal_employee,
    ensure_org_access,
    is_internal_employee,
)
from modules.employee.models import Employee, EmployeeRole
from modules.employee.repository import EmployeeRepository
from modules.employee.service import EmployeeContext
from modules.engagements.camp_no import format_camp_name
from modules.organizations.models import Organization
from modules.organizations.repository import OrganizationsRepository
from modules.organizations.schemas import OrganizationCreateRequest, OrganizationUpdateRequest
from modules.users.repository import UsersRepository


_ALLOWED_ORGANIZATION_STATUS = {"active", "inactive", "archived"}


def _normalize_status(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_departments(
    inputs: list | None,
) -> list[dict[str, str]] | None:
    if inputs is None:
        return None
    if not inputs:
        return None

    seen_names: set[str] = set()
    seen_slugs: set[str] = set()
    normalized: list[dict[str, str]] = []

    for item in inputs:
        name = (getattr(item, "department", None) or "").strip()
        if not name:
            continue
        name_key = name.casefold()
        if name_key in seen_names:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        slug = slugify_department(name)
        if not slug:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if slug in seen_slugs:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        seen_names.add(name_key)
        seen_slugs.add(slug)
        normalized.append({"department": name, "slug": slug})

    return normalized or None


def get_department_slugs(organization: Organization) -> set[str]:
    departments = organization.departments or []
    if not isinstance(departments, list):
        return set()
    slugs: set[str] = set()
    for item in departments:
        if isinstance(item, dict):
            slug = (item.get("slug") or "").strip()
            if slug:
                slugs.add(slug)
    return slugs


def resolve_department_label(organization: Organization, slug: str | None) -> str | None:
    value = (slug or "").strip()
    if not value:
        return None
    departments = organization.departments or []
    if not isinstance(departments, list):
        return None
    for item in departments:
        if isinstance(item, dict) and (item.get("slug") or "").strip() == value:
            label = (item.get("department") or "").strip()
            return label or value
    return None


def validate_participant_department_for_organization(
    organization: Organization | None,
    participant_department: str | None,
) -> str | None:
    """Validate and return normalized slug, or None if department not provided."""
    value = (participant_department or "").strip()
    if not value:
        return None
    if organization is None:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Department requires an organization engagement",
        )
    allowed = get_department_slugs(organization)
    if value not in allowed:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid department")
    return value


class OrganizationsService:
    def __init__(
        self,
        repository: OrganizationsRepository,
        employee_repository: EmployeeRepository | None = None,
        users_repository: UsersRepository | None = None,
        audit_service: AuditService | None = None,
    ):
        self._repository = repository
        self._employee_repository = employee_repository or EmployeeRepository()
        self._users_repository = users_repository or UsersRepository()
        self._audit_service = audit_service

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def _validate_contact_person_user_id(self, db, user_id: int | None) -> int | None:
        if user_id is None:
            return None

        user = await self._users_repository.get_user_by_id(db, user_id)
        if user is None or (user.status or "").lower() != "active":
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        return user_id

    async def _ensure_contact_person_employee(self, db, user_id: int) -> None:
        existing = await self._employee_repository.get_by_user_id(db, user_id)
        if existing is None:
            row = Employee(
                user_id=user_id,
                role=EmployeeRole.organization_manager,
                status="active",
            )
            await self._employee_repository.create(db, row)
            return

        if (existing.status or "").lower() != "active":
            existing.status = "active"
            await self._employee_repository.update(db, existing)

    async def create_organization_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        payload: OrganizationCreateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Organization:
        ensure_internal_employee(employee)

        name = payload.name.strip()
        if not name:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        existing = await self._repository.get_by_name(db, name)
        if existing is not None:
            raise AppError(status_code=409, error_code="ORGANIZATION_ALREADY_EXISTS", message="Organization already exists")

        contact_person_user_id = await self._validate_contact_person_user_id(
            db,
            payload.contact_person_user_id,
        )

        organization = Organization(
            name=name,
            organization_type=payload.organization_type,
            logo=payload.logo,
            website_url=payload.website_url,
            address=payload.address,
            pin_code=payload.pin_code,
            city=payload.city,
            state=payload.state,
            country=payload.country,
            contact_person_user_id=contact_person_user_id,
            bd_employee_id=payload.bd_employee_id,
            departments=_normalize_departments(payload.departments),
            status="active",
            created_employee_id=employee.employee_id,
            updated_employee_id=employee.employee_id,
        )

        organization = await self._repository.create(db, organization)

        if contact_person_user_id is not None:
            await self._ensure_contact_person_employee(db, contact_person_user_id)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_CREATE_ORGANIZATION",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return organization

    @staticmethod
    def organization_to_details_dict(organization: Organization) -> dict:
        return {
            "organization_id": organization.organization_id,
            "name": organization.name,
            "organization_type": organization.organization_type,
            "logo": organization.logo,
            "website_url": organization.website_url,
            "address": organization.address,
            "pin_code": organization.pin_code,
            "city": organization.city,
            "state": organization.state,
            "country": organization.country,
            "contact_person_user_id": organization.contact_person_user_id,
            "bd_employee_id": organization.bd_employee_id,
            "departments": organization.departments,
            "status": organization.status,
            "created_at": organization.created_at,
            "created_employee_id": organization.created_employee_id,
            "updated_at": organization.updated_at,
            "updated_employee_id": organization.updated_employee_id,
        }

    async def list_organizations_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
        status: str | None,
        organization_type: str | None,
        bd_employee_id: int | None,
        search: str | None = None,
        city: str | None = None,
        country: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> tuple[list[Organization], int]:
        ensure_internal_employee(employee)

        status_value = None
        if status is not None:
            normalized = _normalize_status(status)
            if normalized not in _ALLOWED_ORGANIZATION_STATUS:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            status_value = normalized

        organizations = await self._repository.list_organizations(
            db,
            page=page,
            limit=limit,
            status=status_value,
            organization_type=organization_type,
            bd_employee_id=bd_employee_id,
            search=search,
            city=city,
            country=country,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        total = await self._repository.count_organizations(
            db,
            status=status_value,
            organization_type=organization_type,
            bd_employee_id=bd_employee_id,
            search=search,
            city=city,
            country=country,
        )
        return organizations, total

    async def list_my_organizations_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> tuple[list[dict], int]:
        ensure_employee_present(employee)

        contact_person_user_id = None
        if employee.role == EmployeeRole.admin:
            pass
        elif employee.role == EmployeeRole.organization_manager:
            contact_person_user_id = employee.user_id
        else:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        organizations = await self._repository.list_organizations(
            db,
            page=page,
            limit=limit,
            contact_person_user_id=contact_person_user_id,
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        total = await self._repository.count_organizations(
            db,
            contact_person_user_id=contact_person_user_id,
            search=search,
        )
        return [self.organization_to_details_dict(org) for org in organizations], total

    async def get_organization_filter_options_for_employee(self, db, *, employee: EmployeeContext) -> dict:
        ensure_internal_employee(employee)
        cities, countries = await self._repository.list_distinct_cities_and_countries(db)
        return {"cities": cities, "countries": countries}

    async def get_organization_details_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        organization_id: int,
    ) -> Organization:
        organization = await self._repository.get_by_id(db, organization_id)
        if organization is None:
            raise AppError(
                status_code=404,
                error_code="ORGANIZATION_NOT_FOUND",
                message="Organization does not exist",
            )

        await ensure_org_access(
            db,
            employee,
            organization_id,
            repository=self._repository,
        )

        return organization

    async def update_organization_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        organization_id: int,
        payload: OrganizationUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Organization:
        organization = await self._repository.get_by_id(db, organization_id)
        if organization is None:
            raise AppError(
                status_code=404,
                error_code="ORGANIZATION_NOT_FOUND",
                message="Organization does not exist",
            )

        await ensure_org_access(
            db,
            employee,
            organization_id,
            repository=self._repository,
        )

        name = payload.name.strip()
        if not name:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        if name != (organization.name or ""):
            existing = await self._repository.get_by_name(db, name)
            if existing is not None and existing.organization_id != organization.organization_id:
                raise AppError(
                    status_code=409,
                    error_code="ORGANIZATION_ALREADY_EXISTS",
                    message="Organization already exists",
                )

        organization.name = name
        organization.organization_type = payload.organization_type
        organization.logo = payload.logo
        organization.website_url = payload.website_url
        organization.address = payload.address
        organization.pin_code = payload.pin_code
        organization.city = payload.city
        organization.state = payload.state
        organization.country = payload.country
        if is_internal_employee(employee.role):
            contact_person_user_id = await self._validate_contact_person_user_id(
                db,
                payload.contact_person_user_id,
            )
            organization.contact_person_user_id = contact_person_user_id
            if contact_person_user_id is not None:
                await self._ensure_contact_person_employee(db, contact_person_user_id)
        organization.bd_employee_id = payload.bd_employee_id
        organization.departments = _normalize_departments(payload.departments)
        organization.updated_employee_id = employee.employee_id

        organization = await self._repository.update(db, organization)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_ORGANIZATION",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return organization

    async def change_organization_status_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        organization_id: int,
        status: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Organization:
        ensure_internal_employee(employee)

        organization = await self._repository.get_by_id(db, organization_id)
        if organization is None:
            raise AppError(
                status_code=404,
                error_code="ORGANIZATION_NOT_FOUND",
                message="Organization does not exist",
            )

        normalized = _normalize_status(status)
        if normalized not in _ALLOWED_ORGANIZATION_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        organization.status = normalized
        organization.updated_employee_id = employee.employee_id
        organization = await self._repository.update(db, organization)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_ORGANIZATION_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return organization

    async def list_participants_for_organization(
        self,
        db,
        *,
        employee: EmployeeContext,
        organization_id: int,
        page: int,
        limit: int,
    ) -> tuple[list[dict], int]:
        """Fetch all distinct users enrolled across all engagements for an organization."""
        ensure_internal_employee(employee)

        # Validate organization exists
        organization = await self._repository.get_by_id(db, organization_id)
        if organization is None:
            raise AppError(
                status_code=404,
                error_code="ORGANIZATION_NOT_FOUND",
                message="Organization does not exist",
            )

        # Fetch participants with pagination
        participants = await self._repository.list_participants_by_organization_id(
            db,
            organization_id=organization_id,
            page=page,
            limit=limit,
        )

        # Count total distinct participants
        total = await self._repository.count_participants_by_organization_id(
            db,
            organization_id=organization_id,
        )

        # Transform tuple results to dictionary format
        result = []
        for row in participants:
            (
                user_id,
                first_name,
                last_name,
                phone,
                email,
                address,
                pin_code,
                city,
                state,
                country,
                status,
            ) = row
            result.append({
                "user_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "phone": phone,
                "email": email,
                "address": address,
                "pin_code": pin_code,
                "city": city,
                "state": state,
                "country": country,
                "status": status,
            })

        return result, total

    @staticmethod
    def _camp_rows_to_dicts(rows: list[tuple]) -> list[dict]:
        result = []
        for (
            camp_no,
            organization_id,
            organization_name,
            start_date,
            engagement_count,
            department_count,
            report_count,
        ) in rows:
            org_name = organization_name or ""
            result.append(
                {
                    "camp_no": int(camp_no),
                    "camp_name": format_camp_name(org_name, start_date),
                    "organization_id": int(organization_id),
                    "organization_name": org_name,
                    "start_date": start_date,
                    "engagement_count": int(engagement_count),
                    "department_count": int(department_count or 0),
                    "report_count": int(report_count or 0),
                }
            )
        return result

    async def list_camps_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> tuple[list[dict], int]:
        ensure_admin(employee)

        total = await self._repository.count_camps(db, search=search)
        rows = await self._repository.list_camps(
            db,
            page=page,
            limit=limit,
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

        return self._camp_rows_to_dicts(rows), total

    async def list_camps_for_organization_for_employee(
        self,
        db,
        *,
        employee: EmployeeContext,
        organization_id: int,
        page: int,
        limit: int,
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> tuple[list[dict], int]:
        await ensure_camp_access_admin_or_org_manager(
            db,
            employee,
            organization_id,
            repository=self._repository,
        )

        total = await self._repository.count_camps(
            db,
            search=search,
            organization_id=organization_id,
        )
        rows = await self._repository.list_camps(
            db,
            page=page,
            limit=limit,
            search=search,
            organization_id=organization_id,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

        return self._camp_rows_to_dicts(rows), total
