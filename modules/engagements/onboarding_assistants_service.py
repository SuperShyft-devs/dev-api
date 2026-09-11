"""Engagement onboarding assistant assignment (partners + staff employees)."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.access_control import (
    ensure_admin,
    ensure_valid_onboarding_assistant_assignee_employee_role,
    ensure_valid_onboarding_assistant_assignee_role,
)
from modules.employee.models import Employee, EmployeeRole
from modules.employee.repository import EmployeeRepository
from modules.employee.service import EmployeeContext
from modules.engagements.models import OnboardingAssistantAssignment
from modules.engagements.repository import EngagementsRepository
from modules.partners.models import Partner, PartnerRole
from modules.partners.repository import PartnersRepository
from modules.partners.schemas import PartnerCreateRequest
from modules.partners.service import PartnersService


def _normalize_int(value: int) -> int:
    """Normalize and validate integer input."""
    if not isinstance(value, int) or value <= 0:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
    return value


def _partner_payload(partner: Partner) -> dict:
    return {
        "kind": "partner",
        "partner_id": partner.partner_id,
        "employee_id": None,
        "name": partner.name,
        "phone": partner.phone,
        "email": partner.email,
        "role": partner.role.value if isinstance(partner.role, PartnerRole) else partner.role,
        "status": partner.status,
    }


def _employee_payload(employee: Employee) -> dict:
    role = employee.role.value if isinstance(employee.role, EmployeeRole) else str(employee.role)
    return {
        "kind": "employee",
        "partner_id": None,
        "employee_id": employee.employee_id,
        "name": employee.name,
        "phone": employee.phone,
        "email": employee.email,
        "role": role,
        "status": employee.status,
    }


def _existing_partner_payload(partner: Partner) -> dict:
    return {"status": "confirmation_required", "existing_partner": _partner_payload(partner)}


class OnboardingAssistantsService:
    """Assign phlebo/expert partners and admin/inferior_admin employees to engagements."""

    def __init__(
        self,
        repository: EngagementsRepository,
        partners_repository: PartnersRepository,
        employee_repository: EmployeeRepository | None = None,
        partners_service: PartnersService | None = None,
        audit_service: AuditService | None = None,
    ):
        self._repository = repository
        self._partners_repository = partners_repository
        self._employee_repository = employee_repository or EmployeeRepository()
        self._partners_service = partners_service
        self._audit_service = audit_service

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    def _require_partners_service(self) -> PartnersService:
        if self._partners_service is None:
            raise RuntimeError("Partners service is required")
        return self._partners_service

    async def list_onboarding_assistants_for_engagement(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
    ) -> list[dict]:
        """List partners and employees assigned as onboarding assistants."""
        ensure_admin(employee)

        engagement_id = _normalize_int(engagement_id)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id=engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        return await self._repository.list_onboarding_assistant_assignee_rows(
            db, engagement_id=engagement_id
        )

    async def assign_onboarding_assistants_to_engagement(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        partner_ids: list[int] | None = None,
        employee_ids: list[int] | None = None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        """Assign partners and/or staff employees as onboarding assistants."""
        ensure_admin(employee)

        engagement_id = _normalize_int(engagement_id)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id=engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        partner_ids = partner_ids or []
        employee_ids = employee_ids or []
        if not partner_ids and not employee_ids:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        added_partner_ids: list[int] = []
        skipped_partner_ids: list[int] = []
        added_employee_ids: list[int] = []
        skipped_employee_ids: list[int] = []

        normalized_partner_ids: list[int] = []
        seen_partners: set[int] = set()
        for raw in partner_ids:
            partner_id = _normalize_int(raw)
            if partner_id in seen_partners:
                continue
            seen_partners.add(partner_id)
            normalized_partner_ids.append(partner_id)

        for partner_id in normalized_partner_ids:
            partner = await self._partners_repository.get_by_id(db, partner_id)
            if partner is None:
                raise AppError(
                    status_code=404,
                    error_code="PARTNER_NOT_FOUND",
                    message="Partner does not exist",
                )
            if (partner.status or "").lower() != "active":
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Partner is not active",
                )
            ensure_valid_onboarding_assistant_assignee_role(partner.role)

            existing = await self._repository.get_onboarding_assistant_assignment(
                db,
                engagement_id=engagement_id,
                partner_id=partner_id,
            )
            if existing is not None:
                skipped_partner_ids.append(partner_id)
                continue

            assignment = OnboardingAssistantAssignment(
                engagement_id=engagement_id,
                partner_id=partner_id,
                employee_id=None,
            )
            await self._repository.create_onboarding_assistant_assignment(db, assignment)
            added_partner_ids.append(partner_id)

        normalized_employee_ids: list[int] = []
        seen_employees: set[int] = set()
        for raw in employee_ids:
            eid = _normalize_int(raw)
            if eid in seen_employees:
                continue
            seen_employees.add(eid)
            normalized_employee_ids.append(eid)

        for eid in normalized_employee_ids:
            emp = await self._employee_repository.get_by_id(db, eid)
            if emp is None:
                raise AppError(
                    status_code=404,
                    error_code="EMPLOYEE_NOT_FOUND",
                    message="Employee does not exist",
                )
            if (emp.status or "").lower() != "active":
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Employee is not active",
                )
            ensure_valid_onboarding_assistant_assignee_employee_role(emp.role)

            existing = await self._repository.get_onboarding_assistant_assignment(
                db,
                engagement_id=engagement_id,
                employee_id=eid,
            )
            if existing is not None:
                skipped_employee_ids.append(eid)
                continue

            assignment = OnboardingAssistantAssignment(
                engagement_id=engagement_id,
                partner_id=None,
                employee_id=eid,
            )
            await self._repository.create_onboarding_assistant_assignment(db, assignment)
            added_employee_ids.append(eid)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_ASSIGN_ONBOARDING_ASSISTANTS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        return {
            "engagement_id": engagement_id,
            "added_partner_ids": added_partner_ids,
            "skipped_partner_ids": skipped_partner_ids,
            "added_employee_ids": added_employee_ids,
            "skipped_employee_ids": skipped_employee_ids,
        }

    async def create_and_assign_phlebo(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        name: str,
        phone: str,
        confirm_existing: bool,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        """Create or reuse a phlebo partner and assign them to an engagement."""
        ensure_admin(employee)

        engagement_id = _normalize_int(engagement_id)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id=engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        existing_partner = await self._partners_repository.get_by_phone(db, phone)
        if existing_partner is not None and not confirm_existing:
            return _existing_partner_payload(existing_partner)

        partner_created = False
        partners_service = self._require_partners_service()

        if existing_partner is not None:
            partner = existing_partner
            role_value = (
                partner.role.value if isinstance(partner.role, PartnerRole) else str(partner.role or "")
            )
            if role_value != PartnerRole.phlebo.value:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="Partner role cannot be assigned as an onboarding assistant",
                )
            if (partner.status or "").lower() != "active":
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Partner is not active",
                )
            result_status = "assigned"
        else:
            partner = await partners_service.create_partner(
                db,
                employee=employee,
                payload=PartnerCreateRequest(
                    name=name.strip(),
                    phone=phone,
                    role=PartnerRole.phlebo,
                    status="active",
                ),
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
            )
            partner_created = True
            result_status = "created"

        assign_result = await self.assign_onboarding_assistants_to_engagement(
            db,
            employee=employee,
            engagement_id=engagement_id,
            partner_ids=[partner.partner_id],
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

        return {
            "status": result_status,
            "partner_id": partner.partner_id,
            "partner_created": partner_created,
            "engagement_id": engagement_id,
            "added_partner_ids": assign_result["added_partner_ids"],
            "skipped_partner_ids": assign_result["skipped_partner_ids"],
            **{k: v for k, v in _partner_payload(partner).items() if k != "kind"},
        }

    async def remove_onboarding_assistant_from_engagement(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        partner_id: int | None = None,
        employee_id: int | None = None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        """Remove a partner or employee assignment from an engagement."""
        ensure_admin(employee)

        engagement_id = _normalize_int(engagement_id)
        if (partner_id is None) == (employee_id is None):
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Exactly one of partner_id or employee_id is required",
            )

        engagement = await self._repository.get_engagement_by_id(db, engagement_id=engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        if partner_id is not None:
            partner_id = _normalize_int(partner_id)
            deleted = await self._repository.delete_onboarding_assistant_assignment(
                db,
                engagement_id=engagement_id,
                partner_id=partner_id,
            )
            if deleted == 0:
                raise AppError(
                    status_code=404,
                    error_code="ONBOARDING_ASSISTANT_ASSIGNMENT_NOT_FOUND",
                    message="Partner is not assigned to this engagement",
                )
            removed = {"removed_partner_id": partner_id, "removed_employee_id": None}
        else:
            employee_id = _normalize_int(employee_id)  # type: ignore[arg-type]
            deleted = await self._repository.delete_onboarding_assistant_assignment(
                db,
                engagement_id=engagement_id,
                employee_id=employee_id,
            )
            if deleted == 0:
                raise AppError(
                    status_code=404,
                    error_code="ONBOARDING_ASSISTANT_ASSIGNMENT_NOT_FOUND",
                    message="Employee is not assigned to this engagement",
                )
            removed = {"removed_partner_id": None, "removed_employee_id": employee_id}

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_REMOVE_ONBOARDING_ASSISTANT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        return {"engagement_id": engagement_id, **removed}
