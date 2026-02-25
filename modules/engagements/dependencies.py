"""Engagements module dependencies."""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.employee.repository import EmployeeRepository
from modules.employee.service import EmployeeService
from modules.engagements.onboarding_assistants_service import OnboardingAssistantsService
from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import EngagementsService
from modules.organizations.repository import OrganizationsRepository


def get_engagements_service() -> EngagementsService:
    audit_service = AuditService(AuditRepository())
    organizations_repository = OrganizationsRepository()
    return EngagementsService(
        repository=EngagementsRepository(),
        audit_service=audit_service,
        organizations_repository=organizations_repository,
    )


def get_onboarding_assistants_service() -> OnboardingAssistantsService:
    audit_service = AuditService(AuditRepository())
    employee_service = EmployeeService(EmployeeRepository())
    return OnboardingAssistantsService(
        repository=EngagementsRepository(),
        employee_service=employee_service,
        audit_service=audit_service,
    )
