"""Experts module dependencies."""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.experts.repository import ExpertsRepository, ExpertTypesRepository
from modules.experts.service import ExpertsService, ExpertTypesService


def get_expert_types_service() -> ExpertTypesService:
    return ExpertTypesService(repository=ExpertTypesRepository())


def get_experts_service() -> ExpertsService:
    from modules.employee.repository import EmployeeRepository

    audit_service = AuditService(AuditRepository())
    expert_types_service = get_expert_types_service()
    return ExpertsService(
        repository=ExpertsRepository(),
        audit_service=audit_service,
        expert_types_service=expert_types_service,
        employee_repository=EmployeeRepository(),
    )
