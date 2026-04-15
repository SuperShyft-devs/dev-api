"""Experts module dependencies."""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.experts.repository import ExpertsRepository
from modules.experts.service import ExpertsService


def get_experts_service() -> ExpertsService:
    audit_service = AuditService(AuditRepository())
    return ExpertsService(repository=ExpertsRepository(), audit_service=audit_service)
