"""Audit module dependencies."""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService


def get_audit_service() -> AuditService:
    return AuditService(AuditRepository())
