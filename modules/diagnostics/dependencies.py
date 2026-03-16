"""Diagnostics module dependencies."""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.diagnostics.repository import DiagnosticsRepository
from modules.diagnostics.service import DiagnosticsService


def get_diagnostics_service() -> DiagnosticsService:
    return DiagnosticsService(
        repository=DiagnosticsRepository(),
        audit_service=AuditService(AuditRepository()),
    )
