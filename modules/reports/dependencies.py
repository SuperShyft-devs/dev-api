"""Reports dependency providers."""

from __future__ import annotations

from modules.assessments.repository import AssessmentsRepository
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.metsights.dependencies import get_metsights_service
from modules.reports.repository import ReportsRepository
from modules.reports.service import ReportsService


def get_reports_service() -> ReportsService:
    return ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=get_metsights_service(),
        audit_service=AuditService(AuditRepository()),
    )
