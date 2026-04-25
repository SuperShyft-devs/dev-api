"""Reports dependency providers."""

from __future__ import annotations

from db.session import AsyncSessionLocal
from modules.assessments.repository import AssessmentsRepository
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.diagnostics.dependencies import get_diagnostics_service
from modules.metsights.dependencies import get_metsights_service
from modules.questionnaire.healthy_habits_service import HealthyHabitsService
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.repository import ReportsRepository
from modules.reports.service import ReportsService


def get_reports_service() -> ReportsService:
    questionnaire_repository = QuestionnaireRepository()
    return ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=get_metsights_service(),
        diagnostics_service=get_diagnostics_service(),
        audit_service=AuditService(AuditRepository()),
        session_factory=AsyncSessionLocal,
        healthy_habits_service=HealthyHabitsService(questionnaire_repository),
        questionnaire_repository=questionnaire_repository,
    )
