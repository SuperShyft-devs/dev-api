"""Reports dependency providers."""

from __future__ import annotations

from db.session import AsyncSessionLocal
from modules.assessments.repository import AssessmentsRepository
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.diagnostics.dependencies import get_diagnostics_service
from modules.diagnostics.healthians.client import (
    get_access_token as healthians_get_access_token,
    get_booking_digital_value as healthians_get_booking_digital_value,
)
from modules.metsights.dependencies import get_metsights_service
from modules.questionnaire.healthy_habits_service import HealthyHabitsService
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.repository import ReportsRepository
from modules.reports.service import ReportsService
from modules.reports.camp_report_sections_repository import CampReportSectionsRepository
from modules.reports.camp_report_sections_service import CampReportSectionsService
from modules.reports.camp_reports_repository import CampReportsRepository
from modules.reports.camp_reports_service import CampReportsService


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
        healthians_get_access_token=healthians_get_access_token,
        healthians_get_booking_digital_value=healthians_get_booking_digital_value,
    )


def get_camp_report_sections_service() -> CampReportSectionsService:
    return CampReportSectionsService(
        repository=CampReportSectionsRepository(),
        audit_service=AuditService(AuditRepository()),
    )


def get_camp_reports_service() -> CampReportsService:
    return CampReportsService(
        repository=CampReportsRepository(),
        audit_service=AuditService(AuditRepository()),
    )
