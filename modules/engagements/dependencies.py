"""Engagements module dependencies."""

from __future__ import annotations

from modules.assessments.dependencies import get_assessments_service
from modules.assessments.repository import AssessmentsRepository
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.employee.repository import EmployeeRepository
from modules.employee.service import EmployeeService
from modules.engagements.assessment_packages_service import (
    EngagementAssessmentPackagesService,
)
from modules.engagements.onboarding_assistants_service import OnboardingAssistantsService
from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import EngagementsService
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.organizations.repository import OrganizationsRepository
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.repository import ReportsRepository
from modules.users.repository import UsersRepository


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


def get_engagement_assessment_packages_service() -> EngagementAssessmentPackagesService:
    return EngagementAssessmentPackagesService(
        engagements_repository=EngagementsRepository(),
        assessments_repository=AssessmentsRepository(),
        reports_repository=ReportsRepository(),
        questionnaire_repository=QuestionnaireRepository(),
        users_repository=UsersRepository(),
        assessments_service=get_assessments_service(),
        metsights_service=MetsightsService(client=MetsightsClient()),
        audit_service=AuditService(AuditRepository()),
    )
