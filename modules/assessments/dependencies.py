"""Assessments module dependencies.

Routers import dependencies, not service constructors.
"""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.assessments.repository import AssessmentsRepository
from modules.assessments.service import AssessmentsService
from modules.assessments.packages_service import AssessmentPackagesService
from modules.assessments.package_questions_service import AssessmentPackageQuestionsService
from modules.questionnaire.repository import QuestionnaireRepository
from modules.questionnaire.service import QuestionnaireService


def get_assessments_service() -> AssessmentsService:
    return AssessmentsService(
        repository=AssessmentsRepository(),
        audit_service=AuditService(AuditRepository()),
    )


def get_assessment_packages_service() -> AssessmentPackagesService:
    return AssessmentPackagesService(
        repository=AssessmentsRepository(),
        audit_service=AuditService(AuditRepository()),
    )


def get_assessment_package_questions_service() -> AssessmentPackageQuestionsService:
    """Service used by employee-only assessment package question routes."""

    audit_service = AuditService(AuditRepository())
    questionnaire_service = QuestionnaireService(repository=QuestionnaireRepository(), audit_service=audit_service)

    return AssessmentPackageQuestionsService(
        repository=AssessmentsRepository(),
        questionnaire_service=questionnaire_service,
        audit_service=audit_service,
    )
