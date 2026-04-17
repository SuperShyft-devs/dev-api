"""Assessments module dependencies.

Routers import dependencies, not service constructors.
"""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.assessments.repository import AssessmentsRepository
from modules.assessments.service import AssessmentsService
from modules.assessments.packages_service import AssessmentPackagesService
from modules.assessments.package_questions_service import AssessmentPackageCategoriesService
from modules.questionnaire.repository import QuestionnaireRepository


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


def get_assessment_package_categories_service() -> AssessmentPackageCategoriesService:
    """Service used by assessment package category routes (employee) and user GET /assessments/{id}/status."""

    audit_service = AuditService(AuditRepository())

    return AssessmentPackageCategoriesService(
        repository=AssessmentsRepository(),
        questionnaire_repository=QuestionnaireRepository(),
        audit_service=audit_service,
    )
