"""Questionnaire module dependencies."""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.questionnaire.repository import QuestionnaireRepository
from modules.questionnaire.service import QuestionnaireService


def get_questionnaire_management_service() -> QuestionnaireService:
    """Service used by employee-only questionnaire routes (audit is mandatory)."""

    audit_service = AuditService(AuditRepository())
    return QuestionnaireService(repository=QuestionnaireRepository(), audit_service=audit_service)


def get_questionnaire_user_service() -> QuestionnaireService:
    """Service used by user-facing questionnaire routes (audit is mandatory)."""

    audit_service = AuditService(AuditRepository())
    return QuestionnaireService(repository=QuestionnaireRepository(), audit_service=audit_service)
