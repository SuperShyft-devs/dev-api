"""Users module dependencies."""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.assessments.dependencies import get_assessments_service
from modules.assessments.repository import AssessmentsRepository
from modules.engagements.dependencies import get_engagements_service
from modules.metsights.dependencies import get_metsights_service
from modules.questionnaire.dependencies import get_questionnaire_user_service
from modules.questionnaire.repository import QuestionnaireRepository
from modules.platform_settings.dependencies import get_platform_settings_service_readonly
from modules.users.participant_journey_service import ParticipantJourneyService
from modules.users.repository import UsersRepository
from modules.users.service import UsersService


def get_users_service() -> UsersService:
    audit_service = AuditService(AuditRepository())
    engagements_service = get_engagements_service()
    assessments_service = get_assessments_service()
    return UsersService(
        repository=UsersRepository(),
        audit_service=audit_service,
        engagements_service=engagements_service,
        assessments_service=assessments_service,
        platform_settings_service=get_platform_settings_service_readonly(),
        metsights_service=get_metsights_service(),
    )


def get_participant_journey_service() -> ParticipantJourneyService:
    return ParticipantJourneyService(
        users_repository=UsersRepository(),
        assessments_repository=AssessmentsRepository(),
        questionnaire_repository=QuestionnaireRepository(),
        questionnaire_service=get_questionnaire_user_service(),
    )
