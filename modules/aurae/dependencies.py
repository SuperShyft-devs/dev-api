"""Aurae module dependencies."""

from __future__ import annotations

from modules.assessments.repository import AssessmentsRepository
from modules.aurae.service import AuraeService
from modules.engagements.repository import EngagementsRepository
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.repository import UsersRepository


def get_aurae_service() -> AuraeService:
    return AuraeService(
        assessments_repository=AssessmentsRepository(),
        engagements_repository=EngagementsRepository(),
        users_repository=UsersRepository(),
        questionnaire_repository=QuestionnaireRepository(),
    )
