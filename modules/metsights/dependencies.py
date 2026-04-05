"""Dependency providers for Metsights module."""

from __future__ import annotations

from modules.assessments.dependencies import get_assessments_service
from modules.engagements.dependencies import get_engagements_service
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import MetsightsSyncService
from modules.platform_settings.dependencies import get_platform_settings_service_readonly
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.repository import UsersRepository


def get_metsights_service() -> MetsightsService:
    return MetsightsService(client=MetsightsClient())


def get_metsights_sync_service() -> MetsightsSyncService:
    return MetsightsSyncService(
        metsights_service=get_metsights_service(),
        users_repository=UsersRepository(),
        engagements_service=get_engagements_service(),
        assessments_service=get_assessments_service(),
        platform_settings_service=get_platform_settings_service_readonly(),
        questionnaire_repository=QuestionnaireRepository(),
    )
