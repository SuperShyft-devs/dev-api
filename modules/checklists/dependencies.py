"""Checklists module dependencies."""

from __future__ import annotations

from fastapi import Depends

from modules.checklists.service import ChecklistsService
from modules.engagements.dependencies import get_engagements_service
from modules.engagements.service import EngagementsService


def get_checklists_service(
    engagements_service: EngagementsService = Depends(get_engagements_service),
) -> ChecklistsService:
    return engagements_service.lazy_checklists_service()
