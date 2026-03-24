"""Dependency providers for Metsights module."""

from __future__ import annotations

from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService


def get_metsights_service() -> MetsightsService:
    return MetsightsService(client=MetsightsClient())
