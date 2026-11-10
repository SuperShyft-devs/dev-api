"""Webhooks module HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter

from modules.webhooks.receiver.router import router as receiver_router
from modules.webhooks.whatapi.router import router as whatapi_router

router = APIRouter()
router.include_router(receiver_router)
router.include_router(whatapi_router)
