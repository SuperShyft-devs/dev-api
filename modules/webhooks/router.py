"""Webhooks module HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter

from modules.webhooks.receiver.router import router as receiver_router

router = APIRouter()
router.include_router(receiver_router)
