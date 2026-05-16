"""Notifications module dependencies."""

from __future__ import annotations

from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService


def get_notifications_service() -> NotificationsService:
    return NotificationsService(repository=NotificationsRepository())
