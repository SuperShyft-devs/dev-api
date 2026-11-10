"""Deprecated — use notifications catalog/bindings API."""

from __future__ import annotations

from core.exceptions import AppError


class NotificationEventsService:
    def _gone(self) -> None:
        raise AppError(
            status_code=410,
            error_code="GONE",
            message="Use GET /notifications/catalog/events",
        )
