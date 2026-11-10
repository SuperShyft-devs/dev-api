"""Unit tests for notification binding lookup by id."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from core.exceptions import AppError
from modules.notifications.bindings_service import NotificationBindingsService


@pytest.mark.asyncio
async def test_get_binding_missing_raises():
    service = NotificationBindingsService()
    service._repo.get_by_id = AsyncMock(return_value=None)
    db = MagicMock()
    with pytest.raises(AppError) as exc:
        await service.get_binding(db, 999)
    assert exc.value.status_code == 404
