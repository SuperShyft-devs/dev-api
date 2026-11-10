"""Shared notification test defaults."""

from __future__ import annotations

import pytest

from core.config import settings


@pytest.fixture(autouse=True)
def _notification_tests_allow_n8n_fallback(monkeypatch):
    """Legacy SQL fixtures still use webhook_path until each test seeds direct content."""
    monkeypatch.setattr(settings, "NOTIFICATION_ALLOW_N8N_FALLBACK", True)
