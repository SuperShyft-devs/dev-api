"""Tests for BioAI report generation guard on MetsightsService."""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from core.config import settings
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService


@pytest.mark.asyncio
async def test_is_bioai_report_generated_returns_true_on_200(monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    async def _ok(self, *, record_id: str, assessment_type_code: str | None):
        return {"detail": "ok", "data": {"id": record_id}}

    monkeypatch.setattr(MetsightsClient, "get_report", _ok)
    svc = MetsightsService(client=MetsightsClient())
    assert await svc.is_bioai_report_generated(record_id="ABC123", assessment_type_code="1") is True


@pytest.mark.asyncio
async def test_is_bioai_report_generated_returns_false_on_404(monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    async def _not_found(self, *, record_id: str, assessment_type_code: str | None):
        response = MagicMock()
        response.status_code = 404
        raise httpx.HTTPStatusError("not found", request=MagicMock(), response=response)

    monkeypatch.setattr(MetsightsClient, "get_report", _not_found)
    svc = MetsightsService(client=MetsightsClient())
    assert await svc.is_bioai_report_generated(record_id="ABC123", assessment_type_code="2") is False


@pytest.mark.asyncio
async def test_is_bioai_report_generated_raises_on_403(monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    async def _forbidden(self, *, record_id: str, assessment_type_code: str | None):
        response = MagicMock()
        response.status_code = 403
        raise httpx.HTTPStatusError("forbidden", request=MagicMock(), response=response)

    monkeypatch.setattr(MetsightsClient, "get_report", _forbidden)
    svc = MetsightsService(client=MetsightsClient())

    from core.exceptions import AppError

    with pytest.raises(AppError) as exc_info:
        await svc.is_bioai_report_generated(record_id="ABC123", assessment_type_code="1")
    assert exc_info.value.error_code == "EXTERNAL_SERVICE_UNAVAILABLE"
