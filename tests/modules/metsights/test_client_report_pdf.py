"""Tests for MetsightsClient report PDF routing and fallbacks."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from core.config import settings
from modules.metsights.client import MetsightsClient


@pytest.mark.asyncio
async def test_get_report_pdf_fitprint_falls_back_when_fitness_pdf_unauthorized(monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    monkeypatch.setattr(settings, "METSIGHTS_BASE_URL", "https://api.metsights.com")
    monkeypatch.setattr(settings, "METSIGHTS_TIMEOUT_SECONDS", 5)

    client = MetsightsClient()
    calls: list[str] = []

    async def _fake_get(self, url, headers=None):
        calls.append(url)
        response = MagicMock()
        response.status_code = 401 if "fitness-reports" in url else 200
        if response.status_code != 200:
            raise httpx.HTTPStatusError("unauthorized", request=MagicMock(), response=response)
        response.json.return_value = {
            "detail": "PDF file for Metabolic Health Report",
            "data": {
                "id": "FITPRINT01",
                "file": "https://storages.metsights.com/reports/example.pdf",
            },
        }
        return response

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.get.side_effect = _fake_get
        mock_client_cls.return_value = mock_client

        payload = await client.get_report_pdf(record_id="FITPRINT01", assessment_type_code="7")

    assert len(calls) == 2
    assert calls[0].endswith("/reports/fitness-reports/FITPRINT01/pdf/")
    assert calls[1].endswith("/reports/FITPRINT01/pdf/")
    assert payload["data"]["file"].endswith("example.pdf")


@pytest.mark.asyncio
async def test_get_report_pdf_metabolic_uses_reports_path_only(monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    monkeypatch.setattr(settings, "METSIGHTS_BASE_URL", "https://api.metsights.com")
    monkeypatch.setattr(settings, "METSIGHTS_TIMEOUT_SECONDS", 5)

    client = MetsightsClient()
    calls: list[str] = []

    async def _fake_get(self, url, headers=None):
        calls.append(url)
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {
            "detail": "ok",
            "data": {"id": "METPRO01", "file": "https://example.com/met.pdf"},
        }
        return response

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.get.side_effect = _fake_get
        mock_client_cls.return_value = mock_client

        await client.get_report_pdf(record_id="METPRO01", assessment_type_code="2")

    assert len(calls) == 1
    assert calls[0].endswith("/reports/METPRO01/pdf/")
