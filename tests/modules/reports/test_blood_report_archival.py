"""Unit tests for blood report PDF archival."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from core.config import settings
from modules.reports.blood_report_archival import (
    archive_blood_report_pdf,
    generate_blood_report_filename,
    is_archived_blood_report_url,
    resolve_persistable_diagnostic_report_url,
)

_MIN_PDF = b"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"
_ARCHIVED_URL = "https://supershyft.com/reports/AbCdEfGhIjKlMnOp.pdf"
_HEALTHIANS_URL = "https://healthians.com/signed/report.pdf"


@pytest.fixture
def blood_reports_paths(tmp_path, monkeypatch):
    root = tmp_path / "reports"
    root.mkdir()
    monkeypatch.setattr(settings, "BLOOD_REPORTS_ROOT", str(root))
    monkeypatch.setattr(settings, "BLOOD_REPORTS_BASE_URL", "https://supershyft.com/reports")
    monkeypatch.setattr(settings, "BLOOD_REPORTS_TIMEOUT_SECONDS", 5)
    monkeypatch.setattr(settings, "BLOOD_REPORTS_MAX_MB", 5)
    return root


def test_generate_blood_report_filename_length_and_charset():
    filename = generate_blood_report_filename()
    assert len(filename) == 16
    assert filename.isalnum()


def test_is_archived_blood_report_url():
    assert is_archived_blood_report_url(_ARCHIVED_URL) is True
    assert is_archived_blood_report_url(_HEALTHIANS_URL) is False
    assert is_archived_blood_report_url(None) is False
    assert is_archived_blood_report_url("https://supershyft.com/reports/short.pdf") is False


@pytest.mark.asyncio
async def test_archive_blood_report_pdf_success(blood_reports_paths, monkeypatch):
    async def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_MIN_PDF)

    transport = httpx.MockTransport(_handler)
    original_client = httpx.AsyncClient

    def _client(*args, **kwargs):
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr("modules.reports.blood_report_archival.httpx.AsyncClient", _client)

    url = await archive_blood_report_pdf(_HEALTHIANS_URL, assessment_instance_id=12345)
    assert url.startswith("https://supershyft.com/reports/")
    assert url.endswith(".pdf")
    filename = url.rsplit("/", 1)[-1]
    assert (blood_reports_paths / filename).is_file()
    assert (blood_reports_paths / filename).read_bytes().startswith(b"%PDF")


@pytest.mark.asyncio
async def test_archive_blood_report_pdf_rejects_non_pdf(blood_reports_paths, monkeypatch):
    async def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not-a-pdf")

    transport = httpx.MockTransport(_handler)
    original_client = httpx.AsyncClient

    def _client(*args, **kwargs):
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr("modules.reports.blood_report_archival.httpx.AsyncClient", _client)

    with pytest.raises(ValueError, match="not a PDF"):
        await archive_blood_report_pdf(_HEALTHIANS_URL, assessment_instance_id=12345)


@pytest.mark.asyncio
async def test_resolve_persistable_diagnostic_report_url_partial_uses_healthians():
    url = await resolve_persistable_diagnostic_report_url(
        _HEALTHIANS_URL,
        is_full_report=False,
        existing_url=None,
        assessment_instance_id=1,
    )
    assert url == _HEALTHIANS_URL


@pytest.mark.asyncio
async def test_resolve_persistable_diagnostic_report_url_full_archives(blood_reports_paths, monkeypatch):
    async def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_MIN_PDF)

    transport = httpx.MockTransport(_handler)
    original_client = httpx.AsyncClient

    def _client(*args, **kwargs):
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr("modules.reports.blood_report_archival.httpx.AsyncClient", _client)

    url = await resolve_persistable_diagnostic_report_url(
        _HEALTHIANS_URL,
        is_full_report=True,
        existing_url=None,
        assessment_instance_id=99,
    )
    assert is_archived_blood_report_url(url)
    assert Path(blood_reports_paths / url.rsplit("/", 1)[-1]).is_file()


@pytest.mark.asyncio
async def test_resolve_persistable_diagnostic_report_url_reuses_existing_archived():
    url = await resolve_persistable_diagnostic_report_url(
        _HEALTHIANS_URL,
        is_full_report=True,
        existing_url=_ARCHIVED_URL,
        assessment_instance_id=1,
    )
    assert url == _ARCHIVED_URL


@pytest.mark.asyncio
async def test_resolve_persistable_diagnostic_report_url_returns_none_on_failure(monkeypatch):
    async def _fail_archive(*args, **kwargs):
        raise RuntimeError("download failed")

    monkeypatch.setattr(
        "modules.reports.blood_report_archival.archive_blood_report_pdf",
        _fail_archive,
    )

    url = await resolve_persistable_diagnostic_report_url(
        _HEALTHIANS_URL,
        is_full_report=True,
        existing_url=None,
        assessment_instance_id=1,
    )
    assert url is None
