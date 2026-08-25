"""Unit tests for bio-ai-reports PDF registration."""

from __future__ import annotations

import pytest

from modules.bioai_report.pdf_registration import (
    extract_registered_report_url,
    summarize_bioreport_payload,
)


def test_summarize_bioreport_payload():
    payload = {
        "patient": {"name": "Jane Doe"},
        "report_metadata": {
            "record_id": "REC123",
            "disease_count": 4,
            "engine_version": "1.0.0",
        },
    }
    summary = summarize_bioreport_payload(payload)
    assert summary == {
        "record_id": "REC123",
        "patient_name": "Jane Doe",
        "disease_count": 4,
        "engine_version": "1.0.0",
        "truncated": True,
    }


def test_extract_registered_report_url_success():
    assert extract_registered_report_url({"url": "https://bio-ai-reports.supershyft.com/r/abc"}) == (
        "https://bio-ai-reports.supershyft.com/r/abc"
    )


def test_extract_registered_report_url_missing():
    with pytest.raises(Exception) as exc:
        extract_registered_report_url({"slug": "abc"})
    assert exc.value.error_code == "BIO_AI_REPORTS_ERROR"
