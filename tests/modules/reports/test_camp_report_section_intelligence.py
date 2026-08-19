"""Unit tests for single-section camp intelligence generation."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from modules.reports.camp_report_intelligence.intelligence_src.assembly import (
    generate_camp_section_intelligence,
    resolve_intelligence_section,
)
from modules.reports.camp_report_intelligence.intelligence_src.engine import (
    generate_insight,
    generate_section_insights,
)

_SAMPLE = Path("modules/reports/camp_report_intelligence/sample_camp_report.json")


def test_resolve_intelligence_section_aliases():
    assert resolve_intelligence_section("sleep") == "distribution_by_sleeping_hours"
    assert resolve_intelligence_section("distribution_by_sleeping_hours") == (
        "distribution_by_sleeping_hours"
    )


def test_resolve_intelligence_section_invalid():
    try:
        resolve_intelligence_section("does_not_exist")
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def test_generate_section_insights_does_not_run_other_sections():
    report = json.loads(_SAMPLE.read_text())
    with patch(
        "modules.reports.camp_report_intelligence.intelligence_src.engine.generate_insight",
        wraps=generate_insight,
    ) as mocked:
        payload = generate_section_insights(report, "sleep")
    assert set(payload["concerns"].keys()) == {"sleep"}
    section_ids = [call.args[0] for call in mocked.call_args_list]
    assert section_ids
    assert all(section_id == "sleep" for section_id in section_ids)


def test_generate_camp_section_intelligence_sleep_from_sample():
    report = json.loads(_SAMPLE.read_text())
    camp_key, intel = generate_camp_section_intelligence(report, "sleep")
    assert camp_key == "distribution_by_sleeping_hours"
    assert "both" in intel
    assert {"tone", "observation", "explanation", "recommendation"} <= set(intel["both"].keys())
