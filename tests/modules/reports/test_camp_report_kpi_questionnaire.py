"""Unit tests for camp-report KPI questionnaire completion rules."""

from __future__ import annotations

from modules.reports.camp_reports_repository import (
    _KPI_QUESTIONNAIRE_REQUIRED_CATEGORY_KEYS,
    _METSIGHTS_CATEGORY_KEYS,
)


def test_kpi_required_category_keys_exclude_vitals():
    assert "vitals" in _METSIGHTS_CATEGORY_KEYS
    assert "vitals" not in _KPI_QUESTIONNAIRE_REQUIRED_CATEGORY_KEYS
    assert "physical-measurement" in _KPI_QUESTIONNAIRE_REQUIRED_CATEGORY_KEYS
    assert "diet-lifestyle-parameters" in _KPI_QUESTIONNAIRE_REQUIRED_CATEGORY_KEYS
    assert "fitness-parameters" in _KPI_QUESTIONNAIRE_REQUIRED_CATEGORY_KEYS


def test_kpi_questionnaire_completed_ignores_incomplete_vitals():
    """Mirror the completion loop: only required keys block questionnaire_completed."""
    assigned_cats = {"physical-measurement", "vitals"}
    status_by_cat = {
        "physical-measurement": "complete",
        "vitals": "incomplete",
    }

    all_assigned_complete = True
    for ck in _KPI_QUESTIONNAIRE_REQUIRED_CATEGORY_KEYS:
        if ck not in assigned_cats:
            continue
        if status_by_cat.get(ck) != "complete":
            all_assigned_complete = False

    assert all_assigned_complete is True


def test_kpi_questionnaire_not_completed_when_required_category_incomplete():
    assigned_cats = {"physical-measurement", "vitals"}
    status_by_cat = {
        "physical-measurement": "incomplete",
        "vitals": "incomplete",
    }

    all_assigned_complete = True
    for ck in _KPI_QUESTIONNAIRE_REQUIRED_CATEGORY_KEYS:
        if ck not in assigned_cats:
            continue
        if status_by_cat.get(ck) != "complete":
            all_assigned_complete = False

    assert all_assigned_complete is False
