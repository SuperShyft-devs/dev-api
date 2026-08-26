"""Unit tests for health-span-index waist measurement extraction."""

from __future__ import annotations

from modules.reports.service import ReportsService


def test_normalize_waist_unit():
    assert ReportsService._normalize_waist_unit("0") == "cm"
    assert ReportsService._normalize_waist_unit("cm") == "cm"
    assert ReportsService._normalize_waist_unit("1") == "in"
    assert ReportsService._normalize_waist_unit("in") == "in"
    assert ReportsService._normalize_waist_unit("inch") == "in"
    assert ReportsService._normalize_waist_unit("inches") == "in"
    assert ReportsService._normalize_waist_unit(None) is None
    assert ReportsService._normalize_waist_unit("mm") is None


def test_extract_waist_measurement_inches():
    lookup = {"waist_circumference": {"value": 30, "unit": "1"}}
    waist = ReportsService._extract_waist_measurement(lookup)
    assert waist is not None
    assert waist.value == 30
    assert waist.unit == "in"


def test_extract_waist_measurement_cm():
    lookup = {"waist_circumference": {"value": 76, "unit": "0"}}
    waist = ReportsService._extract_waist_measurement(lookup)
    assert waist is not None
    assert waist.value == 76
    assert waist.unit == "cm"


def test_extract_waist_measurement_cm_label():
    lookup = {"waist_circumference": {"value": 80.5, "unit": "cm"}}
    waist = ReportsService._extract_waist_measurement(lookup)
    assert waist is not None
    assert waist.value == 80.5
    assert waist.unit == "cm"


def test_extract_waist_measurement_missing():
    assert ReportsService._extract_waist_measurement({}) is None
    assert ReportsService._extract_waist_measurement({"waist_circumference": {"unit": "1"}}) is None
