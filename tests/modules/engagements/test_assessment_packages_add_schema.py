"""Regression: assign/sync must not lowercase package_code via SlugKey."""

from __future__ import annotations

from modules.engagements.schemas import EngagementAssessmentPackageAddRequest


def test_add_request_preserves_uppercase_package_code():
    req = EngagementAssessmentPackageAddRequest(package_code="MY_FITNESS_PRINT")
    assert req.package_code == "MY_FITNESS_PRINT"


def test_add_request_preserves_metsights_pro_code():
    req = EngagementAssessmentPackageAddRequest(package_code="METSIGHTS_PRO")
    assert req.package_code == "METSIGHTS_PRO"


def test_add_request_strips_whitespace_only():
    req = EngagementAssessmentPackageAddRequest(package_code="  MY_FITNESS_PRINT  ")
    assert req.package_code == "MY_FITNESS_PRINT"
