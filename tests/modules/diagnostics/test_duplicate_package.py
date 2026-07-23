"""Tests for POST /diagnostic-packages/{package_id}/duplicate."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from modules.diagnostics.models import (
    DiagnosticPackage,
    DiagnosticPackageReason,
    DiagnosticPackageSample,
    DiagnosticPackageTag,
    DiagnosticPackageTestGroup,
    DiagnosticPackagePreparation,
)
from modules.diagnostics.repository import DiagnosticsRepository
from modules.diagnostics.service import DiagnosticsService


@pytest.mark.asyncio
async def test_duplicate_diagnostic_package_repository(test_db_session: AsyncSession):
    repo = DiagnosticsRepository()

    # Create base diagnostic package
    pkg = DiagnosticPackage(
        reference_id="REF_DUP_TEST",
        package_name="Original Package",
        package_image="http://example.com/pkg.jpg",
        diagnostic_provider="Healthians",
        report_duration_hours=24,
        collection_type="home_collection",
        health_areas_covered="Whole Body",
        about_text="Test Description",
        bookings_count=10,
        price=1500.00,
        original_price=2000.00,
        is_most_popular=True,
        gender_suitability="both",
        status="active",
        package_for="public",
    )
    repo_pkg = await repo.create_package(test_db_session, pkg)
    pkg_id = repo_pkg.diagnostic_package_id

    # Add child rows
    reason = DiagnosticPackageReason(
        diagnostic_package_id=pkg_id,
        reason_text="Routine Health Checkup",
        display_order=1,
    )
    tag = DiagnosticPackageTag(
        diagnostic_package_id=pkg_id,
        tag_name="Popular",
        display_order=1,
    )
    sample = DiagnosticPackageSample(
        diagnostic_package_id=pkg_id,
        sample_type="Blood",
        description="Fasting sample",
        display_order=1,
    )
    prep = DiagnosticPackagePreparation(
        diagnostic_package_id=pkg_id,
        preparation_title="10-12 hours fasting required",
        steps=["Do not eat", "Drink water"],
        display_order=1,
    )
    test_db_session.add_all([reason, tag, sample, prep])
    await test_db_session.flush()

    # Duplicate package via repository
    duplicated = await repo.duplicate_package(test_db_session, package_id=pkg_id)

    assert duplicated is not None
    assert duplicated.diagnostic_package_id != pkg_id
    assert duplicated.package_name == "Original Package copy"
    assert duplicated.bookings_count == 0
    assert float(duplicated.price) == 1500.00
    assert duplicated.diagnostic_provider == "Healthians"

    # Verify relations duplicated
    assert len(duplicated.reasons) == 1
    assert duplicated.reasons[0].reason_text == "Routine Health Checkup"

    assert len(duplicated.tags) == 1
    assert duplicated.tags[0].tag_name == "Popular"

    assert len(duplicated.samples) == 1
    assert duplicated.samples[0].sample_type == "Blood"

    assert len(duplicated.preparations) == 1
    assert duplicated.preparations[0].preparation_title == "10-12 hours fasting required"


@pytest.mark.asyncio
async def test_duplicate_diagnostic_package_not_found(test_db_session: AsyncSession):
    repo = DiagnosticsRepository()
    duplicated = await repo.duplicate_package(test_db_session, package_id=999999)
    assert duplicated is None
