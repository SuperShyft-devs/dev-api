from __future__ import annotations

import pytest
from sqlalchemy import text

from core.exceptions import AppError
from modules.engagements.metsights_pro_validation import (
    missing_metsights_pro_hormone_labels,
    ensure_metsights_pro_allowed_for_engagement,
    ensure_metsights_pro_allowed_for_b2c_type_defaults,
    resolve_diagnostic_package_id_for_pro_gate,
)


def test_missing_metsights_pro_hormone_labels_all_present():
    keys = {"lh", "fsh", "total_testosterone", "glucose_fasting"}
    assert missing_metsights_pro_hormone_labels(keys) == []


def test_missing_metsights_pro_hormone_labels_aliases():
    keys = {"lh_value", "fsh_value", "testosterone"}
    assert missing_metsights_pro_hormone_labels(keys) == []


def test_missing_metsights_pro_hormone_labels_partial():
    keys = {"lh", "fsh"}
    assert missing_metsights_pro_hormone_labels(keys) == ["total testosterone"]


def test_resolve_diagnostic_package_id_for_pro_gate_prefers_female():
    assert (
        resolve_diagnostic_package_id_for_pro_gate(
            diagnostic_package_id=10,
            diagnostic_package_id_female=20,
        )
        == 20
    )


@pytest.mark.asyncio
async def test_ensure_metsights_pro_rejects_missing_hormones(test_db_session):
    bio_ai_type_id = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'bio_ai' LIMIT 1")
        )
    ).scalar_one()
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (902, 'METSIGHTS_PRO', 'Metsights Pro', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, "
            "diagnostic_provider, status, bookings_count) "
            "VALUES (903, 'REF903', 'No hormones', 'healthians', 1, 0) "
            "ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.commit()

    with pytest.raises(AppError) as exc:
        await ensure_metsights_pro_allowed_for_engagement(
            test_db_session,
            engagement_type_id=int(bio_ai_type_id),
            assessment_package_id=902,
            diagnostic_package_id=903,
            diagnostic_package_id_male=None,
            diagnostic_package_id_female=None,
        )
    assert exc.value.status_code == 400
    assert "missing" in exc.value.message.lower()


@pytest.mark.asyncio
async def test_ensure_metsights_pro_allows_basic_without_hormones(test_db_session):
    bio_ai_type_id = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'bio_ai' LIMIT 1")
        )
    ).scalar_one()
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (912, 'METSIGHTS_BASIC', 'Metsights Basic', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, "
            "diagnostic_provider, status, bookings_count) "
            "VALUES (913, 'REF913', 'No hormones', 'healthians', 1, 0) "
            "ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.commit()

    await ensure_metsights_pro_allowed_for_engagement(
        test_db_session,
        engagement_type_id=int(bio_ai_type_id),
        assessment_package_id=912,
        diagnostic_package_id=913,
        diagnostic_package_id_male=None,
        diagnostic_package_id_female=None,
    )


@pytest.mark.asyncio
async def test_ensure_metsights_pro_b2c_defaults_rejects_pro_without_hormones(test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (922, 'METSIGHTS_PRO', 'Metsights Pro', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, "
            "diagnostic_provider, status, bookings_count) "
            "VALUES (923, 'REF923', 'No hormones', 'healthians', 1, 0) "
            "ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.commit()

    with pytest.raises(AppError) as exc:
        await ensure_metsights_pro_allowed_for_b2c_type_defaults(
            test_db_session,
            engagement_type_code="bio_ai",
            assessment_package_id=922,
            diagnostic_package_id=923,
            diagnostic_package_id_male=None,
            diagnostic_package_id_female=None,
        )
    assert exc.value.status_code == 400
