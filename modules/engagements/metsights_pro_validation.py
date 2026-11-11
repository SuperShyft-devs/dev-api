"""Validate Metsights Pro against diagnostic package hormone parameters."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.repository import AssessmentsRepository
from modules.diagnostics.repository import DiagnosticsRepository
from modules.engagement_types.repository import EngagementTypesRepository

logger = logging.getLogger(__name__)

METSIGHTS_PRO_PACKAGE_CODE = "METSIGHTS_PRO"
METSIGHTS_ESSENTIALS_PACKAGE_CODES = ("METSIGHTS_BASIC", "METSIGHTS_ESSENTIALS")

BIO_AI_ENGAGEMENT_TYPE_CODES = frozenset({"bio_ai", "bio_ai_with_consultation"})

# Satisfied if any alias (case-insensitive) is present on the diagnostic package.
_METSIGHTS_PRO_HORMONE_GROUPS: tuple[tuple[str, str], ...] = (
    (("lh", "lh_value"), "LH"),
    (("fsh", "fsh_value"), "FSH"),
    (("total_testosterone", "testosterone"), "total testosterone"),
)


def format_metsights_pro_hormone_violation(
    *,
    engagement_type_label: str,
    diagnostic_package_name: str,
    missing: list[str],
) -> str:
    joined = ", ".join(missing)
    return (
        f"{engagement_type_label} uses {diagnostic_package_name}, "
        f"which is missing {joined} required by Metsights Pro."
    )


def missing_metsights_pro_hormone_labels(parameter_keys: set[str]) -> list[str]:
    normalized = {k.strip().lower() for k in parameter_keys if k and str(k).strip()}
    missing: list[str] = []
    for aliases, label in _METSIGHTS_PRO_HORMONE_GROUPS:
        if not any(alias in normalized for alias in aliases):
            missing.append(label)
    return missing


def resolve_diagnostic_package_id_for_pro_gate(
    *,
    diagnostic_package_id: int | None,
    diagnostic_package_id_female: int | None,
) -> int | None:
    if diagnostic_package_id_female is not None:
        return int(diagnostic_package_id_female)
    if diagnostic_package_id is not None:
        return int(diagnostic_package_id)
    return None


async def ensure_metsights_pro_allowed_for_engagement(
    db: AsyncSession,
    *,
    engagement_type_id: int,
    assessment_package_id: int | None,
    diagnostic_package_id: int | None,
    diagnostic_package_id_male: int | None,
    diagnostic_package_id_female: int | None,
    engagement_types_repository: EngagementTypesRepository | None = None,
    assessments_repository: AssessmentsRepository | None = None,
    diagnostics_repository: DiagnosticsRepository | None = None,
) -> None:
    """Raise AppError when Metsights Pro is selected without required diagnostic hormones."""
    if assessment_package_id is None:
        return

    assessments_repo = assessments_repository or AssessmentsRepository()
    package = await assessments_repo.get_package_by_id(db, package_id=int(assessment_package_id))
    if package is None:
        return
    if (package.package_code or "").strip() != METSIGHTS_PRO_PACKAGE_CODE:
        return

    types_repo = engagement_types_repository or EngagementTypesRepository()
    engagement_type = await types_repo.get_by_id(db, int(engagement_type_id))
    if engagement_type is None:
        return
    type_code = (engagement_type.code or "").strip().lower()
    if type_code not in BIO_AI_ENGAGEMENT_TYPE_CODES:
        return

    type_label = (engagement_type.display_name or engagement_type.code or type_code).strip()

    diag_id = resolve_diagnostic_package_id_for_pro_gate(
        diagnostic_package_id=diagnostic_package_id,
        diagnostic_package_id_female=diagnostic_package_id_female,
    )
    if diag_id is None:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message=(
                f"{type_label}: Metsights Pro requires a diagnostic package that includes "
                "LH, FSH, and total testosterone. Select a diagnostic package first."
            ),
        )

    diagnostics_repo = diagnostics_repository or DiagnosticsRepository()
    diag_row = await diagnostics_repo.get_package_by_id_basic(db, package_id=int(diag_id))
    diag_name = (diag_row.package_name if diag_row else None) or f"diagnostic package #{diag_id}"

    keys = await diagnostics_repo.list_distinct_parameter_keys_for_package(db, package_id=diag_id)
    missing = missing_metsights_pro_hormone_labels(keys)
    if missing:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message=format_metsights_pro_hormone_violation(
                engagement_type_label=type_label,
                diagnostic_package_name=diag_name,
                missing=missing,
            ),
        )


async def resolve_active_metsights_essentials_package_id(
    db: AsyncSession,
    *,
    assessments_repository: AssessmentsRepository | None = None,
) -> int | None:
    """Active MetSights Essentials (Basic) package_id, if configured."""
    repo = assessments_repository or AssessmentsRepository()
    for code in METSIGHTS_ESSENTIALS_PACKAGE_CODES:
        pkg = await repo.get_package_by_code(db, package_code=code)
        if pkg is None:
            continue
        if (pkg.status or "").strip().lower() != "active":
            continue
        return int(pkg.package_id)
    return None


async def coerce_b2c_assessment_away_from_pro_when_ineligible(
    db: AsyncSession,
    *,
    engagement_type_id: int | None,
    assessment_package_id: int | None,
    diagnostic_package_id: int | None,
    diagnostic_package_id_male: int | None,
    diagnostic_package_id_female: int | None,
    engagement_types_repository: EngagementTypesRepository | None = None,
    assessments_repository: AssessmentsRepository | None = None,
) -> int | None:
    """Use MetSights Essentials when B2C defaults select Pro but the diagnostic lacks hormones."""
    if assessment_package_id is None or engagement_type_id is None:
        return assessment_package_id

    types_repo = engagement_types_repository or EngagementTypesRepository()
    engagement_type = await types_repo.get_by_id(db, int(engagement_type_id))
    if engagement_type is None:
        return assessment_package_id
    type_code = (engagement_type.code or "").strip().lower()
    if type_code not in BIO_AI_ENGAGEMENT_TYPE_CODES:
        return assessment_package_id

    assessments_repo = assessments_repository or AssessmentsRepository()
    package = await assessments_repo.get_package_by_id(db, package_id=int(assessment_package_id))
    if package is None:
        return assessment_package_id
    if (package.package_code or "").strip() != METSIGHTS_PRO_PACKAGE_CODE:
        return assessment_package_id

    violation = await metsights_pro_b2c_defaults_violation(
        db,
        engagement_type_code=type_code,
        assessment_package_id=int(assessment_package_id),
        diagnostic_package_id=diagnostic_package_id,
        diagnostic_package_id_male=diagnostic_package_id_male,
        diagnostic_package_id_female=diagnostic_package_id_female,
        engagement_types_repository=types_repo,
    )
    if not violation:
        return assessment_package_id

    essentials_id = await resolve_active_metsights_essentials_package_id(
        db, assessments_repository=assessments_repo
    )
    if essentials_id is None:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message=violation)

    logger.info(
        "B2C onboarding: Metsights Pro ineligible (%s); using MetSights Essentials package_id=%s",
        violation,
        essentials_id,
    )
    return essentials_id


async def metsights_pro_b2c_defaults_violation(
    db: AsyncSession,
    *,
    engagement_type_code: str,
    assessment_package_id: int,
    diagnostic_package_id: int | None,
    diagnostic_package_id_male: int | None,
    diagnostic_package_id_female: int | None,
    engagement_types_repository: EngagementTypesRepository | None = None,
) -> str | None:
    """Return a user-facing violation message, or None when defaults are valid."""
    code = (engagement_type_code or "").strip().lower()
    if code not in BIO_AI_ENGAGEMENT_TYPE_CODES:
        return None

    types_repo = engagement_types_repository or EngagementTypesRepository()
    engagement_type = await types_repo.get_by_code(db, code)
    if engagement_type is None:
        return None

    try:
        await ensure_metsights_pro_allowed_for_engagement(
            db,
            engagement_type_id=int(engagement_type.id),
            assessment_package_id=assessment_package_id,
            diagnostic_package_id=diagnostic_package_id,
            diagnostic_package_id_male=diagnostic_package_id_male,
            diagnostic_package_id_female=diagnostic_package_id_female,
            engagement_types_repository=types_repo,
        )
    except AppError as exc:
        if exc.error_code == "INVALID_INPUT":
            return str(exc.message)
        raise
    return None


async def ensure_metsights_pro_allowed_for_b2c_type_defaults(
    db: AsyncSession,
    *,
    engagement_type_code: str,
    assessment_package_id: int,
    diagnostic_package_id: int | None,
    diagnostic_package_id_male: int | None,
    diagnostic_package_id_female: int | None,
    engagement_types_repository: EngagementTypesRepository | None = None,
) -> None:
    """Validate Metsights Pro against diagnostic defaults for a B2C onboarding type."""
    message = await metsights_pro_b2c_defaults_violation(
        db,
        engagement_type_code=engagement_type_code,
        assessment_package_id=assessment_package_id,
        diagnostic_package_id=diagnostic_package_id,
        diagnostic_package_id_male=diagnostic_package_id_male,
        diagnostic_package_id_female=diagnostic_package_id_female,
        engagement_types_repository=engagement_types_repository,
    )
    if message:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message=message)


async def ensure_metsights_pro_allowed_before_b2c_draft_pay(
    db: AsyncSession,
    *,
    engagement: Any,
) -> None:
    """Block Razorpay checkout when post-payment defaults would fail the Pro hormone gate."""
    if not getattr(engagement, "enforce_metsights_pro_gate", False):
        return

    types_repo = EngagementTypesRepository()
    engagement_type_id = getattr(engagement, "engagement_type", None)
    type_code = "bio_ai"
    if engagement_type_id is None:
        bio_ai_row = await types_repo.get_by_code(db, "bio_ai")
        if bio_ai_row is None:
            return
        engagement_type_id = int(bio_ai_row.id)
    else:
        from modules.engagement_types.models import EngagementType

        type_row = await db.execute(
            select(EngagementType.code).where(EngagementType.id == int(engagement_type_id)).limit(1)
        )
        type_code = (type_row.scalar_one_or_none() or "bio_ai").strip() or "bio_ai"

    resolved_assessment = getattr(engagement, "assessment_package_id", None)
    if resolved_assessment is None:
        from modules.platform_settings.service import PlatformSettingsService

        settings_service = PlatformSettingsService()
        defaults = await settings_service.resolve_b2c_onboarding_defaults(db, type_code)
        resolved_assessment = defaults.assessment_package_id

    resolved_assessment = await coerce_b2c_assessment_away_from_pro_when_ineligible(
        db,
        engagement_type_id=int(engagement_type_id),
        assessment_package_id=int(resolved_assessment) if resolved_assessment is not None else None,
        diagnostic_package_id=getattr(engagement, "diagnostic_package_id", None),
        diagnostic_package_id_male=getattr(engagement, "diagnostic_package_id_male", None),
        diagnostic_package_id_female=getattr(engagement, "diagnostic_package_id_female", None),
    )
    if resolved_assessment is not None:
        engagement.assessment_package_id = int(resolved_assessment)

    await ensure_metsights_pro_allowed_for_engagement(
        db,
        engagement_type_id=int(engagement_type_id),
        assessment_package_id=int(resolved_assessment),
        diagnostic_package_id=getattr(engagement, "diagnostic_package_id", None),
        diagnostic_package_id_male=getattr(engagement, "diagnostic_package_id_male", None),
        diagnostic_package_id_female=getattr(engagement, "diagnostic_package_id_female", None),
    )
