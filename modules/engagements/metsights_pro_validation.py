"""Validate Metsights Pro against diagnostic package hormone parameters."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.repository import AssessmentsRepository
from modules.diagnostics.repository import DiagnosticsRepository
from modules.engagement_types.repository import EngagementTypesRepository

METSIGHTS_PRO_PACKAGE_CODE = "METSIGHTS_PRO"

BIO_AI_ENGAGEMENT_TYPE_CODES = frozenset({"bio_ai", "bio_ai_with_consultation"})

# Satisfied if any alias (case-insensitive) is present on the diagnostic package.
_METSIGHTS_PRO_HORMONE_GROUPS: tuple[tuple[str, str], ...] = (
    (("lh", "lh_value"), "LH"),
    (("fsh", "fsh_value"), "FSH"),
    (("total_testosterone", "testosterone"), "total testosterone"),
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

    diag_id = resolve_diagnostic_package_id_for_pro_gate(
        diagnostic_package_id=diagnostic_package_id,
        diagnostic_package_id_female=diagnostic_package_id_female,
    )
    if diag_id is None:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message=(
                "Metsights Pro requires a diagnostic package that includes LH, FSH, and total testosterone. "
                "Select a diagnostic package first."
            ),
        )

    diagnostics_repo = diagnostics_repository or DiagnosticsRepository()
    keys = await diagnostics_repo.list_distinct_parameter_keys_for_package(db, package_id=diag_id)
    missing = missing_metsights_pro_hormone_labels(keys)
    if missing:
        joined = ", ".join(missing)
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message=(
                f"Metsights Pro requires LH, FSH, and total testosterone on the diagnostic package "
                f"used for women. Missing: {joined}."
            ),
        )


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
    code = (engagement_type_code or "").strip().lower()
    if code not in BIO_AI_ENGAGEMENT_TYPE_CODES:
        return

    types_repo = engagement_types_repository or EngagementTypesRepository()
    engagement_type = await types_repo.get_by_code(db, code)
    if engagement_type is None:
        return

    await ensure_metsights_pro_allowed_for_engagement(
        db,
        engagement_type_id=int(engagement_type.id),
        assessment_package_id=assessment_package_id,
        diagnostic_package_id=diagnostic_package_id,
        diagnostic_package_id_male=diagnostic_package_id_male,
        diagnostic_package_id_female=diagnostic_package_id_female,
        engagement_types_repository=types_repo,
    )
