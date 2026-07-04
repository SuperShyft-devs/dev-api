"""Backfill ``blood_parameters`` into package-shaped groups (list of groups → tests).

Converts legacy storage shapes:
- Healthians customer / envelope (``digital_data``)
- Canonical provider map (``parameters`` dict)
- Metsights flat dict (``{parameter_key: value}``)
- Rows with only ``blood_report_raw`` (rebuild groups from raw Healthians)

Already-grouped rows are skipped. Empty / metadata-only blobs are cleared.

Run via::

    python -m db.jobs.migrate_blood_parameters --dry-run
    python -m db.jobs.migrate_blood_parameters --yes
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.seed.blood_parameters_registry import (
    ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
    ALL_BLOOD_PARAMETER_KEYS,
    BLOOD_PARAMETER_CATEGORY_KEY,
    UNITLESS_BLOOD_PARAMETER_KEYS,
)
from modules.diagnostics.repository import DiagnosticsRepository
from modules.diagnostics.service import DiagnosticsService
from modules.engagements.models import Engagement
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireOption,
    QuestionnaireResponse,
)
from modules.reports.blood_parameters_normalizer import (
    build_grouped_from_canonical,
    build_grouped_from_healthians,
    build_grouped_from_metsights_flat,
)
from modules.reports.blood_parameters_schemas import (
    describe_blood_parameters_blob,
    extract_healthians_customer_blob,
    is_canonical_blood_parameters,
    is_empty_blood_parameters,
    is_grouped_blood_parameters,
    is_legacy_healthians_format,
    is_legacy_metsights_flat_format,
    is_metsights_metadata_only,
)
from modules.reports.models import IndividualHealthReport

logger = logging.getLogger(__name__)

_BLOOD_CATEGORY_KEYS = (BLOOD_PARAMETER_CATEGORY_KEY, ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY)


async def migrate_blood_parameters(
    db: AsyncSession,
    *,
    dry_run: bool = False,
    batch_size: int = 200,
) -> dict[str, Any]:
    """Migrate all legacy ``blood_parameters`` rows to package-shaped groups."""
    diagnostics = DiagnosticsService(repository=DiagnosticsRepository())
    question_map = await _load_blood_question_map(db)
    package_groups_cache: dict[int, Any] = {}

    stats: dict[str, Any] = {
        "dry_run": dry_run,
        "scanned": 0,
        "skipped_grouped": 0,
        "migrated_healthians": 0,
        "migrated_canonical": 0,
        "migrated_metsights": 0,
        "migrated_from_raw": 0,
        "cleared_empty": 0,
        "skipped_no_package": 0,
        "skipped_metsights_pending_questionnaire": 0,
        "failed": 0,
        "unmapped_metsights_keys": [],
        "errors": [],
        "questionnaire_definitions_loaded": len(question_map),
    }

    # Cursor pagination avoids skipping rows when we clear blood_parameters to null.
    last_report_id = 0
    while True:
        result = await db.execute(
            select(IndividualHealthReport)
            .where(IndividualHealthReport.report_id > last_report_id)
            .where(
                or_(
                    IndividualHealthReport.blood_parameters.isnot(None),
                    IndividualHealthReport.blood_report_raw.isnot(None),
                )
            )
            .order_by(IndividualHealthReport.report_id.asc())
            .limit(batch_size)
        )
        rows = list(result.scalars().all())
        if not rows:
            break

        for row in rows:
            last_report_id = int(row.report_id)
            stats["scanned"] += 1
            blob = row.blood_parameters
            try:
                if is_grouped_blood_parameters(blob):
                    stats["skipped_grouped"] += 1
                    continue

                package_groups = await _package_groups_for_row(
                    db,
                    diagnostics=diagnostics,
                    row=row,
                    cache=package_groups_cache,
                )
                if package_groups is None:
                    stats["skipped_no_package"] += 1
                    stats["errors"].append(
                        {
                            "report_id": row.report_id,
                            "reason": "engagement has no diagnostic package",
                        }
                    )
                    continue

                # Prefer rebuilding from blood_report_raw when present (authoritative Healthians blob).
                healthians_customer = extract_healthians_customer_blob(row.blood_report_raw)
                if healthians_customer is None:
                    healthians_customer = extract_healthians_customer_blob(blob)

                if healthians_customer is not None:
                    grouped, raw = build_grouped_from_healthians(
                        healthians_customer,
                        package_groups=package_groups,
                    )
                    if not dry_run:
                        row.blood_parameters = grouped
                        row.blood_report_raw = raw
                        db.add(row)
                    if blob is None or is_empty_blood_parameters(blob):
                        stats["migrated_from_raw"] += 1
                    else:
                        stats["migrated_healthians"] += 1
                    continue

                if is_empty_blood_parameters(blob) or is_metsights_metadata_only(blob):
                    if not dry_run:
                        row.blood_parameters = None
                        # Keep blood_report_raw only if it might still be useful; clear when empty too.
                        if is_empty_blood_parameters(row.blood_report_raw):
                            row.blood_report_raw = None
                        db.add(row)
                    stats["cleared_empty"] += 1
                    continue

                if is_legacy_healthians_format(blob):
                    grouped, raw = build_grouped_from_healthians(
                        blob,
                        package_groups=package_groups,
                    )
                    if not dry_run:
                        row.blood_parameters = grouped
                        row.blood_report_raw = raw
                        db.add(row)
                    stats["migrated_healthians"] += 1
                    continue

                if is_canonical_blood_parameters(blob):
                    grouped = build_grouped_from_canonical(blob, package_groups=package_groups)
                    if not dry_run:
                        row.blood_parameters = grouped
                        db.add(row)
                    stats["migrated_canonical"] += 1
                    continue

                if is_legacy_metsights_flat_format(blob):
                    grouped = build_grouped_from_metsights_flat(blob, package_groups=package_groups)
                    if not dry_run:
                        row.blood_parameters = grouped
                        db.add(row)
                    stats["migrated_metsights"] += 1

                    # Best-effort: also copy values into questionnaire responses when possible.
                    if question_map and row.assessment_instance_id is not None:
                        unmapped, _migrated_count = await _migrate_metsights_flat_to_questionnaire(
                            db,
                            row=row,
                            flat=blob,
                            question_map=question_map,
                            dry_run=dry_run,
                            clear_blood_fields=False,
                        )
                        if unmapped:
                            stats["unmapped_metsights_keys"].extend(unmapped)
                    continue

                stats["failed"] += 1
                stats["errors"].append(
                    {
                        "report_id": row.report_id,
                        "reason": f"unknown blood_parameters shape: {describe_blood_parameters_blob(blob)}",
                    }
                )
            except Exception as exc:
                stats["failed"] += 1
                stats["errors"].append(
                    {"report_id": row.report_id, "reason": str(exc)[:500]},
                )
                logger.exception("Failed to migrate report_id=%s", row.report_id)

        if not dry_run:
            await db.flush()

    return stats


async def _package_groups_for_row(
    db: AsyncSession,
    *,
    diagnostics: DiagnosticsService,
    row: IndividualHealthReport,
    cache: dict[int, Any],
) -> Any | None:
    eng_result = await db.execute(
        select(Engagement).where(Engagement.engagement_id == row.engagement_id).limit(1)
    )
    engagement = eng_result.scalar_one_or_none()
    if engagement is None or engagement.diagnostic_package_id is None:
        return None
    package_id = int(engagement.diagnostic_package_id)
    if package_id not in cache:
        package_tests = await diagnostics.get_package_tests(db=db, package_id=package_id)
        cache[package_id] = package_tests.groups
    return cache[package_id]


async def _load_blood_question_map(
    db: AsyncSession,
) -> dict[str, tuple[QuestionnaireDefinition, int]]:
    """Map question_key -> (definition, category_id)."""
    result = await db.execute(
        select(QuestionnaireDefinition, QuestionnaireCategoryQuestion.category_id)
        .join(
            QuestionnaireCategoryQuestion,
            QuestionnaireCategoryQuestion.question_id == QuestionnaireDefinition.question_id,
        )
        .join(
            QuestionnaireCategory,
            QuestionnaireCategory.category_id == QuestionnaireCategoryQuestion.category_id,
        )
        .where(QuestionnaireCategory.category_key.in_(_BLOOD_CATEGORY_KEYS))
        .where(QuestionnaireCategory.category_of == "metsights")
    )
    mapping: dict[str, tuple[QuestionnaireDefinition, int]] = {}
    for definition, category_id in result.all():
        key = (definition.question_key or "").strip()
        if key and key in ALL_BLOOD_PARAMETER_KEYS:
            mapping[key] = (definition, int(category_id))
    return mapping


async def _migrate_metsights_flat_to_questionnaire(
    db: AsyncSession,
    *,
    row: IndividualHealthReport,
    flat: dict[str, Any],
    question_map: dict[str, tuple[QuestionnaireDefinition, int]],
    dry_run: bool,
    clear_blood_fields: bool = True,
) -> tuple[list[str], int]:
    """Return ``(unmapped_keys, migrated_response_count)``."""
    unmapped: list[str] = []
    migrated_count = 0
    now = datetime.now(timezone.utc)

    if row.assessment_instance_id is None:
        return unmapped, migrated_count

    for question_key in ALL_BLOOD_PARAMETER_KEYS:
        if question_key not in flat:
            continue
        raw_value = flat.get(question_key)
        if raw_value is None:
            continue

        mapped = question_map.get(question_key)
        if mapped is None:
            unmapped.append(question_key)
            continue

        definition, category_id = mapped
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            unmapped.append(question_key)
            continue

        unit_code = "0"
        if question_key not in UNITLESS_BLOOD_PARAMETER_KEYS:
            unit_key = f"{question_key}_unit"
            raw_unit = flat.get(unit_key)
            if raw_unit is not None:
                unit_code = await _resolve_unit_option_code(
                    db,
                    question_id=int(definition.question_id),
                    unit_display=str(raw_unit).strip(),
                ) or str(raw_unit).strip()

        migrated_count += 1
        if dry_run:
            continue

        existing = await db.execute(
            select(QuestionnaireResponse)
            .where(QuestionnaireResponse.assessment_instance_id == row.assessment_instance_id)
            .where(QuestionnaireResponse.question_id == definition.question_id)
            .limit(1)
        )
        response = existing.scalar_one_or_none()
        answer = {"value": value, "unit": unit_code}
        if response is None:
            db.add(
                QuestionnaireResponse(
                    assessment_instance_id=row.assessment_instance_id,
                    question_id=definition.question_id,
                    category_id=category_id,
                    answer=answer,
                    submitted_at=now,
                )
            )
        else:
            response.answer = answer
            response.category_id = category_id
            response.submitted_at = now
            db.add(response)

    if migrated_count > 0 and not dry_run and clear_blood_fields:
        row.blood_parameters = None
        row.blood_report_raw = None
        db.add(row)

    return unmapped, migrated_count


async def _resolve_unit_option_code(
    db: AsyncSession,
    *,
    question_id: int,
    unit_display: str,
) -> str | None:
    if not unit_display:
        return None
    result = await db.execute(
        select(QuestionnaireOption).where(QuestionnaireOption.question_id == question_id)
    )
    options = list(result.scalars().all())
    normalized = unit_display.strip().lower()
    for option in options:
        if str(option.display_name or "").strip().lower() == normalized:
            return str(option.option_value).strip()
        if str(option.option_value).strip() == unit_display.strip():
            return str(option.option_value).strip()
    return None
