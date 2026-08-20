"""Batch orchestration for legacy DB text sanitization."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from common.data_sanitize import SanitizeKind, SanitizeOutcome, SanitizeStatus, sanitize_value
from common.slug import collect_cabin_keys_from_slot_detail
from modules.maintenance.sanitize_registry import ColumnSpec, HandlerKind, filter_specs
from modules.maintenance.special_handlers import (
    resolve_cabin_or_null,
    sanitize_camp_report_json,
    sanitize_expert_languages,
    sanitize_notification_services_json,
    sanitize_org_departments,
    sanitize_questionnaire_answer,
    sanitize_slot_detail,
)
from modules.engagements.models import Engagement, EngagementSlotInfo
from modules.questionnaire.models import QuestionnaireDefinition, QuestionnaireOption, QuestionnaireResponse

logger = logging.getLogger(__name__)

_SAMPLE_LIMIT = 20


@dataclass
class ColumnStats:
    scanned: int = 0
    updated: int = 0
    nulled: int = 0
    skipped_required: int = 0
    unchanged: int = 0


@dataclass
class SanitizeReport:
    dry_run: bool
    summary: dict[str, ColumnStats] = field(default_factory=dict)
    manual_review: list[dict[str, Any]] = field(default_factory=list)
    samples: list[dict[str, Any]] = field(default_factory=list)
    skipped_columns: list[dict[str, str]] = field(default_factory=list)

    def stats_key(self, table: str, column: str) -> str:
        return f"{table}.{column}"

    def bump(self, table: str, column: str) -> ColumnStats:
        key = self.stats_key(table, column)
        if key not in self.summary:
            self.summary[key] = ColumnStats()
        return self.summary[key]

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "summary": {
                key: {
                    "scanned": stats.scanned,
                    "updated": stats.updated,
                    "nulled": stats.nulled,
                    "skipped_required": stats.skipped_required,
                    "unchanged": stats.unchanged,
                }
                for key, stats in self.summary.items()
            },
            "manual_review": self.manual_review,
            "samples": self.samples,
            "skipped_columns": self.skipped_columns,
        }


async def _load_public_schema_columns(db: AsyncSession) -> dict[str, set[str]]:
    result = await db.execute(
        text(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            """
        )
    )
    schema: dict[str, set[str]] = {}
    for table_name, column_name in result.all():
        schema.setdefault(str(table_name), set()).add(str(column_name))
    return schema


def _filter_applicable_specs(
    specs: list[ColumnSpec],
    schema: dict[str, set[str]],
    report: SanitizeReport,
) -> list[ColumnSpec]:
    applicable: list[ColumnSpec] = []
    for spec in specs:
        table_cols = schema.get(spec.table)
        if not table_cols:
            reason = "table not found"
            logger.warning("Skipping %s.%s — %s", spec.table, spec.column, reason)
            report.skipped_columns.append(
                {"table": spec.table, "column": spec.column, "reason": reason}
            )
            continue
        needed = {spec.column, *spec.pk_columns}
        missing = needed - table_cols
        if missing:
            reason = f"missing columns: {', '.join(sorted(missing))}"
            logger.warning("Skipping %s.%s — %s", spec.table, spec.column, reason)
            report.skipped_columns.append(
                {"table": spec.table, "column": spec.column, "reason": reason}
            )
            continue
        applicable.append(spec)
    return applicable

def _record_sample(
    report: SanitizeReport,
    *,
    table: str,
    column: str,
    pk: dict[str, Any],
    old_value: Any,
    new_value: Any,
    action: str,
) -> None:
    key = report.stats_key(table, column)
    existing = sum(1 for s in report.samples if s.get("target") == key)
    if existing >= _SAMPLE_LIMIT:
        return
    report.samples.append(
        {
            "target": key,
            "table": table,
            "column": column,
            "pk": pk,
            "action": action,
            "old_value": old_value,
            "new_value": new_value,
        }
    )


def _apply_outcome(
    report: SanitizeReport,
    stats: ColumnStats,
    *,
    table: str,
    column: str,
    pk: dict[str, Any],
    old_value: Any,
    outcome: SanitizeOutcome,
    required: bool,
) -> Any:
    stats.scanned += 1

    if outcome.status == SanitizeStatus.UNCHANGED:
        stats.unchanged += 1
        return old_value

    if outcome.status == SanitizeStatus.SKIP:
        stats.skipped_required += 1
        report.manual_review.append(
            {
                "table": table,
                "column": column,
                "pk": pk,
                "old_value": old_value,
                "reason": outcome.reason or "Validation failed",
            }
        )
        return old_value

    if outcome.status == SanitizeStatus.NULL:
        stats.nulled += 1
        _record_sample(report, table=table, column=column, pk=pk, old_value=old_value, new_value=None, action="null")
        return None

    stats.updated += 1
    _record_sample(
        report,
        table=table,
        column=column,
        pk=pk,
        old_value=old_value,
        new_value=outcome.value,
        action="update",
    )
    return outcome.value


async def _load_slot_detail_map(db: AsyncSession) -> dict[int, dict]:
    result = await db.execute(select(EngagementSlotInfo.slot_detail_id, EngagementSlotInfo.slot_detail))
    mapping: dict[int, dict] = {}
    for slot_detail_id, slot_detail in result.all():
        if isinstance(slot_detail, dict):
            mapping[int(slot_detail_id)] = slot_detail
    return mapping


async def _load_engagement_slot_detail_ids(db: AsyncSession) -> dict[int, int | None]:
    result = await db.execute(select(Engagement.engagement_id, Engagement.slot_detail_id))
    return {int(eid): int(sid) if sid is not None else None for eid, sid in result.all()}


async def _load_question_meta(db: AsyncSession) -> dict[int, dict[str, Any]]:
    result = await db.execute(
        select(
            QuestionnaireDefinition.question_id,
            QuestionnaireDefinition.question_key,
            QuestionnaireDefinition.question_type,
        )
    )
    meta: dict[int, dict[str, Any]] = {}
    for qid, qkey, qtype in result.all():
        meta[int(qid)] = {"question_key": qkey, "question_type": qtype}
    return meta


async def _load_option_values(db: AsyncSession) -> dict[int, set[str]]:
    result = await db.execute(
        select(QuestionnaireOption.question_id, QuestionnaireOption.option_value)
    )
    options: dict[int, set[str]] = {}
    for qid, opt in result.all():
        options.setdefault(int(qid), set()).add(str(opt))
    return options


async def _sanitize_scalar_column(
    db: AsyncSession,
    spec: ColumnSpec,
    report: SanitizeReport,
    *,
    batch_size: int,
    dry_run: bool,
) -> None:
    assert spec.kind is not None
    stats = report.bump(spec.table, spec.column)
    pk_col = spec.pk_columns[0]
    last_id = 0

    while True:
        query = text(
            f"""
            SELECT {", ".join(spec.pk_columns)}, {spec.column}
            FROM {spec.table}
            WHERE {pk_col} > :last_id
            ORDER BY {pk_col}
            LIMIT :limit
            """
        )
        rows = (
            await db.execute(query, {"last_id": last_id, "limit": batch_size})
        ).mappings().all()
        if not rows:
            break

        for row in rows:
            last_id = int(row[pk_col])
            old_value = row[spec.column]
            pk = {col: row[col] for col in spec.pk_columns}

            outcome = sanitize_value(old_value, kind=spec.kind, required=spec.required)
            new_value = _apply_outcome(
                report,
                stats,
                table=spec.table,
                column=spec.column,
                pk=pk,
                old_value=old_value,
                outcome=outcome,
                required=spec.required,
            )

            if not dry_run and new_value is not old_value and (
                outcome.status in {SanitizeStatus.OK, SanitizeStatus.NULL}
            ):
                set_clause = f"{spec.column} = :new_value"
                params: dict[str, Any] = {"new_value": new_value, **pk}
                where = " AND ".join(f"{col} = :{col}" for col in spec.pk_columns)
                await db.execute(
                    text(f"UPDATE {spec.table} SET {set_clause} WHERE {where}"),
                    params,
                )


async def _sanitize_questionnaire_answers(
    db: AsyncSession,
    report: SanitizeReport,
    *,
    batch_size: int,
    dry_run: bool,
) -> None:
    table = "questionnaire_responses"
    column = "answer"
    stats = report.bump(table, column)
    question_meta = await _load_question_meta(db)
    option_values = await _load_option_values(db)
    last_id = 0

    while True:
        result = await db.execute(
            select(QuestionnaireResponse)
            .where(QuestionnaireResponse.response_id > last_id)
            .order_by(QuestionnaireResponse.response_id.asc())
            .limit(batch_size)
        )
        rows = list(result.scalars().all())
        if not rows:
            break

        for row in rows:
            last_id = int(row.response_id)
            pk = {"response_id": row.response_id}
            old_value = row.answer
            meta = question_meta.get(int(row.question_id), {})
            allowed = option_values.get(int(row.question_id))

            outcome = sanitize_questionnaire_answer(
                old_value,
                question_key=meta.get("question_key"),
                question_type=meta.get("question_type"),
                allowed_option_values=allowed,
            )
            new_value = _apply_outcome(
                report,
                stats,
                table=table,
                column=column,
                pk=pk,
                old_value=old_value,
                outcome=outcome,
                required=False,
            )

            if not dry_run and new_value is not old_value:
                row.answer = new_value


async def _sanitize_slot_detail_rows(
    db: AsyncSession,
    report: SanitizeReport,
    *,
    batch_size: int,
    dry_run: bool,
) -> None:
    table = "engagement_slot_info"
    column = "slot_detail"
    stats = report.bump(table, column)
    last_id = 0

    while True:
        result = await db.execute(
            select(EngagementSlotInfo)
            .where(EngagementSlotInfo.slot_detail_id > last_id)
            .order_by(EngagementSlotInfo.slot_detail_id.asc())
            .limit(batch_size)
        )
        rows = list(result.scalars().all())
        if not rows:
            break

        for row in rows:
            last_id = int(row.slot_detail_id)
            pk = {"slot_detail_id": row.slot_detail_id}
            old_value = row.slot_detail
            outcome = sanitize_slot_detail(old_value)
            new_value = _apply_outcome(
                report,
                stats,
                table=table,
                column=column,
                pk=pk,
                old_value=old_value,
                outcome=outcome,
                required=True,
            )
            if not dry_run and new_value is not old_value:
                row.slot_detail = new_value


async def _sanitize_orphan_cabins(
    db: AsyncSession,
    report: SanitizeReport,
    *,
    batch_size: int,
    dry_run: bool,
) -> None:
    slot_details = await _load_slot_detail_map(db)
    engagement_slot_ids = await _load_engagement_slot_detail_ids(db)

    for table, column in (
        ("engagement_participants", "blood_collection_cabin"),
        ("consultation_bookings", "consultation_cabin"),
    ):
        stats = report.bump(table, column)
        pk_col = "engagement_participant_id" if table == "engagement_participants" else "consultation_id"
        last_id = 0

        while True:
            extra_cols = ", engagement_id" if table == "engagement_participants" else ", engagement_participant_id"
            query = text(
                f"""
                SELECT {pk_col}{extra_cols}, {column}
                FROM {table}
                WHERE {pk_col} > :last_id
                  AND {column} IS NOT NULL
                ORDER BY {pk_col}
                LIMIT :limit
                """
            )
            rows = (await db.execute(query, {"last_id": last_id, "limit": batch_size})).mappings().all()
            if not rows:
                break

            for row in rows:
                last_id = int(row[pk_col])
                cabin = row[column]
                pk = {pk_col: row[pk_col]}

                valid_keys: set[str] = set()
                if table == "engagement_participants":
                    slot_detail_id = engagement_slot_ids.get(int(row["engagement_id"]))
                else:
                    ep_result = await db.execute(
                        text(
                            "SELECT engagement_id FROM engagement_participants "
                            "WHERE engagement_participant_id = :epid"
                        ),
                        {"epid": row["engagement_participant_id"]},
                    )
                    ep_row = ep_result.first()
                    slot_detail_id = (
                        engagement_slot_ids.get(int(ep_row[0])) if ep_row else None
                    )

                if slot_detail_id is not None:
                    valid_keys = collect_cabin_keys_from_slot_detail(slot_details.get(slot_detail_id))

                outcome = resolve_cabin_or_null(cabin, valid_keys=valid_keys)
                new_value = _apply_outcome(
                    report,
                    stats,
                    table=table,
                    column=column,
                    pk=pk,
                    old_value=cabin,
                    outcome=outcome,
                    required=False,
                )

                if not dry_run and new_value is not cabin:
                    await db.execute(
                        text(f"UPDATE {table} SET {column} = :val WHERE {pk_col} = :id"),
                        {"val": new_value, "id": row[pk_col]},
                    )


async def _sanitize_special_column(
    db: AsyncSession,
    spec: ColumnSpec,
    report: SanitizeReport,
    *,
    batch_size: int,
    dry_run: bool,
) -> None:
    table = spec.table
    column = spec.column
    stats = report.bump(table, column)
    pk_col = spec.pk_columns[0]
    last_id = 0

    while True:
        query = text(
            f"""
            SELECT {", ".join(spec.pk_columns)}, {column}
            FROM {table}
            WHERE {pk_col} > :last_id
            ORDER BY {pk_col}
            LIMIT :limit
            """
        )
        rows = (await db.execute(query, {"last_id": last_id, "limit": batch_size})).mappings().all()
        if not rows:
            break

        for row in rows:
            last_id = int(row[pk_col])
            old_value = row[column]
            pk = {col: row[col] for col in spec.pk_columns}

            if spec.handler == HandlerKind.NESTED_JSON:
                from common.data_sanitize import sanitize_nested_json

                outcome = sanitize_nested_json(old_value, required=spec.required)
            elif spec.handler == HandlerKind.ORG_DEPARTMENTS:
                outcome = sanitize_org_departments(old_value)
            elif spec.handler == HandlerKind.NOTIFICATION_SERVICES:
                outcome = sanitize_notification_services_json(old_value)
            elif spec.handler == HandlerKind.EXPERT_LANGUAGES:
                outcome = sanitize_expert_languages(old_value)
            elif spec.handler == HandlerKind.CAMP_REPORT_JSON:
                outcome = sanitize_camp_report_json(old_value)
            else:
                continue

            new_value = _apply_outcome(
                report,
                stats,
                table=table,
                column=column,
                pk=pk,
                old_value=old_value,
                outcome=outcome,
                required=spec.required,
            )

            if not dry_run and new_value is not old_value and outcome.status in {
                SanitizeStatus.OK,
                SanitizeStatus.NULL,
            }:
                where = " AND ".join(f"{col} = :{col}" for col in spec.pk_columns)
                if new_value is None:
                    await db.execute(
                        text(f"UPDATE {table} SET {column} = NULL WHERE {where}"),
                        pk,
                    )
                elif isinstance(new_value, (dict, list)):
                    await db.execute(
                        text(
                            f"UPDATE {table} SET {column} = CAST(:new_value AS jsonb) WHERE {where}"
                        ),
                        {"new_value": json.dumps(new_value), **pk},
                    )
                else:
                    await db.execute(
                        text(f"UPDATE {table} SET {column} = :new_value WHERE {where}"),
                        {"new_value": new_value, **pk},
                    )


async def sanitize_legacy_data(
    db: AsyncSession,
    *,
    dry_run: bool = False,
    batch_size: int = 500,
    only: set[str] | None = None,
) -> SanitizeReport:
    """Run full legacy data sanitization pass."""
    report = SanitizeReport(dry_run=dry_run)
    schema = await _load_public_schema_columns(db)
    specs = _filter_applicable_specs(filter_specs(only=only), schema, report)

    scalar_specs = [s for s in specs if s.handler == HandlerKind.SCALAR]
    special_specs = [
        s
        for s in specs
        if s.handler
        not in {HandlerKind.SCALAR, HandlerKind.SLOT_DETAIL, HandlerKind.QUESTIONNAIRE_ANSWER}
    ]

    for spec in scalar_specs:
        logger.info("Sanitizing %s.%s", spec.table, spec.column)
        await _sanitize_scalar_column(db, spec, report, batch_size=batch_size, dry_run=dry_run)

    if any(s.handler == HandlerKind.SLOT_DETAIL for s in specs):
        logger.info("Sanitizing engagement_slot_info.slot_detail")
        await _sanitize_slot_detail_rows(db, report, batch_size=batch_size, dry_run=dry_run)

    if any(
        s.table in {"engagement_participants", "consultation_bookings"}
        and s.column.endswith("cabin")
        for s in specs
    ):
        logger.info("Fixing orphan cabin references")
        await _sanitize_orphan_cabins(db, report, batch_size=batch_size, dry_run=dry_run)

    if any(s.handler == HandlerKind.QUESTIONNAIRE_ANSWER for s in specs):
        logger.info("Sanitizing questionnaire_responses.answer")
        await _sanitize_questionnaire_answers(db, report, batch_size=batch_size, dry_run=dry_run)

    for spec in special_specs:
        logger.info("Sanitizing %s.%s (%s)", spec.table, spec.column, spec.handler.value)
        await _sanitize_special_column(db, spec, report, batch_size=batch_size, dry_run=dry_run)

    return report
