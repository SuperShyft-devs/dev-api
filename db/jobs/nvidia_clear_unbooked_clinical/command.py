"""Clear clinical categories for unbooked participants on NVIDIA engagements.

Deletes questionnaire answers for vitals / health_vitals / blood-parameters /
advanced-blood-parameters on the primary Metsights Pro assessment, for participants
whose ``engagement_participants.booking_id`` is null. Resets matching
``assessment_category_progress`` rows to ``status = incomplete``,
``is_submitted = false``, ``completed_at = NULL``.

Entrypoint::

    python -m db.jobs.nvidia_clear_unbooked_clinical --dry-run
    python -m db.jobs.nvidia_clear_unbooked_clinical --yes

Production example (Linux cron, one-off or ad-hoc)::

    cd /var/www/backend/api && ./venv/bin/python -m db.jobs.nvidia_clear_unbooked_clinical --yes \\
        >> /var/log/supershyft/nvidia-clear-unbooked-clinical.log 2>&1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from db.engine import create_job_engine, job_session_factory

ENGAGEMENT_CODES = (
    "NFGU0926",
    "NMGU0926",
    "NFHY0926",
    "NMHY0926",
    "NFBA0826",
    "NMBA0826",
    "NFPU0926",
    "NMPU0926",
)

CLINICAL_CATEGORY_KEYS = (
    "vitals",
    "health_vitals",
    "blood-parameters",
    "advanced-blood-parameters",
)

CLINICAL_GROUPS = ("vitals", "blood-parameters", "advanced-blood-parameters")

SCOPED_INSTANCES_CTE = """
    scoped_instances AS (
        SELECT DISTINCT
            ai.assessment_instance_id,
            ai.user_id,
            e.engagement_id,
            e.engagement_code
        FROM engagement_participants ep
        JOIN engagements e ON e.engagement_id = ep.engagement_id
        JOIN assessment_instances ai
            ON ai.engagement_id = ep.engagement_id
           AND ai.user_id = ep.user_id
        JOIN assessment_packages ap ON ap.package_id = ai.package_id
        WHERE e.engagement_code = ANY(:engagement_codes)
          AND ep.booking_id IS NULL
          AND ap.assessment_type_code = '2'
    )
"""

SELECT_RESPONSE_DETAILS_SQL = text(
    f"""
    WITH {SCOPED_INSTANCES_CTE}
    SELECT
        si.engagement_code,
        si.user_id,
        u.phone,
        qc.category_key,
        qd.question_key,
        qd.question_text,
        qr.response_id
    FROM questionnaire_responses qr
    JOIN scoped_instances si ON si.assessment_instance_id = qr.assessment_instance_id
    JOIN users u ON u.user_id = si.user_id
    JOIN questionnaire_definitions qd ON qd.question_id = qr.question_id
    JOIN questionnaire_categories qc
        ON qc.category_id = ANY(qr.category_ids)
       AND qc.category_key = ANY(:clinical_keys)
    ORDER BY si.engagement_code, si.user_id, qc.category_key, qd.question_key, qr.response_id
    """
)

COUNT_RESPONSES_BY_ENGAGEMENT_SQL = text(
    f"""
    WITH {SCOPED_INSTANCES_CTE}
    SELECT si.engagement_code, count(DISTINCT qr.response_id) AS response_count
    FROM questionnaire_responses qr
    JOIN scoped_instances si ON si.assessment_instance_id = qr.assessment_instance_id
    WHERE EXISTS (
        SELECT 1
        FROM questionnaire_categories qc
        WHERE qc.category_id = ANY(qr.category_ids)
          AND qc.category_key = ANY(:clinical_keys)
    )
    GROUP BY si.engagement_code
    ORDER BY si.engagement_code
    """
)

COUNT_PARTICIPANTS_SQL = text(
    f"""
    WITH {SCOPED_INSTANCES_CTE}
    SELECT count(DISTINCT user_id) AS participant_count
    FROM scoped_instances
    """
)

COUNT_PROGRESS_BY_GROUP_SQL = text(
    f"""
    WITH {SCOPED_INSTANCES_CTE},
    clinical_progress AS (
        SELECT
            si.assessment_instance_id,
            CASE
                WHEN qc.category_key IN ('vitals', 'health_vitals') THEN 'vitals'
                WHEN qc.category_key = 'blood-parameters' THEN 'blood-parameters'
                ELSE 'advanced-blood-parameters'
            END AS clinical_group,
            acp.status,
            acp.is_submitted
        FROM scoped_instances si
        JOIN assessment_instances ai ON ai.assessment_instance_id = si.assessment_instance_id
        JOIN assessment_package_categories apc ON apc.package_id = ai.package_id
        JOIN questionnaire_categories qc ON qc.category_id = apc.category_id
        JOIN assessment_category_progress acp
            ON acp.assessment_instance_id = si.assessment_instance_id
           AND acp.category_id = qc.category_id
        WHERE qc.category_key = ANY(:clinical_keys)
    )
    SELECT
        clinical_group,
        count(*) AS progress_rows,
        count(*) FILTER (WHERE lower(coalesce(status, '')) = 'complete') AS complete_rows,
        count(*) FILTER (WHERE is_submitted) AS submitted_rows
    FROM clinical_progress
    GROUP BY clinical_group
    ORDER BY clinical_group
    """
)

RESET_PROGRESS_SQL = text(
    f"""
    WITH {SCOPED_INSTANCES_CTE},
    clinical_progress AS (
        SELECT
            si.assessment_instance_id,
            apc.category_id
        FROM scoped_instances si
        JOIN assessment_instances ai ON ai.assessment_instance_id = si.assessment_instance_id
        JOIN assessment_package_categories apc ON apc.package_id = ai.package_id
        JOIN questionnaire_categories qc ON qc.category_id = apc.category_id
        WHERE qc.category_key = ANY(:clinical_keys)
    )
    UPDATE assessment_category_progress acp
    SET status = 'incomplete',
        is_submitted = false,
        completed_at = NULL
    FROM clinical_progress cp
    WHERE acp.assessment_instance_id = cp.assessment_instance_id
      AND acp.category_id = cp.category_id
    """
)

DELETE_RESPONSES_SQL = text(
    """
    DELETE FROM questionnaire_responses
    WHERE response_id = ANY(:response_ids)
    """
)


@dataclass
class ProgressBar:
    total: int
    label: str
    started_at: float = field(default_factory=time.monotonic)
    done: int = 0

    def update(self, n: int = 1) -> None:
        self.done = min(self.total, self.done + n)
        self._render()

    def _render(self) -> None:
        total = max(self.total, 1)
        now = time.monotonic()
        finished = self.done >= self.total
        pct = 100.0 * self.done / total
        elapsed = max(now - self.started_at, 0.001)
        rate = self.done / elapsed
        remaining = (total - self.done) / rate if rate > 0 else 0.0
        bar_len = 28
        filled = int(bar_len * self.done / total)
        bar = "#" * filled + "-" * (bar_len - filled)
        msg = (
            f"\r{self.label}: [{bar}] {pct:5.1f}% "
            f"({self.done}/{self.total}) "
            f"elapsed {elapsed:6.1f}s ETA {remaining:6.1f}s"
        )
        sys.stdout.write(msg)
        sys.stdout.flush()
        if finished:
            sys.stdout.write("\n")
            sys.stdout.flush()


def _progress_by_group(rows: list[Any]) -> dict[str, dict[str, int]]:
    grouped = {
        group: {"progress_rows": 0, "complete_rows": 0, "submitted_rows": 0}
        for group in CLINICAL_GROUPS
    }
    for row in rows:
        group = str(row.clinical_group)
        if group not in grouped:
            continue
        grouped[group] = {
            "progress_rows": int(row.progress_rows),
            "complete_rows": int(row.complete_rows),
            "submitted_rows": int(row.submitted_rows),
        }
    return grouped


async def _fetch_preview(
    session: AsyncSession,
    *,
    engagement_codes: tuple[str, ...],
) -> dict[str, Any]:
    params = {
        "engagement_codes": list(engagement_codes),
        "clinical_keys": list(CLINICAL_CATEGORY_KEYS),
    }

    participant_count = int(
        (await session.execute(COUNT_PARTICIPANTS_SQL, params)).scalar_one()
    )
    progress_group_rows = (await session.execute(COUNT_PROGRESS_BY_GROUP_SQL, params)).all()
    response_detail_rows = (await session.execute(SELECT_RESPONSE_DETAILS_SQL, params)).all()
    engagement_response_rows = (
        await session.execute(COUNT_RESPONSES_BY_ENGAGEMENT_SQL, params)
    ).all()

    response_ids = sorted({int(row.response_id) for row in response_detail_rows})
    responses_to_delete = [
        {
            "engagement_code": row.engagement_code,
            "user_id": int(row.user_id),
            "phone": row.phone,
            "category_key": row.category_key,
            "question_key": row.question_key,
            "question_text": row.question_text,
            "response_id": int(row.response_id),
        }
        for row in response_detail_rows
    ]

    progress_by_group = _progress_by_group(progress_group_rows)
    progress_rows_to_reset_total = sum(
        group["progress_rows"] for group in progress_by_group.values()
    )

    return {
        "participant_count": participant_count,
        "response_count": len(response_ids),
        "response_ids": response_ids,
        "responses_to_delete": responses_to_delete,
        "progress_by_group": progress_by_group,
        "progress_rows_to_reset_total": progress_rows_to_reset_total,
        "by_engagement_responses": {
            str(row.engagement_code): int(row.response_count)
            for row in engagement_response_rows
        },
    }


def _print_summary(*, dry_run: bool, preview: dict[str, Any]) -> None:
    banner = "DRY RUN — no database changes" if dry_run else "APPLIED — database updated"
    print("=" * 64, flush=True)
    print(f"NVIDIA clear unbooked clinical — {banner}", flush=True)
    print("=" * 64, flush=True)

    print("\nWHO (scope)", flush=True)
    print(f"  {preview['participant_count']} participants", flush=True)
    print("  booking_id IS NULL on engagement_participants", flush=True)
    print("  primary assessment = Metsights Pro (assessment_type_code = 2)", flush=True)
    print(f"  engagements: {', '.join(preview['engagement_codes'])}", flush=True)

    print("\nDELETE — table: questionnaire_responses", flush=True)
    print("  removes columns: response_id, assessment_instance_id, question_id, category_ids, answer", flush=True)
    print(f"  total rows to delete: {preview['response_count']}", flush=True)
    if preview["responses_to_delete"]:
        print(
            "\n  "
            f"{'#':<4} {'engagement':<10} {'user_id':<8} {'phone':<14} "
            f"{'category':<26} {'question_key':<28} question_text",
            flush=True,
        )
        seen_response_ids: set[int] = set()
        line_no = 0
        for item in preview["responses_to_delete"]:
            if item["response_id"] in seen_response_ids:
                continue
            seen_response_ids.add(item["response_id"])
            line_no += 1
            phone = (item["phone"] or "")[:14]
            question_text = (item["question_text"] or "")[:60]
            print(
                f"  {line_no:<4} {item['engagement_code']:<10} {item['user_id']:<8} {phone:<14} "
                f"{item['category_key']:<26} {item['question_key']:<28} {question_text}",
                flush=True,
            )
    else:
        print("  (none — no clinical answers found in scope)", flush=True)

    print("\nUPDATE — table: assessment_category_progress", flush=True)
    print("  status        -> 'incomplete'", flush=True)
    print("  is_submitted  -> false", flush=True)
    print("  completed_at  -> NULL", flush=True)
    print(f"  total rows to update: {preview['reset_progress_rows']}", flush=True)
    for group in CLINICAL_GROUPS:
        stats = preview["progress_by_group"][group]
        note = ""
        if group == "vitals":
            note = " (includes vitals + health_vitals category keys)"
        print(
            f"  {group}{note}: {stats['progress_rows']} rows "
            f"(was complete={stats['complete_rows']}, was submitted={stats['submitted_rows']})",
            flush=True,
        )

    print("\nNOT TOUCHED", flush=True)
    print("  engagement_participants, assessment_instances, individual_health_reports, Metsights", flush=True)

    if dry_run:
        print("\nRun with --yes to apply these changes.", flush=True)
    else:
        print(
            f"\nDone: deleted {preview['deleted_responses']} response row(s), "
            f"updated {preview['reset_progress_rows']} progress row(s).",
            flush=True,
        )
    print("=" * 64, flush=True)


async def run_clear_unbooked_clinical(
    *,
    yes: bool,
    dry_run: bool,
    engagement_codes: tuple[str, ...],
    report_path: Path | None,
) -> dict[str, Any]:
    settings.validate()

    if not yes and not dry_run:
        raise SystemExit(
            "Refusing to run without explicit confirmation. Re-run with --yes to apply changes, "
            "or --dry-run to preview."
        )

    engine = create_job_engine()
    session_factory = job_session_factory(engine)
    params = {
        "engagement_codes": list(engagement_codes),
        "clinical_keys": list(CLINICAL_CATEGORY_KEYS),
    }

    try:
        async with session_factory() as session:
            preview = await _fetch_preview(session, engagement_codes=engagement_codes)
            preview["engagement_codes"] = list(engagement_codes)

            deleted_responses = 0
            reset_progress_rows = 0

            if yes:
                response_ids = preview["response_ids"]
                if response_ids:
                    bar = ProgressBar(total=len(response_ids), label="Deleting responses")
                    batch_size = 500
                    for offset in range(0, len(response_ids), batch_size):
                        batch = response_ids[offset : offset + batch_size]
                        result = await session.execute(
                            DELETE_RESPONSES_SQL,
                            {"response_ids": batch},
                        )
                        deleted_responses += int(result.rowcount or 0)
                        bar.update(len(batch))

                result = await session.execute(RESET_PROGRESS_SQL, params)
                reset_progress_rows = int(result.rowcount or 0)
                await session.commit()

            preview["deleted_responses"] = deleted_responses
            preview["reset_progress_rows"] = (
                reset_progress_rows if yes else preview["progress_rows_to_reset_total"]
            )
            preview["dry_run"] = dry_run
            preview["clinical_category_keys"] = list(CLINICAL_CATEGORY_KEYS)

            _print_summary(dry_run=dry_run, preview=preview)

            if report_path is not None:
                payload = {k: v for k, v in preview.items() if k != "response_ids"}
                report_path.parent.mkdir(parents=True, exist_ok=True)
                report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                print(f"Wrote report: {report_path}", flush=True)

            return preview
    finally:
        await engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Delete vitals/blood clinical questionnaire answers for participants with "
            "booking_id null on the 8 NVIDIA engagements, and reset category progress "
            "(status=incomplete, is_submitted=false). Safe to re-run."
        )
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply changes. Without this flag (and without --dry-run), the command exits without writing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview counts without deleting or updating.",
    )
    parser.add_argument(
        "--engagement-code",
        action="append",
        dest="engagement_codes",
        help="Limit to engagement code(s). Default: all 8 NVIDIA engagements.",
    )
    parser.add_argument(
        "--report",
        default=None,
        metavar="PATH",
        help="Optional JSON report path.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    engagement_codes = tuple(args.engagement_codes) if args.engagement_codes else ENGAGEMENT_CODES
    report_path = Path(args.report) if args.report else None

    asyncio.run(
        run_clear_unbooked_clinical(
            yes=args.yes,
            dry_run=args.dry_run,
            engagement_codes=engagement_codes,
            report_path=report_path,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
