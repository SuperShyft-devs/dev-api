"""Reconcile assessment_instance.status with assessment_category_progress.

After load_prev copy + vitals cleanup, some instances still have
``assessment_instances.status = completed`` even though one or more package
categories are incomplete in ``assessment_category_progress``.

This job finds those mismatches in the 8 NVIDIA 2026 engagements and sets
instance status back to ``active`` (clears ``completed_at``).

Entrypoint: ``python -m db.jobs.nvidia_reconcile_assessment_status --yes``

Production example (Linux cron, daily)::

    15 3 * * * cd /var/www/backend/api && ./venv/bin/python -m db.jobs.nvidia_reconcile_assessment_status --yes >> /var/log/supershyft/nvidia-reconcile-assessment-status.log 2>&1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
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

FIND_MISMATCHES_SQL = text(
    """
    WITH scoped_instances AS (
        SELECT
            ai.assessment_instance_id,
            ai.user_id,
            ai.engagement_id,
            e.engagement_code,
            ai.package_id,
            ap.package_code,
            ai.status AS instance_status,
            ai.completed_at
        FROM assessment_instances ai
        JOIN engagements e ON e.engagement_id = ai.engagement_id
        JOIN assessment_packages ap ON ap.package_id = ai.package_id
        WHERE e.engagement_code = ANY(:engagement_codes)
          AND lower(coalesce(ai.status, '')) = 'completed'
    ),
    category_rows AS (
        SELECT
            si.assessment_instance_id,
            si.user_id,
            si.engagement_id,
            si.engagement_code,
            si.package_id,
            si.package_code,
            si.instance_status,
            si.completed_at,
            qc.category_id,
            qc.category_key,
            coalesce(lower(acp.status), 'incomplete') AS category_status
        FROM scoped_instances si
        JOIN assessment_package_categories apc ON apc.package_id = si.package_id
        JOIN questionnaire_categories qc ON qc.category_id = apc.category_id
        LEFT JOIN assessment_category_progress acp
            ON acp.assessment_instance_id = si.assessment_instance_id
           AND acp.category_id = qc.category_id
    ),
    rolled AS (
        SELECT
            assessment_instance_id,
            user_id,
            engagement_id,
            engagement_code,
            package_id,
            package_code,
            instance_status,
            completed_at,
            count(*) AS total_categories,
            count(*) FILTER (WHERE category_status = 'complete') AS complete_categories,
            count(*) FILTER (WHERE category_status <> 'complete') AS incomplete_categories,
            array_agg(category_key ORDER BY category_key)
                FILTER (WHERE category_status <> 'complete') AS incomplete_category_keys
        FROM category_rows
        GROUP BY
            assessment_instance_id,
            user_id,
            engagement_id,
            engagement_code,
            package_id,
            package_code,
            instance_status,
            completed_at
    )
    SELECT *
    FROM rolled
    WHERE incomplete_categories > 0
    ORDER BY engagement_code, assessment_instance_id
    """
)

UPDATE_INSTANCE_SQL = text(
    """
    UPDATE assessment_instances
    SET status = 'active',
        completed_at = NULL
    WHERE assessment_instance_id = ANY(:instance_ids)
      AND lower(coalesce(status, '')) = 'completed'
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


@dataclass
class MismatchRow:
    assessment_instance_id: int
    user_id: int
    engagement_id: int
    engagement_code: str
    package_id: int
    package_code: str | None
    instance_status: str
    completed_at: str | None
    total_categories: int
    complete_categories: int
    incomplete_categories: int
    incomplete_category_keys: list[str]
    planned_new_status: str = "active"


def _row_to_mismatch(row: Any) -> MismatchRow:
    keys = list(row.incomplete_category_keys or [])
    completed_at = row.completed_at
    if completed_at is not None and hasattr(completed_at, "isoformat"):
        completed_at = completed_at.isoformat()
    return MismatchRow(
        assessment_instance_id=int(row.assessment_instance_id),
        user_id=int(row.user_id),
        engagement_id=int(row.engagement_id),
        engagement_code=str(row.engagement_code),
        package_id=int(row.package_id),
        package_code=row.package_code,
        instance_status=str(row.instance_status or ""),
        completed_at=completed_at,
        total_categories=int(row.total_categories),
        complete_categories=int(row.complete_categories),
        incomplete_categories=int(row.incomplete_categories),
        incomplete_category_keys=keys,
    )


async def find_mismatches(
    session: AsyncSession,
    *,
    engagement_codes: tuple[str, ...],
) -> list[MismatchRow]:
    result = await session.execute(
        FIND_MISMATCHES_SQL,
        {"engagement_codes": list(engagement_codes)},
    )
    return [_row_to_mismatch(row) for row in result.all()]


def _counts_by_engagement(
    mismatches: list[MismatchRow],
    engagement_codes: tuple[str, ...],
) -> dict[str, int]:
    counts = Counter(row.engagement_code for row in mismatches)
    return {code: int(counts.get(code, 0)) for code in engagement_codes}


def _print_summary(
    *,
    dry_run: bool,
    engagement_codes: tuple[str, ...],
    mismatches: list[MismatchRow],
    updated_count: int,
) -> None:
    mode = "dry-run" if dry_run else "applied"
    by_engagement = _counts_by_engagement(mismatches, engagement_codes)
    print(
        f"Reconcile assessment instance status ({mode}): "
        f"mismatch_count={len(mismatches)} updated_count={updated_count}",
        flush=True,
    )
    for code in engagement_codes:
        count = by_engagement.get(code, 0)
        if count:
            print(f"  {code}: {count}", flush=True)
    if dry_run and mismatches:
        print("Re-run with --yes to set status=active on these instances.", flush=True)


def _write_optional_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote report: {path}", flush=True)


async def run_reconcile_assessment_status(
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

    try:
        async with session_factory() as session:
            print("Scanning for completed instances with incomplete categories...", flush=True)
            mismatches = await find_mismatches(session, engagement_codes=engagement_codes)

            updated_count = 0
            if yes and mismatches:
                instance_ids = [row.assessment_instance_id for row in mismatches]
                bar = ProgressBar(total=len(instance_ids), label="Updating instances")
                batch_size = 100
                for offset in range(0, len(instance_ids), batch_size):
                    batch = instance_ids[offset : offset + batch_size]
                    result = await session.execute(
                        UPDATE_INSTANCE_SQL,
                        {"instance_ids": batch},
                    )
                    updated_count += int(result.rowcount or 0)
                    bar.update(len(batch))
                await session.commit()
            else:
                await session.rollback()

            _print_summary(
                dry_run=dry_run,
                engagement_codes=engagement_codes,
                mismatches=mismatches,
                updated_count=updated_count,
            )

            if report_path is not None:
                _write_optional_report(
                    report_path,
                    {
                        "dry_run": dry_run,
                        "engagement_codes": list(engagement_codes),
                        "mismatch_count": len(mismatches),
                        "updated_count": updated_count,
                        "by_engagement": _counts_by_engagement(mismatches, engagement_codes),
                        "mismatches": [asdict(row) for row in mismatches],
                    },
                )

            return {
                "dry_run": dry_run,
                "mismatch_count": len(mismatches),
                "updated_count": updated_count,
                "by_engagement": _counts_by_engagement(mismatches, engagement_codes),
            }
    finally:
        await engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Set assessment_instances back to active when status is completed but "
            "package category progress is incomplete (NVIDIA engagements). "
            "Safe to re-run (idempotent)."
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
        help="Report mismatches without updating assessment_instances.",
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
        help="Optional JSON report path (for audit). Output also goes to stdout / cron log.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    engagement_codes = tuple(args.engagement_codes) if args.engagement_codes else ENGAGEMENT_CODES
    report_path = Path(args.report) if args.report else None

    asyncio.run(
        run_reconcile_assessment_status(
            yes=args.yes,
            dry_run=args.dry_run,
            engagement_codes=engagement_codes,
            report_path=report_path,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
