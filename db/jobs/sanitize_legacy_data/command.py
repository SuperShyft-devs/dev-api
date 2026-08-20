"""One-off legacy DB text sanitization job.

Entrypoint::

    python -m db.jobs.sanitize_legacy_data --dry-run
    python -m db.jobs.sanitize_legacy_data --dry-run --report /tmp/sanitize-dry-run.json
    python -m db.jobs.sanitize_legacy_data --yes --report /tmp/sanitize-applied.json
    python -m db.jobs.sanitize_legacy_data --yes --only users,engagements
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.config import settings
from modules.maintenance.sanitize_legacy_data import sanitize_legacy_data

logger = logging.getLogger(__name__)


async def run_sanitize(
    *,
    yes: bool,
    dry_run: bool,
    batch_size: int,
    report_path: str | None,
    only: set[str] | None,
) -> dict:
    settings.validate()
    if not yes and not dry_run:
        raise SystemExit(
            "Refusing to run without explicit confirmation. Re-run with --yes to apply changes, "
            "or --dry-run to preview."
        )

    engine = create_async_engine(settings.DATABASE_URL, echo=False, pool_pre_ping=True)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with session_factory() as session:
        report = await sanitize_legacy_data(
            session,
            dry_run=dry_run,
            batch_size=batch_size,
            only=only,
        )
        if not dry_run:
            await session.commit()

    await engine.dispose()
    result = report.to_dict()

    if report_path:
        path = Path(report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
        logger.info("Wrote report to %s", path)

    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Sanitize legacy DB text to match post-85506b35 validation rules. "
            "Optional fields that cannot be cleaned are set to NULL; required-field "
            "failures are logged for manual review."
        )
    )
    parser.add_argument("--yes", action="store_true", help="Apply changes.")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing.")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows per batch (default: 500).",
    )
    parser.add_argument(
        "--report",
        type=str,
        default=None,
        help="Write JSON report to this path.",
    )
    parser.add_argument(
        "--only",
        type=str,
        default=None,
        help="Comma-separated table names or groups: users, organizations, engagements, questionnaire, catalog.",
    )
    return parser


def _print_summary(result: dict) -> None:
    mode = "dry-run" if result.get("dry_run") else "applied"
    print(f"\nLegacy data sanitization ({mode}):")
    summary = result.get("summary") or {}
    if not summary:
        print("  (no columns processed)")
    for key, stats in sorted(summary.items()):
        print(
            f"  {key}: scanned={stats['scanned']} updated={stats['updated']} "
            f"nulled={stats['nulled']} skipped_required={stats['skipped_required']} "
            f"unchanged={stats['unchanged']}"
        )
    manual = result.get("manual_review") or []
    print(f"\nManual review items: {len(manual)}")
    if manual:
        for item in manual[:10]:
            print(
                f"  {item['table']}.{item['column']} pk={item['pk']}: "
                f"{item.get('reason', 'validation failed')}"
            )
        if len(manual) > 10:
            print(f"  ... and {len(manual) - 10} more (see report file)")


def main(argv: list[str] | None = None) -> int:
    load_dotenv(override=False)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args = _build_parser().parse_args(argv)
    only: set[str] | None = None
    if args.only:
        only = {part.strip().lower() for part in args.only.split(",") if part.strip()}

    result = asyncio.run(
        run_sanitize(
            yes=args.yes,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
            report_path=args.report,
            only=only,
        )
    )
    _print_summary(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
