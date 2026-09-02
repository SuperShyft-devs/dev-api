"""Sync diagnostic_report_url from Healthians for participants with booking_id.

URL-only backfill for all engagements (any status) where engagement_participants
has a booking_id. Does not load digital blood values or send notifications.

Entrypoint: ``python -m db.jobs.sync_diagnostic_report_urls --yes``
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from core.config import settings
from db.engine import create_job_engine, job_session_factory
from modules.reports.sync_diagnostic_report_urls import sync_diagnostic_report_urls

_PROGRESS_BAR_WIDTH = 30


def _format_progress(
    done: int,
    total: int,
    updated: int,
    skipped: int,
    failed: int,
) -> str:
    pct = 100 if total == 0 else int(100 * done / total)
    filled = _PROGRESS_BAR_WIDTH if total == 0 else int(_PROGRESS_BAR_WIDTH * done / total)
    bar = "█" * filled + "░" * (_PROGRESS_BAR_WIDTH - filled)
    return (
        f"[{bar}] {done}/{total} ({pct}%)  "
        f"updated={updated} skipped={skipped} failed={failed}"
    )


def _make_progress_printer():
    is_tty = sys.stdout.isatty()
    last_pct = -1

    def on_progress(
        done: int,
        total: int,
        updated: int,
        skipped: int,
        failed: int,
    ) -> None:
        nonlocal last_pct
        line = _format_progress(done, total, updated, skipped, failed)
        if is_tty:
            print(f"\r{line}", end="", flush=True)
            if done >= total:
                print(flush=True)
            return

        pct = 100 if total == 0 else int(100 * done / total)
        if done == 0 or done >= total or pct != last_pct:
            print(line, flush=True)
            last_pct = pct

    return on_progress


async def run_sync(
    *,
    yes: bool,
    dry_run: bool,
    force: bool,
    engagement_id: int | None,
) -> dict:
    settings.validate()

    if not yes and not dry_run:
        raise SystemExit(
            "Refusing to run without explicit confirmation. Re-run with --yes to apply changes, "
            "or --dry-run to preview."
        )

    engine = create_job_engine()
    session_factory = job_session_factory(engine)
    on_progress = _make_progress_printer()

    if engagement_id is not None:
        print(
            f"Syncing diagnostic_report_url for engagement_id={engagement_id} "
            "(all statuses, booking_id required)...",
            flush=True,
        )
    else:
        print(
            "Syncing diagnostic_report_url for all participants with booking_id...",
            flush=True,
        )

    async with session_factory() as session:
        result = await sync_diagnostic_report_urls(
            session,
            dry_run=dry_run,
            force=force,
            engagement_id=engagement_id,
            on_progress=on_progress,
        )
        await session.commit()

    await engine.dispose()
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Sync individual_health_report.diagnostic_report_url from Healthians "
            "getBookingReport for participants with booking_id."
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
        help="Report what would be synced without making changes.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch and update even when diagnostic_report_url is already archived.",
    )
    parser.add_argument(
        "--engagement-id",
        type=int,
        default=None,
        metavar="ID",
        help="Optional: limit to one engagement_id.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_sync(
            yes=args.yes,
            dry_run=args.dry_run,
            force=args.force,
            engagement_id=args.engagement_id,
        )
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    engagement_line = ""
    if result.get("engagement_id") is not None:
        engagement_line = f"  engagement_id={result['engagement_id']}\n"
    force_line = "  force=true\n" if result.get("force") else ""
    print(
        f"\nSync diagnostic report URLs ({mode}):\n"
        f"{force_line}"
        f"{engagement_line}"
        f"  matched={result['matched']}, updated={result['updated']}, "
        f"skipped={result['skipped']}, failed={result['failed']}"
    )
    details = result.get("details", [])
    if details:
        print(f"\n  {'USER':>8}  {'ENG':>6}  {'ACTION':>10}  REASON")
        print(f"  {'─' * 8}  {'─' * 6}  {'─' * 10}  {'─' * 40}")
        for d in details:
            print(
                f"  {d['user_id']:>8}  {d['engagement_id']:>6}  "
                f"{d['action']:>10}  {d['reason']}"
            )
    print()
    return 0
