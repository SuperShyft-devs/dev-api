"""Sync assessment_instances.completed_at from MetSights for all engagements.

Fetches GET /records/{metsights_record_id}/ for every linked assessment instance
(all engagement statuses) and overwrites local completed_at to match MetSights.

Entrypoint: ``python -m db.jobs.all_engagements.sync_metsights_completed_at --yes``
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from core.config import settings
from db.engine import create_job_engine, job_session_factory
from modules.assessments.repository import AssessmentsRepository
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.notifications.sync_metsights_completed_at import sync_metsights_completed_at

_PROGRESS_BAR_WIDTH = 30
_NON_TTY_PRINT_EVERY = 5
_PROGRESS_LINE_PAD = 120


def _format_progress(
    done: int,
    total: int,
    updated: int,
    cleared: int,
    skipped: int,
    failed: int,
) -> str:
    pct = 100 if total == 0 else int(100 * done / total)
    filled = _PROGRESS_BAR_WIDTH if total == 0 else int(_PROGRESS_BAR_WIDTH * done / total)
    bar = "█" * filled + "░" * (_PROGRESS_BAR_WIDTH - filled)
    return (
        f"[{bar}] {done}/{total} ({pct}%)  "
        f"updated={updated} cleared={cleared} skipped={skipped} failed={failed}"
    )


def _make_progress_printer():
    is_tty = sys.stdout.isatty()
    last_counts: tuple[int, int, int, int] | None = None
    last_printed_done = -1

    def on_progress(
        done: int,
        total: int,
        updated: int,
        cleared: int,
        skipped: int,
        failed: int,
    ) -> None:
        nonlocal last_counts, last_printed_done
        line = _format_progress(done, total, updated, cleared, skipped, failed)
        counts = (updated, cleared, skipped, failed)

        if is_tty:
            # Pad so shorter redraws clear leftover characters on Windows.
            padded = line.ljust(_PROGRESS_LINE_PAD)
            print(f"\r{padded}", end="", flush=True)
            if done >= total:
                print(flush=True)
            return

        counts_changed = counts != last_counts
        should_print = (
            done == 0
            or done >= total
            or counts_changed
            or (done - last_printed_done) >= _NON_TTY_PRINT_EVERY
        )
        if should_print:
            print(line, flush=True)
            last_counts = counts
            last_printed_done = done

    return on_progress


async def run_sync(
    *,
    yes: bool,
    dry_run: bool,
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

    metsights_client = MetsightsClient()
    metsights_service = MetsightsService(client=metsights_client)
    on_progress = _make_progress_printer()

    scope = (
        f"engagement_id={engagement_id}"
        if engagement_id is not None
        else "all engagements"
    )
    print(f"Loading assessment instances with MetSights record ids ({scope})...", flush=True)
    print("Syncing completed_at from MetSights...", flush=True)

    async with session_factory() as session:
        result = await sync_metsights_completed_at(
            session,
            metsights_service=metsights_service,
            assessments_repo=AssessmentsRepository(),
            engagement_id=engagement_id,
            dry_run=dry_run,
            on_progress=on_progress,
        )

    await engine.dispose()
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Overwrite assessment_instances.completed_at from MetSights record detail "
            "for every instance with a metsights_record_id (all engagement statuses)."
        )
    )
    parser.add_argument("--yes", action="store_true", help="Apply updates.")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing.")
    parser.add_argument(
        "--engagement-id",
        type=int,
        default=None,
        metavar="ID",
        help="Limit to one engagement (useful for testing).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-instance action details after the run.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_sync(
            yes=args.yes,
            dry_run=args.dry_run,
            engagement_id=args.engagement_id,
        )
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    eng = result.get("engagement_id")
    scope = f"engagement_id={eng}" if eng is not None else "all engagements"
    print(
        f"\nSync MetSights completed_at ({mode}, {scope}):\n"
        f"  matched={result['matched']}, updated={result['updated']}, "
        f"cleared={result['cleared']}, skipped={result['skipped']}, failed={result['failed']}"
    )
    details = result.get("details", [])
    if args.verbose and details:
        print(f"\n  {'INSTANCE':>10}  {'USER':>8}  {'ACTION':>10}  REASON")
        print(f"  {'─' * 10}  {'─' * 8}  {'─' * 10}  {'─' * 40}")
        for d in details:
            print(
                f"  {d['assessment_instance_id']:>10}  {d['user_id']:>8}  "
                f"{d['action']:>10}  {d['reason']}"
            )
    print()
    return 0
