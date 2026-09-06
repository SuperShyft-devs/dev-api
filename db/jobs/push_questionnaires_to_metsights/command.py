"""Push questionnaire answers to MetSights for running engagements.

Merges answers from all assessment packages per participant and PATCHes Metsights
sub-resources (physical, vitals, diet-lifestyle, fitness). Blood categories are
excluded (see load_blood_reports).

Entrypoint: ``python -m db.jobs.push_questionnaires_to_metsights --yes``

Production example (Linux cron — adjust schedule on your server):
  # 0 */6 * * * cd /var/www/backend/api && ./venv/bin/python -m db.jobs.push_questionnaires_to_metsights --yes >> /var/log/push_questionnaires_to_metsights.log 2>&1
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date

from core.config import settings
from db.engine import create_job_engine, job_session_factory
from modules.assessments.dependencies import get_assessments_service
from modules.engagements.dependencies import get_engagements_service
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import MetsightsSyncService
from modules.notifications.push_questionnaires_to_metsights import push_questionnaires_to_metsights
from modules.platform_settings.dependencies import get_platform_settings_service_readonly
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.repository import UsersRepository

_PROGRESS_BAR_WIDTH = 30


def _format_progress(
    done: int,
    total: int,
    pushed: int,
    skipped: int,
    failed: int,
) -> str:
    pct = 100 if total == 0 else int(100 * done / total)
    filled = _PROGRESS_BAR_WIDTH if total == 0 else int(_PROGRESS_BAR_WIDTH * done / total)
    bar = "█" * filled + "░" * (_PROGRESS_BAR_WIDTH - filled)
    return (
        f"[{bar}] {done}/{total} ({pct}%)  "
        f"pushed={pushed} skipped={skipped} failed={failed}"
    )


def _make_progress_printer():
    """Return a progress callback that redraws in-place on a TTY, or logs lines otherwise."""
    is_tty = sys.stdout.isatty()
    last_pct = -1

    def on_progress(
        done: int,
        total: int,
        pushed: int,
        skipped: int,
        failed: int,
    ) -> None:
        nonlocal last_pct
        line = _format_progress(done, total, pushed, skipped, failed)
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


async def run_push(
    *,
    yes: bool,
    dry_run: bool,
    as_of: date | None,
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
    sync_service = MetsightsSyncService(
        metsights_service=metsights_service,
        users_repository=UsersRepository(),
        engagements_service=get_engagements_service(),
        assessments_service=get_assessments_service(),
        platform_settings_service=get_platform_settings_service_readonly(),
        questionnaire_repository=QuestionnaireRepository(),
    )
    on_progress = _make_progress_printer()
    print("Loading assessment instances for running engagements...", flush=True)

    async with session_factory() as session:
        async with session.begin():
            result = await push_questionnaires_to_metsights(
                session,
                sync_service=sync_service,
                questionnaire_repository=QuestionnaireRepository(),
                as_of=as_of,
                dry_run=dry_run,
                on_progress=on_progress,
            )

    await engine.dispose()
    return result


def _parse_as_of(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Expected YYYY-MM-DD.") from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Push questionnaire answers from the database to MetSights for all "
            "participants in running engagements (Metsights Basic/Pro and FitPrint)."
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
        help="Report what would be pushed without calling MetSights.",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_as_of,
        default=None,
        metavar="YYYY-MM-DD",
        help="Override 'today' date for summary metadata. Useful for testing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_push(
            yes=args.yes,
            dry_run=args.dry_run,
            as_of=args.as_of,
        )
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    print(
        f"\nPush questionnaires to MetSights ({mode}):\n"
        f"  as_of={result['as_of']}\n"
        f"  matched={result['matched']}, pushed={result['pushed']}, "
        f"skipped={result['skipped']}, failed={result['failed']}"
    )
    details = result.get("details", [])
    if details:
        print(f"\n  {'INSTANCE':>10}  {'USER':>8}  {'ENG':>6}  {'ACTION':>10}  REASON")
        print(f"  {'─' * 10}  {'─' * 8}  {'─' * 6}  {'─' * 10}  {'─' * 40}")
        for d in details:
            print(
                f"  {d['assessment_instance_id']:>10}  {d['user_id']:>8}  "
                f"{d['engagement_id']:>6}  {d['action']:>10}  {d['reason']}"
            )
    print()
    return 0
