"""Transition engagement statuses job.

Activates scheduled engagements whose start_date has arrived, and completes running
engagements whose end_date has passed. Intended to run once daily via an external
scheduler (cron, Task Scheduler, etc.).

Production example (Linux cron, 2:00 AM daily):

    0 2 * * * cd /path/to/dev-api && /path/to/venv/bin/python -m db.jobs.transition_engagement_statuses --yes >> /var/log/supershyft/transition-engagement-statuses.log 2>&1

Entrypoint: ``python -m db.jobs.transition_engagement_statuses --yes``
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.config import settings
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import EngagementsService


async def run_transition_engagement_statuses(
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

    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    service = EngagementsService(
        EngagementsRepository(),
        audit_service=AuditService(AuditRepository()),
    )

    async with session_factory() as session:
        async with session.begin():
            result = await service.transition_engagement_statuses(
                session,
                as_of=as_of,
                dry_run=dry_run,
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
            "Activate scheduled engagements whose start_date has arrived, and complete "
            "running engagements whose end_date has passed. Safe to re-run daily (idempotent)."
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
        help="Report how many engagements would be updated without writing.",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_as_of,
        default=None,
        metavar="YYYY-MM-DD",
        help="Reference date for status transitions (default: today). Useful for testing or backfill.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_transition_engagement_statuses(
            yes=args.yes,
            dry_run=args.dry_run,
            as_of=args.as_of,
        )
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    print(
        f"Transition engagement statuses ({mode}): as_of={result['as_of']}, "
        f"activated_count={result['activated_count']}, completed_count={result['completed_count']}"
    )
    return 0
