"""Expire notifications stuck in pending without an n8n callback.

Entrypoint: ``python -m db.jobs.expire_stale_notifications --yes``
"""

from __future__ import annotations

import argparse
import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.config import settings
from modules.notifications.expire_stale import (
    DEFAULT_PENDING_TIMEOUT_HOURS,
    expire_stale_notifications,
)
from modules.notifications.repository import NotificationsRepository


async def run_expire(*, yes: bool, dry_run: bool, timeout_hours: int | None) -> dict:
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

    async with session_factory() as session:
        result = await expire_stale_notifications(
            session,
            repository=NotificationsRepository(),
            timeout_hours=timeout_hours,
            dry_run=dry_run,
        )
        if not dry_run:
            await session.commit()

    await engine.dispose()
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Expire notifications stuck in pending without a callback."
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply changes. Without this flag (and without --dry-run), the command exits without writing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be expired without making changes.",
    )
    parser.add_argument(
        "--timeout-hours",
        type=int,
        default=None,
        metavar="HOURS",
        help=(
            "Pending age threshold in hours. "
            f"Defaults to {DEFAULT_PENDING_TIMEOUT_HOURS}."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_expire(
            yes=args.yes,
            dry_run=args.dry_run,
            timeout_hours=args.timeout_hours,
        )
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    print(
        f"\nExpire stale notifications ({mode}):\n"
        f"  timeout_hours={result['timeout_hours']}\n"
        f"  cutoff={result['cutoff']}\n"
        f"  expired={result['expired']}\n"
        f"  matched={result['would_expire']}"
    )
    ids = result.get("notification_ids") or []
    if ids:
        print(f"  notification_ids={ids}")
    print()
    return 0
