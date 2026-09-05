"""Retry failed notification dispatches from the last N hours.

Throttles retries to avoid Gmail SMTP rate limits (454 too many login attempts).
Creates new notification rows via the standard dispatch path; does not mutate
failed rows.

Production example (Linux cron, hourly):

    0 * * * * cd /var/www/backend/api && ./venv/bin/python -m db.jobs.retry_failed_notifications --yes --delay-seconds 4 --limit 400 >> /var/log/retry-failed-notifications.log 2>&1

Entrypoint: ``python -m db.jobs.retry_failed_notifications --yes``
"""

from __future__ import annotations

import argparse
import asyncio

from core.config import settings
from db.engine import create_job_engine, job_session_factory
from modules.notifications.repository import NotificationsRepository
from modules.notifications.retry_failed import (
    DEFAULT_DELAY_SECONDS,
    DEFAULT_HOURS,
    DEFAULT_LIMIT,
    DEFAULT_MAX_CONSECUTIVE_FAILURES,
    retry_failed_notifications,
)
from modules.notifications.service import NotificationsService


async def run_retry_failed_notifications(
    *,
    yes: bool,
    dry_run: bool,
    hours: int,
    channel: str,
    service_key: str | None,
    limit: int,
    delay_seconds: float,
    max_consecutive_failures: int,
) -> dict:
    settings.validate()

    if not yes and not dry_run:
        raise SystemExit(
            "Refusing to run without explicit confirmation. Re-run with --yes to apply changes, "
            "or --dry-run to preview."
        )

    engine = create_job_engine()
    session_factory = job_session_factory(engine)
    notifications_service = NotificationsService(NotificationsRepository())

    result = await retry_failed_notifications(
        session_factory,
        notifications_service=notifications_service,
        hours=hours,
        channel=channel,
        service_key=service_key,
        limit=limit,
        delay_seconds=delay_seconds,
        max_consecutive_failures=max_consecutive_failures,
        dry_run=dry_run,
    )

    await engine.dispose()
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Retry failed email notifications from the last N hours, one at a time "
            "with a delay between dispatches."
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
        help="List candidates that would be retried without dispatching.",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=DEFAULT_HOURS,
        metavar="HOURS",
        help=f"Look back window for failed notifications (default: {DEFAULT_HOURS}).",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        metavar="SECONDS",
        help=f"Pause between retries (default: {DEFAULT_DELAY_SECONDS}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        metavar="N",
        help=f"Maximum retries per run (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--channel",
        default="email",
        choices=("email", "whatsapp"),
        help="Notification channel to retry (default: email).",
    )
    parser.add_argument(
        "--service-key",
        default=None,
        metavar="KEY",
        help="Optional service_key filter for targeted reruns.",
    )
    parser.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=DEFAULT_MAX_CONSECUTIVE_FAILURES,
        metavar="N",
        help=(
            "Stop the run after this many consecutive dispatch failures "
            f"(default: {DEFAULT_MAX_CONSECUTIVE_FAILURES})."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_retry_failed_notifications(
            yes=args.yes,
            dry_run=args.dry_run,
            hours=args.hours,
            channel=args.channel,
            service_key=args.service_key,
            limit=args.limit,
            delay_seconds=args.delay_seconds,
            max_consecutive_failures=args.max_consecutive_failures,
        )
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    print(
        f"\nRetry failed notifications ({mode}):\n"
        f"  hours={result['hours']}\n"
        f"  channel={result['channel']}\n"
        f"  service_key={result['service_key']}\n"
        f"  limit={result['limit']}\n"
        f"  delay_seconds={result['delay_seconds']}\n"
        f"  matched={result['matched']}\n"
        f"  retried={result['retried']}\n"
        f"  skipped={result['skipped']}\n"
        f"  failed={result['failed']}\n"
        f"  stopped_early={result['stopped_early']}\n"
    )
    for row in result.get("details") or []:
        print(f"  {row}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
