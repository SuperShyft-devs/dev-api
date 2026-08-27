"""Link duplicate primary users that share a phone number.

When two accounts store the same Indian number as ``9769746493`` and
``+919769746493``, OTP/booking lookup finds both primaries and returns
``Multiple accounts match this phone number``.

This job keeps the account with more engagement enrollments as the main
profile (``parent_id`` null) and links the others as sub-accounts.

Production example (Linux cron, IST) — preview then apply::

    0 2 * * 0 cd /path/to/dev-api && TZ=Asia/Kolkata /path/to/venv/bin/python -m db.jobs.link_duplicate_phone_accounts --dry-run --report /var/log/supershyft/link-duplicate-phones-dry-run.json >> /var/log/supershyft/link-duplicate-phones.log 2>&1

One-off on the server::

    python -m db.jobs.link_duplicate_phone_accounts --dry-run
    python -m db.jobs.link_duplicate_phone_accounts --yes
    python -m db.jobs.link_duplicate_phone_accounts --yes --phone 9769746493
    python -m db.jobs.link_duplicate_phone_accounts --yes --require-similar-name

Entrypoint: ``python -m db.jobs.link_duplicate_phone_accounts``
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import async_sessionmaker

from core.config import settings
from db.engine import create_job_engine, job_session_factory
from modules.maintenance.link_duplicate_phone_accounts import link_duplicate_phone_accounts

logger = logging.getLogger(__name__)


async def run_link(
    *,
    yes: bool,
    dry_run: bool,
    require_similar_name: bool,
    phone: str | None,
    limit: int | None,
    report_path: str | None,
) -> dict:
    settings.validate()
    if not yes and not dry_run:
        raise SystemExit(
            "Refusing to run without explicit confirmation. Re-run with --yes to apply changes, "
            "or --dry-run to preview."
        )

    engine = create_job_engine()
    session_factory = job_session_factory(engine)

    async with session_factory() as session:
        result = await link_duplicate_phone_accounts(
            session,
            dry_run=dry_run,
            require_similar_name=require_similar_name,
            phone=phone,
            limit=limit,
        )
        if not dry_run:
            await session.commit()

    await engine.dispose()

    if report_path:
        path = Path(report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
        logger.info("Wrote report to %s", path)

    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Link extra primary users that share a last-10-digit phone number. "
            "The account with more engagement enrollments stays main; others become sub-accounts."
        )
    )
    parser.add_argument("--yes", action="store_true", help="Apply changes.")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing.")
    parser.add_argument(
        "--require-similar-name",
        action="store_true",
        help="Skip groups whose primary names do not look like the same person.",
    )
    parser.add_argument(
        "--phone",
        type=str,
        default=None,
        help="Only process the group whose last 10 digits match this number.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max duplicate groups to consider.",
    )
    parser.add_argument(
        "--report",
        type=str,
        default=None,
        help="Write JSON report to this path.",
    )
    return parser


def _print_summary(result: dict) -> None:
    mode = "dry-run" if result.get("dry_run") else "applied"
    print(f"\nLink duplicate phone accounts ({mode}):")
    print(f"  scanned_groups={result.get('scanned_groups')}")
    print(f"  linked_groups={result.get('linked_groups')}")
    print(f"  skipped_groups={result.get('skipped_groups')}")
    if result.get("phone_key"):
        print(f"  phone_key={result['phone_key']}")

    for item in result.get("linked") or []:
        main_id = item["main_user_id"]
        subs = ",".join(str(uid) for uid in item["sub_user_ids"])
        print(f"  linked {item['phone_key']}: main={main_id} subs={subs}")
    for item in result.get("skipped") or []:
        ids = ",".join(str(u["user_id"]) for u in item.get("users") or [])
        print(f"  skipped {item['phone_key']}: {item['reason']} users={ids}")
    print()


def main(argv: list[str] | None = None) -> int:
    load_dotenv(override=False)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_link(
            yes=args.yes,
            dry_run=args.dry_run,
            require_similar_name=args.require_similar_name,
            phone=args.phone,
            limit=args.limit,
            report_path=args.report,
        )
    )
    _print_summary(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
