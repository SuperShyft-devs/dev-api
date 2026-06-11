"""Auto-import questionnaire answers from MetSights for running engagements.

Checks each incomplete assessment instance, queries MetSights sub-resources
for completion, and imports answers when any sub-resource is marked complete.

Entrypoint: ``python -m db.jobs.import_metsights_answers --yes``
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.config import settings
from modules.assessments.dependencies import get_assessments_service
from modules.engagements.dependencies import get_engagements_service
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import MetsightsSyncService
from modules.notifications.import_metsights_answers import import_metsights_answers
from modules.platform_settings.dependencies import get_platform_settings_service_readonly
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.repository import UsersRepository


async def run_import(
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

    async with session_factory() as session:
        async with session.begin():
            result = await import_metsights_answers(
                session,
                metsights_service=metsights_service,
                sync_service=sync_service,
                questionnaire_repository=QuestionnaireRepository(),
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
        description="Auto-import questionnaire answers from MetSights for incomplete assessment instances."
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply changes. Without this flag (and without --dry-run), the command exits without writing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be imported without making changes.",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_as_of,
        default=None,
        metavar="YYYY-MM-DD",
        help="Override 'today' date. Useful for testing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_import(
            yes=args.yes,
            dry_run=args.dry_run,
            as_of=args.as_of,
        )
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    print(
        f"\nImport MetSights answers ({mode}):\n"
        f"  as_of={result['as_of']}\n"
        f"  matched={result['matched']}, imported={result['imported']}, "
        f"skipped={result['skipped']}, failed={result['failed']}"
    )
    details = result.get("details", [])
    if details:
        print(f"\n  {'INSTANCE':>10}  {'USER':>8}  {'ACTION':>10}  REASON")
        print(f"  {'─' * 10}  {'─' * 8}  {'─' * 10}  {'─' * 40}")
        for d in details:
            print(
                f"  {d['assessment_instance_id']:>10}  {d['user_id']:>8}  "
                f"{d['action']:>10}  {d['reason']}"
            )
    print()
    return 0
