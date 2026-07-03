"""Migrate legacy blood_parameters storage to canonical provider + questionnaire paths.

Entrypoint: ``python -m db.jobs.migrate_blood_parameters --yes``
"""

from __future__ import annotations

import argparse
import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.config import settings
from modules.reports.migrate_blood_parameters import migrate_blood_parameters


async def run_migrate(*, yes: bool, dry_run: bool) -> dict:
    settings.validate()
    if not yes and not dry_run:
        raise SystemExit(
            "Refusing to run without explicit confirmation. Re-run with --yes to apply changes, "
            "or --dry-run to preview."
        )

    engine = create_async_engine(settings.DATABASE_URL, echo=False, pool_pre_ping=True)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with session_factory() as session:
        result = await migrate_blood_parameters(session, dry_run=dry_run)
        if not dry_run:
            await session.commit()

    await engine.dispose()
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Migrate legacy blood_parameters rows.")
    parser.add_argument("--yes", action="store_true", help="Apply changes.")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(run_migrate(yes=args.yes, dry_run=args.dry_run))
    mode = "dry-run" if result["dry_run"] else "applied"
    print(f"\nMigrate blood parameters ({mode}):")
    for key in (
        "scanned",
        "skipped_canonical",
        "migrated_healthians",
        "migrated_metsights",
        "cleared_empty",
        "skipped_metsights_pending_questionnaire",
        "failed",
        "questionnaire_definitions_loaded",
    ):
        print(f"  {key}={result.get(key)}")
    unmapped = result.get("unmapped_metsights_keys") or []
    if unmapped:
        print(f"  unmapped_keys={sorted(set(unmapped))}")
    errors = result.get("errors") or []
    if errors:
        print(f"  errors={len(errors)}")
        for err in errors[:20]:
            print(f"    report_id={err.get('report_id')}: {err.get('reason')}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
