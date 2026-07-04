"""Migrate legacy blood_parameters storage to package-shaped groups.

Converts Healthians / canonical / Metsights-flat blobs (and rows with only
``blood_report_raw``) into the list-of-groups format used by the blood-parameters API.

Entrypoint::

    python -m db.jobs.migrate_blood_parameters --dry-run
    python -m db.jobs.migrate_blood_parameters --yes
"""

from __future__ import annotations

import argparse
import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.config import settings
from modules.reports.migrate_blood_parameters import migrate_blood_parameters


async def run_migrate(*, yes: bool, dry_run: bool, batch_size: int) -> dict:
    settings.validate()
    if not yes and not dry_run:
        raise SystemExit(
            "Refusing to run without explicit confirmation. Re-run with --yes to apply changes, "
            "or --dry-run to preview."
        )

    engine = create_async_engine(settings.DATABASE_URL, echo=False, pool_pre_ping=True)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with session_factory() as session:
        result = await migrate_blood_parameters(
            session,
            dry_run=dry_run,
            batch_size=batch_size,
        )
        if not dry_run:
            await session.commit()

    await engine.dispose()
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Migrate all individual_health_report.blood_parameters rows to the "
            "package-shaped groups format (list of groups, each with list of tests)."
        )
    )
    parser.add_argument("--yes", action="store_true", help="Apply changes.")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing.")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Rows per batch (default: 200).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_migrate(yes=args.yes, dry_run=args.dry_run, batch_size=args.batch_size)
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    print(f"\nMigrate blood parameters → grouped format ({mode}):")
    for key in (
        "scanned",
        "skipped_grouped",
        "migrated_healthians",
        "migrated_canonical",
        "migrated_metsights",
        "migrated_from_raw",
        "cleared_empty",
        "skipped_no_package",
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
        if len(errors) > 20:
            print(f"    ... and {len(errors) - 20} more")
    print()
    return 1 if result.get("failed") else 0


if __name__ == "__main__":
    raise SystemExit(main())
