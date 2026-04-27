"""Database seeding command.

Production guidance (aligned with instructions/*):
- Seeding must be explicit (never on app startup).
- Seeds must be idempotent (safe to re-run).
- Use ORM, not raw SQL.
- Run after migrations.

Entrypoint: `python -m db.seed --yes`
"""

from __future__ import annotations

import argparse
import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.config import settings
from db.seed.data import (
    DEFAULT_ASSESSMENT_PACKAGES,
    DEFAULT_CATEGORIES,
    DEFAULT_CATEGORY_QUESTIONS,
    DEFAULT_EMPLOYEES,
    DEFAULT_OPTIONS,
    DEFAULT_PACKAGE_CATEGORIES,
    DEFAULT_QUESTIONS,
    DEFAULT_USERS,
)
from db.seed.diagnostics_csv import resolve_diagnostics_csv_dir
from db.seed.diagnostics_operations import (
    reset_diagnostics_sequences,
    seed_diagnostics_reference_from_csv_dir,
)
from db.seed.operations import (
    delete_options_for_question_ids,
    reset_sequences,
    upsert_assessment_packages,
    upsert_categories,
    upsert_category_questions,
    upsert_default_platform_settings,
    upsert_employees,
    upsert_options,
    upsert_package_categories,
    upsert_questions,
    upsert_users,
)


async def seed_reference_data(*, yes: bool) -> None:
    """Seed reference data.

    Intended to be run after migrations.

    Args:
        yes: Must be True to perform any writes.
    """
    settings.validate()

    if not yes:
        raise SystemExit(
            "Refusing to seed without explicit confirmation. Re-run with --yes to apply changes."
        )

    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with session_factory() as session:
        async with session.begin():
            await upsert_users(session, DEFAULT_USERS)
            await upsert_employees(session, DEFAULT_EMPLOYEES)
            await upsert_assessment_packages(session, DEFAULT_ASSESSMENT_PACKAGES)
            await upsert_categories(session, DEFAULT_CATEGORIES)
            await upsert_questions(session, DEFAULT_QUESTIONS)
            await delete_options_for_question_ids(session, (q.question_id for q in DEFAULT_QUESTIONS))
            await upsert_category_questions(session, DEFAULT_CATEGORY_QUESTIONS)
            await upsert_options(session, DEFAULT_OPTIONS)
            await upsert_package_categories(session, DEFAULT_PACKAGE_CATEGORIES)

            csv_dir = resolve_diagnostics_csv_dir()
            await seed_diagnostics_reference_from_csv_dir(session, csv_dir)
            await upsert_default_platform_settings(session)

            await reset_sequences(session)
            await reset_diagnostics_sequences(session)

            print("Seeded users, employees, assessment packages, and diagnostics reference data")
            print("Reset PostgreSQL sequences for auto-increment")

    await engine.dispose()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seed Supershyft reference data (idempotent, ORM-based)."
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply changes. Without this flag, the command exits without writing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    asyncio.run(seed_reference_data(yes=args.yes))
    return 0
