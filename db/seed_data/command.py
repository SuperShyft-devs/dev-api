"""Diagnostic reference data seeding command.

Production guidance (aligned with instructions/*):
- Seeding must be explicit (never on app startup).
- Seeds must be idempotent (safe to re-run).
- Use ORM, not raw SQL.
- Run after migrations.

Entrypoint: `python -m db.seed-data --yes`
"""

from __future__ import annotations

import argparse
import asyncio

from core.config import settings

from db.seed_data.data import DIAGNOSTIC_TEST_GROUPS, DIAGNOSTIC_TEST_PACKAGES, DIAGNOSTIC_TESTS
from db.seed_data.operations import (
    upsert_diagnostic_test_groups,
    upsert_diagnostic_test_packages,
    upsert_diagnostic_tests,
)


async def seed_diagnostic_reference_data(*, yes: bool) -> None:
    """Seed diagnostic reference data (groups, packages, tests).

    Args:
        yes: Must be True to perform any writes.
    """
    settings.validate()

    if not yes:
        raise SystemExit(
            "Refusing to seed without explicit confirmation. Re-run with --yes to apply changes."
        )

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with session_factory() as session:
        async with session.begin():
            await upsert_diagnostic_test_groups(session, DIAGNOSTIC_TEST_GROUPS)
            await upsert_diagnostic_test_packages(session, DIAGNOSTIC_TEST_PACKAGES)
            await upsert_diagnostic_tests(session, DIAGNOSTIC_TESTS)

            print("Seeded diagnostic groups, packages, and tests")

    await engine.dispose()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seed Supershyft diagnostic reference data (idempotent, ORM-based)."
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply changes. Without this flag, the command exits without writing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    asyncio.run(seed_diagnostic_reference_data(yes=args.yes))
    return 0

