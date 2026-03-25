from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.diagnostics.models import DiagnosticPackage, DiagnosticTest, DiagnosticTestGroup

from db.seed_data.data import (
    SeedDiagnosticPackage,
    SeedDiagnosticTest,
    SeedDiagnosticTestGroup,
)


async def upsert_diagnostic_test_groups(
    session: AsyncSession, groups: Iterable[SeedDiagnosticTestGroup]
) -> None:
    for seed in groups:
        result = await session.execute(
            select(DiagnosticTestGroup)
            .where(DiagnosticTestGroup.group_name == seed.group_name)
            .limit(1)
        )
        row = result.scalar_one_or_none()

        if row is None:
            session.add(DiagnosticTestGroup(group_name=seed.group_name))
        else:
            # Only group_name is part of your request for now.
            row.group_name = seed.group_name


async def upsert_diagnostic_test_packages(
    session: AsyncSession, packages: Iterable[SeedDiagnosticPackage]
) -> None:
    for seed in packages:
        result = await session.execute(
            select(DiagnosticPackage)
            .where(DiagnosticPackage.package_name == seed.package_name)
            .limit(1)
        )
        row = result.scalar_one_or_none()

        if row is None:
            session.add(DiagnosticPackage(package_name=seed.package_name))
        else:
            # Only package_name is part of your request for now.
            row.package_name = seed.package_name


async def upsert_diagnostic_tests(session: AsyncSession, tests: Iterable[SeedDiagnosticTest]) -> None:
    for seed in tests:
        result = await session.execute(
            select(DiagnosticTest).where(DiagnosticTest.test_name == seed.test_name).limit(1)
        )
        row = result.scalar_one_or_none()

        if row is None:
            session.add(DiagnosticTest(**_build_test_payload(seed)))
            continue

        payload = _build_test_payload(seed)
        for key, value in payload.items():
            if value is not None:
                setattr(row, key, value)


def _build_test_payload(seed: SeedDiagnosticTest) -> dict:
    # We keep `is_available` as default from the model; seeding is only what you asked for.
    return {
        "test_name": seed.test_name,
        "parameter_key": seed.parameter_key,
        "unit": seed.unit,
        "meaning": seed.meaning,
        "lower_range_male": seed.lower_range_male,
        "higher_range_male": seed.higher_range_male,
        "lower_range_female": seed.lower_range_female,
        "higher_range_female": seed.higher_range_female,
        "causes_when_high": seed.causes_when_high,
        "causes_when_low": seed.causes_when_low,
        "effects_when_high": seed.effects_when_high,
        "effects_when_low": seed.effects_when_low,
        "what_to_do_when_low": seed.what_to_do_when_low,
        "what_to_do_when_high": seed.what_to_do_when_high,
    }

