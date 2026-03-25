from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.diagnostics.models import (
    DiagnosticPackage,
    DiagnosticTest,
    DiagnosticTestGroup,
    DiagnosticTestGroupTest,
)

from db.seed_data.data import (
    LIVER_PROFILE_TEST_NAMES,
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


async def sync_liver_profile_tests(session: AsyncSession) -> None:
    """Make Liver profile contain exactly LIVER_PROFILE_TEST_NAMES (skip missing tests).

    Removes any other tests previously assigned to this group so re-seeding matches the UI.
    """
    group_result = await session.execute(
        select(DiagnosticTestGroup)
        .where(DiagnosticTestGroup.group_name == "Liver profile")
        .limit(1)
    )
    group = group_result.scalar_one_or_none()
    if group is None:
        return

    resolved_ids: list[int] = []
    for test_name in LIVER_PROFILE_TEST_NAMES:
        test_result = await session.execute(
            select(DiagnosticTest).where(DiagnosticTest.test_name == test_name).limit(1)
        )
        test = test_result.scalar_one_or_none()
        if test is not None:
            resolved_ids.append(int(test.test_id))

    await session.execute(
        delete(DiagnosticTestGroupTest).where(DiagnosticTestGroupTest.group_id == group.group_id)
    )

    for order, tid in enumerate(resolved_ids, start=1):
        session.add(
            DiagnosticTestGroupTest(
                group_id=group.group_id,
                test_id=tid,
                display_order=order,
            )
        )


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

