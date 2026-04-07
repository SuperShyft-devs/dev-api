"""Idempotent ORM upserts for diagnostics reference data (fixed IDs)."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from modules.diagnostics.models import (
    DiagnosticPackage,
    DiagnosticPackageReason,
    DiagnosticPackageSample,
    DiagnosticPackageTag,
    DiagnosticPackageTestGroup,
    DiagnosticTestGroup,
    DiagnosticTestGroupTest,
    HealthParameter,
    ParameterType,
)


def _bool_cell(v: str | None) -> bool:
    if v is None:
        return False
    return v.strip().lower() in ("true", "1", "yes", "t")


def _dec(v: str | None) -> Decimal | None:
    if v is None or not str(v).strip():
        return None
    try:
        return Decimal(str(v).strip())
    except Exception:
        return None


def _int(v: str | None) -> int | None:
    if v is None or not str(v).strip():
        return None
    try:
        return int(str(v).strip())
    except Exception:
        return None


def _str_or_none(v: str | None) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _parse_created_at(value: str | None) -> datetime | None:
    """Parse values like ``2026-03-17 7:03:01`` (single-digit hour allowed)."""
    if value is None or not str(value).strip():
        return None
    s = str(value).strip()
    try:
        date_part, time_part = s.split(" ", 1)
        y, mo, d = (int(x) for x in date_part.split("-", 2))
        tp = time_part.split(":")
        h = int(tp[0])
        mi = int(tp[1]) if len(tp) > 1 else 0
        if len(tp) > 2:
            sec_part = tp[2].split(".")[0]
            se = int(sec_part) if sec_part.strip() else 0
        else:
            se = 0
        return datetime(y, mo, d, h, mi, se, tzinfo=timezone.utc)
    except (ValueError, IndexError):
        return None


async def _apply_health_parameter_row(session: AsyncSession, raw: dict[str, str | None]) -> None:
    tid = _int(raw.get("test_id"))
    if tid is None:
        return
    ptype_raw = (raw.get("parameter_type") or "test").strip().lower()
    ptype = ParameterType.METRIC if ptype_raw == "metric" else ParameterType.TEST

    row = await session.get(HealthParameter, tid)
    if row is None:
        row = HealthParameter(test_id=tid)
        session.add(row)

    row.parameter_type = ptype
    row.test_name = (raw.get("test_name") or "").strip() or row.test_name
    av = raw.get("is_available")
    row.is_available = _bool_cell(av) if av is not None and str(av).strip() != "" else True
    row.display_order = _int(raw.get("display_order"))
    pk = raw.get("parameter_key")
    row.parameter_key = pk.strip() if pk and str(pk).strip() else None
    u = raw.get("unit")
    row.unit = u.strip() if u and str(u).strip() else None
    m = raw.get("meaning")
    row.meaning = m.strip() if m and str(m).strip() else None
    row.lower_range_male = _dec(raw.get("lower_range_male"))
    row.higher_range_male = _dec(raw.get("higher_range_male"))
    row.lower_range_female = _dec(raw.get("lower_range_female"))
    row.higher_range_female = _dec(raw.get("higher_range_female"))

    def _txt(key: str) -> str | None:
        v = raw.get(key)
        if v is None or not str(v).strip():
            return None
        return str(v).strip()

    row.causes_when_high = _txt("causes_when_high")
    row.causes_when_low = _txt("causes_when_low")
    row.effects_when_high = _txt("effects_when_high")
    row.effects_when_low = _txt("effects_when_low")
    row.what_to_do_when_low = _txt("what_to_do_when_low")
    row.what_to_do_when_high = _txt("what_to_do_when_high")


@dataclass(frozen=True)
class SeedDiagPackage:
    diagnostic_package_id: int
    reference_id: str | None
    package_name: str
    diagnostic_provider: str | None
    no_of_tests: int | None
    status: str | None
    created_at: datetime | None
    report_duration_hours: int | None
    collection_type: str | None
    about_text: str | None
    bookings_count: int | None
    price: Decimal | None
    original_price: Decimal | None
    is_most_popular: bool
    gender_suitability: str | None


@dataclass(frozen=True)
class SeedDiagReason:
    reason_id: int
    diagnostic_package_id: int
    display_order: int | None
    reason_text: str


@dataclass(frozen=True)
class SeedDiagTag:
    tag_id: int
    diagnostic_package_id: int
    tag_name: str
    display_order: int | None


@dataclass(frozen=True)
class SeedDiagGroup:
    group_id: int
    group_name: str
    display_order: int | None


@dataclass(frozen=True)
class SeedDiagGroupTest:
    id: int
    group_id: int
    test_id: int
    display_order: int | None


@dataclass(frozen=True)
class SeedDiagPackageTestGroup:
    id: int
    diagnostic_package_id: int
    group_id: int
    display_order: int | None


@dataclass(frozen=True)
class SeedDiagSample:
    sample_id: int
    diagnostic_package_id: int
    sample_type: str
    description: str | None
    display_order: int | None


async def upsert_diagnostic_packages(
    session: AsyncSession, rows: Iterable[SeedDiagPackage]
) -> None:
    for seed in rows:
        row = await session.get(DiagnosticPackage, seed.diagnostic_package_id)
        if row is None:
            row = DiagnosticPackage(diagnostic_package_id=seed.diagnostic_package_id)
            session.add(row)
        row.reference_id = seed.reference_id
        row.package_name = seed.package_name
        row.diagnostic_provider = seed.diagnostic_provider
        row.no_of_tests = seed.no_of_tests
        row.status = seed.status or "active"
        if seed.created_at is not None:
            row.created_at = seed.created_at
        row.report_duration_hours = seed.report_duration_hours
        row.collection_type = seed.collection_type
        row.about_text = seed.about_text
        row.bookings_count = seed.bookings_count if seed.bookings_count is not None else 0
        row.price = seed.price
        row.original_price = seed.original_price
        row.is_most_popular = seed.is_most_popular
        row.gender_suitability = seed.gender_suitability


async def upsert_diagnostic_groups(session: AsyncSession, rows: Iterable[SeedDiagGroup]) -> None:
    for seed in rows:
        row = await session.get(DiagnosticTestGroup, seed.group_id)
        if row is None:
            row = DiagnosticTestGroup(group_id=seed.group_id)
            session.add(row)
        row.group_name = seed.group_name
        row.display_order = seed.display_order


async def upsert_health_parameters_from_tsv(session: AsyncSession, tsv_path: Path) -> None:
    if not tsv_path.is_file():
        raise FileNotFoundError(f"Missing diagnostics TSV: {tsv_path}")

    with tsv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for raw in reader:
            await _apply_health_parameter_row(session, raw)


async def upsert_health_parameters_from_csv(session: AsyncSession, csv_path: Path) -> None:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Missing health_parameters CSV: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            await _apply_health_parameter_row(session, raw)


async def upsert_diagnostic_packages_from_csv(session: AsyncSession, csv_path: Path) -> None:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Missing diagnostic_package CSV: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            pid = _int(raw.get("diagnostic_package_id"))
            if pid is None:
                continue
            row = await session.get(DiagnosticPackage, pid)
            if row is None:
                row = DiagnosticPackage(diagnostic_package_id=pid)
                session.add(row)

            row.reference_id = _str_or_none(raw.get("reference_id"))
            row.package_name = (raw.get("package_name") or "").strip() or row.package_name
            row.diagnostic_provider = _str_or_none(raw.get("diagnostic_provider"))
            row.no_of_tests = _int(raw.get("no_of_tests"))
            st = raw.get("status")
            row.status = st.strip() if st and str(st).strip() else "active"
            created = _parse_created_at(raw.get("created_at"))
            if created is not None:
                row.created_at = created
            row.report_duration_hours = _int(raw.get("report_duration_hours"))
            row.collection_type = _str_or_none(raw.get("collection_type"))
            about = raw.get("about_text")
            row.about_text = about.strip() if about and str(about).strip() else None
            bc = raw.get("bookings_count")
            row.bookings_count = _int(bc) if bc is not None and str(bc).strip() != "" else 0
            row.price = _dec(raw.get("price"))
            row.original_price = _dec(raw.get("original_price"))
            imp = raw.get("is_most_popular")
            row.is_most_popular = (
                _bool_cell(imp) if imp is not None and str(imp).strip() != "" else False
            )
            row.gender_suitability = _str_or_none(raw.get("gender_suitability"))


async def upsert_diagnostic_groups_from_csv(session: AsyncSession, csv_path: Path) -> None:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Missing diagnostic_test_groups CSV: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            gid = _int(raw.get("group_id"))
            if gid is None:
                continue
            row = await session.get(DiagnosticTestGroup, gid)
            if row is None:
                row = DiagnosticTestGroup(group_id=gid)
                session.add(row)
            row.group_name = (raw.get("group_name") or "").strip() or row.group_name
            row.display_order = _int(raw.get("display_order"))


async def upsert_diagnostic_group_tests_from_csv(session: AsyncSession, csv_path: Path) -> None:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Missing diagnostic_test_group_tests CSV: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            rid = _int(raw.get("id"))
            if rid is None:
                continue
            row = await session.get(DiagnosticTestGroupTest, rid)
            if row is None:
                row = DiagnosticTestGroupTest(id=rid)
                session.add(row)
            gid = _int(raw.get("group_id"))
            tid = _int(raw.get("test_id"))
            if gid is None or tid is None:
                continue
            row.group_id = gid
            row.test_id = tid
            row.display_order = _int(raw.get("display_order"))


async def upsert_diagnostic_package_reasons_from_csv(session: AsyncSession, csv_path: Path) -> None:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Missing diagnostic_package_reasons CSV: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            rid = _int(raw.get("reason_id"))
            if rid is None:
                continue
            row = await session.get(DiagnosticPackageReason, rid)
            if row is None:
                row = DiagnosticPackageReason(reason_id=rid)
                session.add(row)
            pkg_id = _int(raw.get("diagnostic_package_id"))
            if pkg_id is None:
                continue
            row.diagnostic_package_id = pkg_id
            row.display_order = _int(raw.get("display_order"))
            rt = raw.get("reason_text")
            row.reason_text = (rt or "").strip() if rt is not None else ""


async def upsert_diagnostic_package_tags_from_csv(session: AsyncSession, csv_path: Path) -> None:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Missing diagnostic_package_tags CSV: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            tid = _int(raw.get("tag_id"))
            if tid is None:
                continue
            row = await session.get(DiagnosticPackageTag, tid)
            if row is None:
                row = DiagnosticPackageTag(tag_id=tid)
                session.add(row)
            pkg_id = _int(raw.get("diagnostic_package_id"))
            if pkg_id is None:
                continue
            row.diagnostic_package_id = pkg_id
            row.tag_name = (raw.get("tag_name") or "").strip() or row.tag_name
            row.display_order = _int(raw.get("display_order"))


async def upsert_diagnostic_package_test_groups_from_csv(session: AsyncSession, csv_path: Path) -> None:
    if not csv_path.is_file():
        raise FileNotFoundError(f"Missing diagnostic_package_test_groups CSV: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            rid = _int(raw.get("id"))
            if rid is None:
                continue
            row = await session.get(DiagnosticPackageTestGroup, rid)
            if row is None:
                row = DiagnosticPackageTestGroup(id=rid)
                session.add(row)
            pkg_id = _int(raw.get("diagnostic_package_id"))
            gid = _int(raw.get("group_id"))
            if pkg_id is None or gid is None:
                continue
            row.diagnostic_package_id = pkg_id
            row.group_id = gid
            row.display_order = _int(raw.get("display_order"))


def validate_diagnostics_csv_fks(directory: Path) -> list[str]:
    """Read CSVs on disk and return human-readable FK warnings (no DB)."""
    warnings: list[str] = []

    def _read(name: str) -> list[dict[str, str]]:
        path = directory / name
        if not path.is_file():
            warnings.append(f"Missing file for validation: {path}")
            return []
        with path.open(newline="", encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))

    hp_rows = _read("health_parameters.csv")
    hp_ids = {int(r["test_id"]) for r in hp_rows if r.get("test_id", "").strip().isdigit()}

    pkg_rows = _read("diagnostic_package.csv")
    pkg_ids = {
        int(r["diagnostic_package_id"])
        for r in pkg_rows
        if r.get("diagnostic_package_id", "").strip().isdigit()
    }

    grp_rows = _read("diagnostic_test_groups.csv")
    grp_ids = {int(r["group_id"]) for r in grp_rows if r.get("group_id", "").strip().isdigit()}

    for r in _read("diagnostic_test_group_tests.csv"):
        tid = r.get("test_id", "").strip()
        gid = r.get("group_id", "").strip()
        if tid.isdigit() and int(tid) not in hp_ids:
            warnings.append(f"diagnostic_test_group_tests: test_id {tid} not in health_parameters.csv")
        if gid.isdigit() and int(gid) not in grp_ids:
            warnings.append(f"diagnostic_test_group_tests: group_id {gid} not in diagnostic_test_groups.csv")

    for r in _read("diagnostic_package_test_groups.csv"):
        pid = r.get("diagnostic_package_id", "").strip()
        gid = r.get("group_id", "").strip()
        if pid.isdigit() and int(pid) not in pkg_ids:
            warnings.append(
                f"diagnostic_package_test_groups: diagnostic_package_id {pid} not in diagnostic_package.csv"
            )
        if gid.isdigit() and int(gid) not in grp_ids:
            warnings.append(
                f"diagnostic_package_test_groups: group_id {gid} not in diagnostic_test_groups.csv"
            )

    for r in _read("diagnostic_package_reasons.csv"):
        pid = r.get("diagnostic_package_id", "").strip()
        if pid.isdigit() and int(pid) not in pkg_ids:
            warnings.append(
                f"diagnostic_package_reasons: diagnostic_package_id {pid} not in diagnostic_package.csv"
            )

    for r in _read("diagnostic_package_tags.csv"):
        pid = r.get("diagnostic_package_id", "").strip()
        if pid.isdigit() and int(pid) not in pkg_ids:
            warnings.append(
                f"diagnostic_package_tags: diagnostic_package_id {pid} not in diagnostic_package.csv"
            )

    return sorted(set(warnings))


REQUIRED_DIAGNOSTICS_CSV_FILES: tuple[str, ...] = (
    "health_parameters.csv",
    "diagnostic_package.csv",
    "diagnostic_test_groups.csv",
    "diagnostic_test_group_tests.csv",
    "diagnostic_package_test_groups.csv",
    "diagnostic_package_tags.csv",
    "diagnostic_package_reasons.csv",
)


async def seed_diagnostics_reference_from_csv_dir(session: AsyncSession, directory: Path) -> None:
    """Load diagnostics reference tables from CSV exports (idempotent upserts)."""
    d = directory.resolve()
    missing = [name for name in REQUIRED_DIAGNOSTICS_CSV_FILES if not (d / name).is_file()]
    if missing:
        raise FileNotFoundError(
            f"Diagnostics CSV directory {d} is missing: {', '.join(missing)}. "
            f"Expected checked-in files under dev-api/db/seed/csv."
        )

    for w in validate_diagnostics_csv_fks(d):
        print(f"CSV validation warning: {w}")

    await upsert_diagnostic_packages_from_csv(session, d / "diagnostic_package.csv")
    await upsert_health_parameters_from_csv(session, d / "health_parameters.csv")
    await upsert_diagnostic_groups_from_csv(session, d / "diagnostic_test_groups.csv")
    await upsert_diagnostic_group_tests_from_csv(session, d / "diagnostic_test_group_tests.csv")
    await upsert_diagnostic_package_reasons_from_csv(session, d / "diagnostic_package_reasons.csv")
    await upsert_diagnostic_package_tags_from_csv(session, d / "diagnostic_package_tags.csv")
    await upsert_diagnostic_package_test_groups_from_csv(
        session, d / "diagnostic_package_test_groups.csv"
    )


async def upsert_diagnostic_group_tests(
    session: AsyncSession, rows: Iterable[SeedDiagGroupTest]
) -> None:
    for seed in rows:
        row = await session.get(DiagnosticTestGroupTest, seed.id)
        if row is None:
            row = DiagnosticTestGroupTest(id=seed.id)
            session.add(row)
        row.group_id = seed.group_id
        row.test_id = seed.test_id
        row.display_order = seed.display_order


async def upsert_diagnostic_package_reasons(
    session: AsyncSession, rows: Iterable[SeedDiagReason]
) -> None:
    for seed in rows:
        row = await session.get(DiagnosticPackageReason, seed.reason_id)
        if row is None:
            row = DiagnosticPackageReason(reason_id=seed.reason_id)
            session.add(row)
        row.diagnostic_package_id = seed.diagnostic_package_id
        row.display_order = seed.display_order
        row.reason_text = seed.reason_text


async def upsert_diagnostic_package_tags(
    session: AsyncSession, rows: Iterable[SeedDiagTag]
) -> None:
    for seed in rows:
        row = await session.get(DiagnosticPackageTag, seed.tag_id)
        if row is None:
            row = DiagnosticPackageTag(tag_id=seed.tag_id)
            session.add(row)
        row.diagnostic_package_id = seed.diagnostic_package_id
        row.tag_name = seed.tag_name
        row.display_order = seed.display_order


async def upsert_diagnostic_package_samples(
    session: AsyncSession, rows: Iterable[SeedDiagSample]
) -> None:
    for seed in rows:
        row = await session.get(DiagnosticPackageSample, seed.sample_id)
        if row is None:
            row = DiagnosticPackageSample(sample_id=seed.sample_id)
            session.add(row)
        row.diagnostic_package_id = seed.diagnostic_package_id
        row.sample_type = seed.sample_type
        row.description = seed.description
        row.display_order = seed.display_order


async def upsert_diagnostic_package_test_groups(
    session: AsyncSession, rows: Iterable[SeedDiagPackageTestGroup]
) -> None:
    for seed in rows:
        row = await session.get(DiagnosticPackageTestGroup, seed.id)
        if row is None:
            row = DiagnosticPackageTestGroup(id=seed.id)
            session.add(row)
        row.diagnostic_package_id = seed.diagnostic_package_id
        row.group_id = seed.group_id
        row.display_order = seed.display_order


async def reset_diagnostics_sequences(session: AsyncSession) -> None:
    stmts = [
        ("diagnostic_package", "diagnostic_package_id"),
        ("diagnostic_test_groups", "group_id"),
        ("health_parameters", "test_id"),
        ("diagnostic_test_group_tests", "id"),
        ("diagnostic_package_test_groups", "id"),
        ("diagnostic_package_reasons", "reason_id"),
        ("diagnostic_package_tags", "tag_id"),
        ("diagnostic_package_samples", "sample_id"),
    ]
    for table, col in stmts:
        await session.execute(
            text(
                f"""
            SELECT setval(
                pg_get_serial_sequence('{table}', '{col}'),
                COALESCE((SELECT MAX({col}) FROM {table}), 1),
                true
            )
            """
            )
        )
