"""Normalize Healthians lab payloads into package-shaped ``blood_parameters`` storage."""

from __future__ import annotations

from typing import Any, Protocol, Sequence

from modules.reports.blood_parameters_schemas import (
    GroupedBloodParameterGroup,
    GroupedBloodParameterTest,
    is_canonical_blood_parameters,
    is_legacy_healthians_format,
)


class PackageTestRow(Protocol):
    test_id: int
    parameter_type: Any
    test_name: str
    parameter_key: str | None
    unit: str | None
    external_parameter_id: int | None


class PackageGroupRow(Protocol):
    group_name: str
    tests: Sequence[PackageTestRow]


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parameter_type_value(parameter_type: Any) -> str:
    if parameter_type is None:
        return "test"
    if hasattr(parameter_type, "value"):
        return str(parameter_type.value)
    return str(parameter_type)


def _build_digital_data_lookup(raw_customer: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    digital_data = raw_customer.get("digital_data")
    if not isinstance(digital_data, list):
        return lookup
    for entry in digital_data:
        if not isinstance(entry, dict):
            continue
        pid = str(entry.get("parameter_id") or "").strip()
        if pid:
            lookup[pid] = entry
    return lookup


def build_grouped_from_healthians(
    raw_customer: dict[str, Any],
    *,
    package_groups: Sequence[PackageGroupRow],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return ``(grouped_blood_parameters_list, raw_for_blood_report_raw_column)``."""
    dd_lookup = _build_digital_data_lookup(raw_customer)
    groups: list[GroupedBloodParameterGroup] = []

    for group in package_groups:
        tests: list[GroupedBloodParameterTest] = []
        for test in group.tests:
            entry: dict[str, Any] | None = None
            external_pid = test.external_parameter_id
            if external_pid is not None:
                entry = dd_lookup.get(str(external_pid))

            value = machine_value = unit = lower_range = higher_range = None
            provider_test_name: str | None = None
            if entry is not None:
                value = _parse_float(entry.get("value"))
                machine_value = _parse_float(entry.get("machine_value"))
                raw_unit = entry.get("unit")
                if isinstance(raw_unit, str) and raw_unit.strip():
                    unit = raw_unit.strip()
                elif isinstance(test.unit, str) and test.unit.strip():
                    unit = test.unit.strip()
                lower_range = _parse_float(entry.get("min_range"))
                higher_range = _parse_float(entry.get("max_range"))
                if entry.get("test_name") is not None:
                    provider_test_name = str(entry.get("test_name")).strip() or None
            elif isinstance(test.unit, str) and test.unit.strip():
                unit = test.unit.strip()

            if provider_test_name is None:
                provider_test_name = test.test_name or None

            tests.append(
                GroupedBloodParameterTest(
                    test_id=int(test.test_id),
                    parameter_type=_parameter_type_value(test.parameter_type),
                    test_name=test.test_name,
                    parameter_key=test.parameter_key,
                    unit=unit,
                    value=value,
                    machine_value=machine_value,
                    lower_range=lower_range,
                    higher_range=higher_range,
                    provider_test_name=provider_test_name,
                )
            )

        groups.append(
            GroupedBloodParameterGroup(
                group_name=group.group_name,
                test_count=len(tests),
                tests=tests,
            )
        )

    return [g.model_dump() for g in groups], raw_customer


def build_grouped_from_canonical(
    blob: dict[str, Any],
    *,
    package_groups: Sequence[PackageGroupRow],
) -> list[dict[str, Any]]:
    """Convert legacy canonical ``parameters`` map into package-shaped groups."""
    params = read_canonical_parameters(blob)
    groups: list[GroupedBloodParameterGroup] = []

    for group in package_groups:
        tests: list[GroupedBloodParameterTest] = []
        for test in group.tests:
            parameter_key = test.parameter_key
            entry = params.get(parameter_key) if parameter_key else None
            if not isinstance(entry, dict):
                tests.append(
                    GroupedBloodParameterTest(
                        test_id=int(test.test_id),
                        parameter_type=_parameter_type_value(test.parameter_type),
                        test_name=test.test_name,
                        parameter_key=parameter_key,
                        unit=test.unit if isinstance(test.unit, str) else None,
                        value=None,
                        machine_value=None,
                        lower_range=None,
                        higher_range=None,
                        provider_test_name=test.test_name or None,
                    )
                )
                continue

            unit = entry.get("unit")
            if not unit and isinstance(test.unit, str):
                unit = test.unit
            tests.append(
                GroupedBloodParameterTest(
                    test_id=int(test.test_id),
                    parameter_type=_parameter_type_value(test.parameter_type),
                    test_name=test.test_name,
                    parameter_key=parameter_key,
                    unit=unit,
                    value=_parse_float(entry.get("value")),
                    machine_value=_parse_float(entry.get("machine_value")),
                    lower_range=_parse_float(entry.get("lower_range")),
                    higher_range=_parse_float(entry.get("higher_range")),
                    provider_test_name=entry.get("provider_test_name") or test.test_name or None,
                )
            )

        groups.append(
            GroupedBloodParameterGroup(
                group_name=group.group_name,
                test_count=len(tests),
                tests=tests,
            )
        )

    return [g.model_dump() for g in groups]


def build_grouped_from_metsights_flat(
    blob: dict[str, Any],
    *,
    package_groups: Sequence[PackageGroupRow],
    user_gender: str | None = None,
) -> list[dict[str, Any]]:
    """Convert legacy Metsights flat dict into package-shaped groups."""
    groups: list[GroupedBloodParameterGroup] = []
    for group in package_groups:
        tests: list[GroupedBloodParameterTest] = []
        for test in group.tests:
            parameter_key = test.parameter_key
            raw_value: Any = blob.get(parameter_key) if parameter_key else None
            value = _parse_float(raw_value)

            unit_key = f"{parameter_key}_unit" if parameter_key else None
            raw_unit = blob.get(unit_key) if unit_key else None
            if isinstance(raw_unit, str) and raw_unit.strip():
                unit: str | None = raw_unit.strip()
            else:
                unit = test.unit.strip() if isinstance(test.unit, str) else None

            tests.append(
                GroupedBloodParameterTest(
                    test_id=int(test.test_id),
                    parameter_type=_parameter_type_value(test.parameter_type),
                    test_name=test.test_name,
                    parameter_key=parameter_key,
                    unit=unit,
                    value=value,
                    machine_value=None,
                    lower_range=None,
                    higher_range=None,
                    provider_test_name=test.test_name or None,
                )
            )

        groups.append(
            GroupedBloodParameterGroup(
                group_name=group.group_name,
                test_count=len(tests),
                tests=tests,
            )
        )
    return [g.model_dump() for g in groups]


def read_canonical_parameters(blob: Any) -> dict[str, dict[str, Any]]:
    """Extract ``parameter_key -> value dict`` from legacy canonical or Healthians storage."""
    if not isinstance(blob, dict):
        return {}

    if is_canonical_blood_parameters(blob):
        raw_params = blob.get("parameters")
        if not isinstance(raw_params, dict):
            return {}
        result: dict[str, dict[str, Any]] = {}
        for k, v in raw_params.items():
            if not isinstance(v, dict):
                continue
            # Accept both old and new key names in stored JSON.
            if "external_parameter_id" not in v and "healthians_parameter_id" in v:
                v = {**v, "external_parameter_id": v.get("healthians_parameter_id")}
            result[str(k)] = v
        return result

    if is_legacy_healthians_format(blob):
        return {}

    return {}


# Back-compat alias used by older imports.
normalize_from_healthians = build_grouped_from_healthians
