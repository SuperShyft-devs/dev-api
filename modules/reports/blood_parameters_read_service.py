"""Unified read adapters for blood parameter API responses."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from modules.diagnostics.service import DiagnosticsService
from modules.reports.blood_parameters_normalizer import read_canonical_parameters
from modules.reports.blood_parameters_questionnaire_reader import BloodParametersQuestionnaireReader
from modules.reports.blood_parameters_schemas import (
    is_canonical_blood_parameters,
    is_legacy_healthians_format,
)
from modules.reports.schemas import (
    BloodParameterGroupInReportResponse,
    BloodParameterTestInReportResponse,
)


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class BloodParametersReadService:
    """Build report groups and extract values from canonical provider or questionnaire data."""

    def __init__(
        self,
        *,
        diagnostics_service: DiagnosticsService,
        questionnaire_reader: BloodParametersQuestionnaireReader | None = None,
    ) -> None:
        self._diagnostics_service = diagnostics_service
        self._questionnaire_reader = questionnaire_reader or BloodParametersQuestionnaireReader()

    async def build_from_canonical_or_legacy_provider(
        self,
        db: AsyncSession,
        *,
        blood_parameters: Any,
        diagnostic_package_id: int,
        user_gender: str | None = None,
    ) -> list[BloodParameterGroupInReportResponse]:
        from modules.reports.blood_parameters_schemas import is_legacy_metsights_flat_format

        if is_legacy_metsights_flat_format(blood_parameters):
            return await self.build_from_metsights_flat(
                db=db,
                blood_parameters=blood_parameters,
                diagnostic_package_id=diagnostic_package_id,
                user_gender=user_gender,
            )
        if is_legacy_healthians_format(blood_parameters):
            return await self._build_from_legacy_healthians(
                db=db,
                blood_parameters=blood_parameters,
                diagnostic_package_id=diagnostic_package_id,
            )
        return await self._build_from_canonical_parameters(
            db=db,
            blood_parameters=blood_parameters,
            diagnostic_package_id=diagnostic_package_id,
        )

    async def build_from_metsights_flat(
        self,
        db: AsyncSession,
        *,
        blood_parameters: Any,
        diagnostic_package_id: int,
        user_gender: str | None,
    ) -> list[BloodParameterGroupInReportResponse]:
        raw: dict[str, Any] = blood_parameters if isinstance(blood_parameters, dict) else {}
        package_tests = await self._diagnostics_service.get_package_tests(
            db=db,
            package_id=diagnostic_package_id,
        )

        groups: list[BloodParameterGroupInReportResponse] = []
        for group in package_tests.groups:
            tests: list[BloodParameterTestInReportResponse] = []
            for test in group.tests:
                parameter_key = test.parameter_key
                raw_value: Any = raw.get(parameter_key) if parameter_key else None
                value = _parse_float(raw_value)

                unit_key = f"{parameter_key}_unit" if parameter_key else None
                raw_unit = raw.get(unit_key) if unit_key else None
                if isinstance(raw_unit, str) and raw_unit.strip():
                    unit: str | None = raw_unit.strip()
                else:
                    unit = test.unit.strip() if isinstance(test.unit, str) else None

                lower_range: float | None = None
                higher_range: float | None = None
                if user_gender == "male":
                    lower_range = (
                        float(test.low_risk_lower_range_male)
                        if test.low_risk_lower_range_male is not None
                        else None
                    )
                    higher_range = (
                        float(test.low_risk_higher_range_male)
                        if test.low_risk_higher_range_male is not None
                        else None
                    )
                elif user_gender == "female":
                    lower_range = (
                        float(test.low_risk_lower_range_female)
                        if test.low_risk_lower_range_female is not None
                        else None
                    )
                    higher_range = (
                        float(test.low_risk_higher_range_female)
                        if test.low_risk_higher_range_female is not None
                        else None
                    )

                tests.append(
                    BloodParameterTestInReportResponse(
                        test_id=test.test_id,
                        test_name=test.test_name,
                        parameter_key=parameter_key,
                        healthians_parameter_id=test.healthians_parameter_id,
                        unit=unit,
                        value=value,
                        machine_value=None,
                        lower_range=lower_range,
                        higher_range=higher_range,
                    )
                )

            groups.append(
                BloodParameterGroupInReportResponse(
                    group_name=group.group_name,
                    test_count=len(tests),
                    tests=tests,
                )
            )
        return groups

    async def build_from_questionnaire_responses(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        diagnostic_package_id: int,
        user_gender: str | None,
    ) -> list[BloodParameterGroupInReportResponse]:
        flat = await self._questionnaire_reader.build_flat_from_questionnaire_responses(
            db,
            assessment_instance_id=assessment_instance_id,
        )
        return await self.build_from_metsights_flat(
            db=db,
            blood_parameters=flat,
            diagnostic_package_id=diagnostic_package_id,
            user_gender=user_gender,
        )

    async def extract_provider_parameter(
        self,
        blood_parameters: Any,
        *,
        parameter_key: str,
        healthians_parameter_id: int | None,
    ) -> tuple[float | None, str | None]:
        if not isinstance(blood_parameters, dict):
            return None, None

        if is_canonical_blood_parameters(blood_parameters):
            params = read_canonical_parameters(blood_parameters)
            entry = params.get(parameter_key)
            if isinstance(entry, dict):
                return _parse_float(entry.get("value")), (
                    str(entry.get("unit")).strip() if entry.get("unit") else None
                )
            return None, None

        if is_legacy_healthians_format(blood_parameters) and healthians_parameter_id is not None:
            pid_str = str(healthians_parameter_id)
            digital_data = blood_parameters.get("digital_data")
            if isinstance(digital_data, list):
                for entry in digital_data:
                    if not isinstance(entry, dict):
                        continue
                    if str(entry.get("parameter_id") or "").strip() == pid_str:
                        raw_unit = entry.get("unit")
                        unit_val = (
                            raw_unit.strip() if isinstance(raw_unit, str) and raw_unit.strip() else None
                        )
                        return _parse_float(entry.get("value")), unit_val
        return None, None

    @staticmethod
    def extract_canonical_value_and_range(
        blood_parameters: Any,
        *,
        parameter_key: str | None,
        gender: str | None,
        catalog_lower_male: Any = None,
        catalog_higher_male: Any = None,
        catalog_lower_female: Any = None,
        catalog_higher_female: Any = None,
    ) -> tuple[float | None, float | None, float | None]:
        """Extract value and range for camp reports from canonical provider storage."""
        if not parameter_key or not is_canonical_blood_parameters(blood_parameters):
            return None, None, None

        params = read_canonical_parameters(blood_parameters)
        entry = params.get(parameter_key)
        if not isinstance(entry, dict):
            return None, None, None

        value = _parse_float(entry.get("value"))
        lower_range = _parse_float(entry.get("lower_range"))
        higher_range = _parse_float(entry.get("higher_range"))

        if lower_range is None or higher_range is None:
            normalized_gender = (gender or "").strip().lower()
            if normalized_gender in ("male", "m", "1"):
                lower_range = lower_range if lower_range is not None else _parse_float(catalog_lower_male)
                higher_range = higher_range if higher_range is not None else _parse_float(catalog_higher_male)
            elif normalized_gender in ("female", "f", "2"):
                lower_range = lower_range if lower_range is not None else _parse_float(catalog_lower_female)
                higher_range = higher_range if higher_range is not None else _parse_float(catalog_higher_female)

        return value, lower_range, higher_range

    async def _build_from_canonical_parameters(
        self,
        db: AsyncSession,
        *,
        blood_parameters: Any,
        diagnostic_package_id: int,
    ) -> list[BloodParameterGroupInReportResponse]:
        params = read_canonical_parameters(blood_parameters)
        package_tests = await self._diagnostics_service.get_package_tests(
            db=db,
            package_id=diagnostic_package_id,
        )

        groups: list[BloodParameterGroupInReportResponse] = []
        for group in package_tests.groups:
            tests: list[BloodParameterTestInReportResponse] = []
            for test in group.tests:
                parameter_key = test.parameter_key
                entry = params.get(parameter_key) if parameter_key else None
                if not isinstance(entry, dict):
                    tests.append(
                        BloodParameterTestInReportResponse(
                            test_id=test.test_id,
                            test_name=test.test_name,
                            parameter_key=parameter_key,
                            healthians_parameter_id=test.healthians_parameter_id,
                            unit=None,
                            value=None,
                            machine_value=None,
                            lower_range=None,
                            higher_range=None,
                        )
                    )
                    continue

                tests.append(
                    BloodParameterTestInReportResponse(
                        test_id=test.test_id,
                        test_name=test.test_name,
                        parameter_key=parameter_key,
                        healthians_parameter_id=test.healthians_parameter_id,
                        unit=entry.get("unit"),
                        value=_parse_float(entry.get("value")),
                        machine_value=_parse_float(entry.get("machine_value")),
                        lower_range=_parse_float(entry.get("lower_range")),
                        higher_range=_parse_float(entry.get("higher_range")),
                    )
                )

            groups.append(
                BloodParameterGroupInReportResponse(
                    group_name=group.group_name,
                    test_count=len(tests),
                    tests=tests,
                )
            )
        return groups

    async def _build_from_legacy_healthians(
        self,
        db: AsyncSession,
        *,
        blood_parameters: dict[str, Any],
        diagnostic_package_id: int,
    ) -> list[BloodParameterGroupInReportResponse]:
        dd_lookup: dict[str, dict[str, Any]] = {}
        digital_data = blood_parameters.get("digital_data")
        if isinstance(digital_data, list):
            for entry in digital_data:
                if not isinstance(entry, dict):
                    continue
                pid = str(entry.get("parameter_id") or "").strip()
                if pid:
                    dd_lookup[pid] = entry

        package_tests = await self._diagnostics_service.get_package_tests(
            db=db,
            package_id=diagnostic_package_id,
        )

        groups: list[BloodParameterGroupInReportResponse] = []
        for group in package_tests.groups:
            tests: list[BloodParameterTestInReportResponse] = []
            for test in group.tests:
                if test.healthians_parameter_id is None:
                    continue
                entry = dd_lookup.get(str(test.healthians_parameter_id))
                value = machine_value = unit = lower_range = higher_range = None
                if entry is not None:
                    value = _parse_float(entry.get("value"))
                    machine_value = _parse_float(entry.get("machine_value"))
                    raw_unit = entry.get("unit")
                    if isinstance(raw_unit, str) and raw_unit.strip():
                        unit = raw_unit.strip()
                    lower_range = _parse_float(entry.get("min_range"))
                    higher_range = _parse_float(entry.get("max_range"))

                tests.append(
                    BloodParameterTestInReportResponse(
                        test_id=test.test_id,
                        test_name=test.test_name,
                        parameter_key=test.parameter_key,
                        healthians_parameter_id=test.healthians_parameter_id,
                        unit=unit,
                        value=value,
                        machine_value=machine_value,
                        lower_range=lower_range,
                        higher_range=higher_range,
                    )
                )

            groups.append(
                BloodParameterGroupInReportResponse(
                    group_name=group.group_name,
                    test_count=len(tests),
                    tests=tests,
                )
            )
        return groups
