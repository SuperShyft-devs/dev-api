"""Diagnostics service.

Business rules:
- Routers must stay thin and delegate business logic here.
- All mutations must be audit logged.
- Parent-child integrity checks are enforced before child mutations.
"""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.diagnostics.models import (
    DiagnosticPackage,
    DiagnosticPackageFilterChip,
    DiagnosticPackagePreparation,
    DiagnosticPackageReason,
    DiagnosticPackageSample,
    DiagnosticPackageTag,
    DiagnosticTestGroup,
    HealthParameter,
    ParameterType as ORMParameterType,
)
from modules.diagnostics.repository import DiagnosticsRepository
from modules.diagnostics.schemas import (
    AssignGroupsToPackageRequest,
    AssignGroupsToPackageResponse,
    AssignTestsToGroupRequest,
    AssignTestsToGroupResponse,
    DiagnosticPackageCreate,
    DiagnosticPackageDetailResponse,
    DiagnosticPackageListItem,
    DiagnosticPackageResponse,
    DiagnosticPackageStatusUpdate,
    DiagnosticPackageUpdate,
    FilterChipCreate,
    FilterChipForSchema,
    FilterChipResponse,
    FilterChipUpdate,
    HealthParameterCreate,
    HealthParameterResponse,
    HealthParameterUpdate,
    PackageForType,
    PackageTestsResponse,
    ParameterType,
    PreparationCreate,
    PreparationResponse,
    PreparationUpdate,
    ReasonCreate,
    ReasonResponse,
    ReasonUpdate,
    SampleCreate,
    SampleResponse,
    SampleUpdate,
    TagCreate,
    TagResponse,
    TestGroupCreate,
    TestGroupResponse,
    TestGroupUpdate,
    PackageFilterChipAssign,
    PackageFilterChipResponse,
    ReorderGroupTestsRequest,
    ReorderPackageGroupsRequest,
)
from modules.employee.service import EmployeeContext


_ALLOWED_COLLECTION_TYPES = {"home_collection", "centre_visit"}
_ALLOWED_GENDER_VALUES = {"male", "female", "both"}
_ALLOWED_STATUS_VALUES = {"active", "inactive"}
_ALLOWED_CHIP_FOR = {e.value for e in FilterChipForSchema}
_ALLOWED_PACKAGE_FOR = {e.value for e in PackageForType}


def _discount_percent(price: float | None, original_price: float | None) -> int | None:
    if original_price is None or original_price == 0 or price is None:
        return None
    return round(((original_price - price) / original_price) * 100)


def _discount_percent_label(price: float | None, original_price: float | None) -> str | None:
    pct = _discount_percent(price, original_price)
    if pct is None:
        return None
    return f"{pct}%"


class DiagnosticsService:
    def __init__(self, repository: DiagnosticsRepository, audit_service: AuditService | None = None):
        self._repository = repository
        self._audit_service = audit_service

    def _ensure_employee_access(self, employee: EmployeeContext | None) -> None:
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    def _normalize(self, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized if normalized else None

    def _normalize_lower(self, value: str | None) -> str | None:
        normalized = self._normalize(value)
        return normalized.lower() if normalized else None

    def _orm_parameter_type(self, value: ParameterType | ORMParameterType | str | None) -> ORMParameterType:
        if value is None:
            return ORMParameterType.TEST
        if isinstance(value, ORMParameterType):
            return value
        if isinstance(value, ParameterType):
            return ORMParameterType(value.value)
        return ORMParameterType(value)

    def _validate_exact_assignment_ids(self, *, requested_ids: list[int], assigned_ids: list[int], field_name: str) -> list[int]:
        requested_unique = list(dict.fromkeys(requested_ids))
        if len(requested_unique) != len(requested_ids):
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"{field_name} contains duplicate ids",
            )
        if set(requested_unique) != set(assigned_ids):
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"{field_name} must contain exactly the currently assigned ids",
            )
        return requested_unique

    def _validate_package_common_fields(self, payload: dict) -> None:
        collection_type = self._normalize_lower(payload.get("collection_type"))
        if collection_type is not None and collection_type not in _ALLOWED_COLLECTION_TYPES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if collection_type is not None:
            payload["collection_type"] = collection_type

        gender = self._normalize_lower(payload.get("gender_suitability"))
        if gender is not None and gender not in _ALLOWED_GENDER_VALUES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if gender is not None:
            payload["gender_suitability"] = gender

    def _validate_package_for_field(self, payload: dict) -> None:
        pf = self._normalize_lower(payload.get("package_for"))
        if pf is not None and pf not in _ALLOWED_PACKAGE_FOR:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if pf is not None:
            payload["package_for"] = pf

    def _validate_optional_gender_field(self, payload: dict) -> None:
        gender = self._normalize_lower(payload.get("gender_suitability"))
        if gender is not None and gender not in _ALLOWED_GENDER_VALUES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if gender is not None:
            payload["gender_suitability"] = gender

    def _to_tag_response(self, row: DiagnosticPackageTag) -> TagResponse:
        return TagResponse(
            tag_id=row.tag_id,
            diagnostic_package_id=row.diagnostic_package_id,
            tag_name=row.tag_name,
            display_order=row.display_order,
        )

    def _package_filter_chip_responses(self, package: DiagnosticPackage) -> list[PackageFilterChipResponse]:
        links = sorted(
            [ln for ln in package.filter_chip_links if ln.diagnostic_package_id is not None],
            key=lambda ln: (ln.display_order is None, ln.display_order or 0, ln.link_id),
        )
        out: list[PackageFilterChipResponse] = []
        for link in links:
            chip = link.filter_chip
            if chip is None:
                continue
            out.append(
                PackageFilterChipResponse(
                    filter_chip_id=chip.filter_chip_id,
                    chip_key=chip.chip_key,
                    display_name=chip.display_name,
                    display_order=link.display_order,
                )
            )
        return out

    def _to_reason_response(self, row: DiagnosticPackageReason) -> ReasonResponse:
        return ReasonResponse(
            reason_id=row.reason_id,
            diagnostic_package_id=row.diagnostic_package_id,
            reason_text=row.reason_text,
            display_order=row.display_order,
        )

    def _to_sample_response(self, row: DiagnosticPackageSample) -> SampleResponse:
        return SampleResponse(
            sample_id=row.sample_id,
            diagnostic_package_id=row.diagnostic_package_id,
            sample_type=row.sample_type,
            description=row.description,
            display_order=row.display_order,
        )

    def _to_preparation_response(self, row: DiagnosticPackagePreparation) -> PreparationResponse:
        steps = row.steps if isinstance(row.steps, list) else None
        return PreparationResponse(
            preparation_id=row.preparation_id,
            diagnostic_package_id=row.diagnostic_package_id,
            preparation_title=row.preparation_title,
            steps=steps,
            display_order=row.display_order,
        )

    def _to_health_parameter_response(self, row: HealthParameter) -> HealthParameterResponse:
        lower_range_male = float(row.lower_range_male) if row.lower_range_male is not None else None
        higher_range_male = float(row.higher_range_male) if row.higher_range_male is not None else None
        lower_range_female = float(row.lower_range_female) if row.lower_range_female is not None else None
        higher_range_female = float(row.higher_range_female) if row.higher_range_female is not None else None
        pt = row.parameter_type
        parameter_type = ParameterType(pt.value) if isinstance(pt, ORMParameterType) else ParameterType(pt)
        price = float(row.price) if row.price is not None else None
        original_price = float(row.original_price) if row.original_price is not None else None
        return HealthParameterResponse(
            test_id=row.test_id,
            parameter_type=parameter_type,
            test_name=row.test_name,
            parameter_key=row.parameter_key,
            unit=row.unit,
            meaning=row.meaning,
            lower_range_male=lower_range_male,
            higher_range_male=higher_range_male,
            lower_range_female=lower_range_female,
            higher_range_female=higher_range_female,
            causes_when_high=row.causes_when_high,
            causes_when_low=row.causes_when_low,
            effects_when_high=row.effects_when_high,
            effects_when_low=row.effects_when_low,
            what_to_do_when_low=row.what_to_do_when_low,
            what_to_do_when_high=row.what_to_do_when_high,
            is_available=bool(row.is_available),
            display_order=row.display_order,
            price=price,
            original_price=original_price,
            is_most_popular=bool(row.is_most_popular),
            gender_suitability=row.gender_suitability,
        )

    def _group_filter_chip_link_responses(self, links: list) -> list[PackageFilterChipResponse]:
        out: list[PackageFilterChipResponse] = []
        for link in links:
            chip = getattr(link, "filter_chip", None)
            if chip is None:
                continue
            out.append(
                PackageFilterChipResponse(
                    filter_chip_id=chip.filter_chip_id,
                    chip_key=chip.chip_key,
                    display_name=chip.display_name,
                    display_order=link.display_order,
                )
            )
        return out

    def _to_group_response(
        self,
        row: DiagnosticTestGroup,
        *,
        tests: list[HealthParameter] | None = None,
        test_count: int | None = None,
        filter_chip_links: list | None = None,
    ) -> TestGroupResponse:
        tests_value = tests or []
        resolved_count = test_count if test_count is not None else len(tests_value)
        price = float(row.price) if row.price is not None else None
        original_price = float(row.original_price) if row.original_price is not None else None
        chip_links = filter_chip_links if filter_chip_links is not None else []
        return TestGroupResponse(
            group_id=row.group_id,
            group_name=row.group_name,
            display_order=row.display_order,
            test_count=resolved_count,
            price=price,
            discount=_discount_percent_label(price, original_price),
            original_price=original_price,
            is_most_popular=bool(row.is_most_popular),
            gender_suitability=row.gender_suitability,
            package_for=row.package_for,
            tests=[self._to_health_parameter_response(test_row) for test_row in tests_value],
            filter_chips=self._group_filter_chip_link_responses(chip_links),
        )

    def _to_package_response(
        self,
        row: DiagnosticPackage,
        *,
        no_of_tests: int | None = None,
    ) -> DiagnosticPackageResponse:
        price = float(row.price) if row.price is not None else None
        original_price = float(row.original_price) if row.original_price is not None else None
        return DiagnosticPackageResponse(
            diagnostic_package_id=row.diagnostic_package_id,
            reference_id=row.reference_id,
            package_name=row.package_name,
            diagnostic_provider=row.diagnostic_provider,
            created_by_user_id=row.created_by_user_id,
            no_of_tests=no_of_tests,
            report_duration_hours=row.report_duration_hours,
            collection_type=row.collection_type,
            about_text=row.about_text,
            bookings_count=row.bookings_count,
            price=price,
            original_price=original_price,
            is_most_popular=row.is_most_popular,
            gender_suitability=row.gender_suitability,
            package_for=row.package_for,
            status=row.status,
            created_at=row.created_at,
            discount_percent=_discount_percent(price, original_price),
        )

    async def _package_response_with_test_count(self, db, row: DiagnosticPackage) -> DiagnosticPackageResponse:
        counts = await self._repository.count_distinct_tests_for_packages(
            db, package_ids=[row.diagnostic_package_id]
        )
        return self._to_package_response(row, no_of_tests=counts.get(row.diagnostic_package_id, 0))

    async def get_packages(
        self,
        db,
        *,
        gender: str | None,
        tag: str | None,
        filter_chip: str | None = None,
        active_only: bool = True,
        list_type: str = "public_package",
        requesting_user_id: int | None = None,
        package_for: str | None = None,
    ) -> list[DiagnosticPackageListItem]:
        gender_value = self._normalize_lower(gender)
        tag_value = self._normalize(tag)
        chip_key_value = self._normalize(filter_chip)

        if gender_value is not None and gender_value not in _ALLOWED_GENDER_VALUES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        if list_type not in ("public_package", "custom_package"):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        public_only = list_type == "public_package"
        created_by_user_id = None if public_only else requesting_user_id

        resolved_package_for = package_for
        if resolved_package_for is None and public_only and active_only:
            resolved_package_for = "public"

        rows = await self._repository.get_all_packages(
            db,
            gender=gender_value,
            tag=tag_value,
            filter_chip=chip_key_value,
            active_only=active_only,
            public_only=public_only,
            created_by_user_id=created_by_user_id,
            package_for=resolved_package_for,
        )
        pkg_ids = [r.diagnostic_package_id for r in rows]
        counts = await self._repository.count_distinct_tests_for_packages(db, package_ids=pkg_ids)
        items: list[DiagnosticPackageListItem] = []
        for row in rows:
            price = float(row.price) if row.price is not None else None
            original_price = float(row.original_price) if row.original_price is not None else None
            tags = sorted(
                list(row.tags),
                key=lambda t: (t.display_order is None, t.display_order or 0, t.tag_id),
            )
            n_tests = counts.get(row.diagnostic_package_id, 0)
            items.append(
                DiagnosticPackageListItem(
                    diagnostic_package_id=row.diagnostic_package_id,
                    package_name=row.package_name,
                    no_of_tests=n_tests,
                    report_duration_hours=row.report_duration_hours,
                    collection_type=row.collection_type,
                    price=price,
                    original_price=original_price,
                    discount_percent=_discount_percent(price, original_price),
                    is_most_popular=row.is_most_popular,
                    gender_suitability=row.gender_suitability,
                    package_for=row.package_for,
                    status=row.status,
                    tags=[self._to_tag_response(tag_row) for tag_row in tags],
                    filter_chips=self._package_filter_chip_responses(row),
                )
            )
        return items

    async def get_package_detail(self, db, *, package_id: int) -> DiagnosticPackageDetailResponse:
        row = await self._repository.get_package_by_id(db, package_id=package_id)
        if row is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        counts = await self._repository.count_distinct_tests_for_packages(
            db, package_ids=[row.diagnostic_package_id]
        )
        n_tests = counts.get(row.diagnostic_package_id, 0)
        package = self._to_package_response(row, no_of_tests=n_tests)
        reasons = sorted(list(row.reasons), key=lambda r: (r.display_order is None, r.display_order or 0, r.reason_id))
        tags = sorted(list(row.tags), key=lambda t: (t.display_order is None, t.display_order or 0, t.tag_id))
        samples = sorted(list(row.samples), key=lambda s: (s.display_order is None, s.display_order or 0, s.sample_id))
        preparations = sorted(
            list(row.preparations),
            key=lambda p: (p.display_order is None, p.display_order or 0, p.preparation_id),
        )
        return DiagnosticPackageDetailResponse(
            **package.model_dump(),
            reasons=[self._to_reason_response(reason_row) for reason_row in reasons],
            tags=[self._to_tag_response(tag_row) for tag_row in tags],
            samples=[self._to_sample_response(sample_row) for sample_row in samples],
            preparations=[self._to_preparation_response(preparation_row) for preparation_row in preparations],
            filter_chips=self._package_filter_chip_responses(row),
        )

    async def get_package_tests(self, db, *, package_id: int) -> PackageTestsResponse:
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        group_rows = await self._repository.get_package_test_groups(db, package_id=package_id)
        groups = [self._to_group_response(group_row, tests=tests) for group_row, tests in group_rows]
        return PackageTestsResponse(diagnostic_package_id=package_id, groups=groups)

    async def create_package(
        self,
        db,
        *,
        employee: EmployeeContext | None,
        current_user_id: int,
        data: DiagnosticPackageCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> DiagnosticPackageResponse:
        is_custom = data.custom
        payload = data.model_dump(exclude_none=True)
        payload.pop("no_of_tests", None)
        payload.pop("custom", None)

        if employee is not None:
            if is_custom:
                payload["created_by_user_id"] = current_user_id
            else:
                payload["created_by_user_id"] = None
        else:
            if not is_custom:
                raise AppError(
                    status_code=403,
                    error_code="FORBIDDEN",
                    message="Only staff can create public packages; set custom to true to create your package",
                )
            payload["created_by_user_id"] = current_user_id

        self._validate_package_common_fields(payload)
        self._validate_package_for_field(payload)
        payload.setdefault("status", "active")
        payload.setdefault("package_for", "public")

        package = DiagnosticPackage(**payload)
        package = await self._repository.create_package(db, package)

        action = "EMPLOYEE_CREATE_DIAGNOSTIC_PACKAGE" if employee is not None else "USER_CREATE_DIAGNOSTIC_PACKAGE"
        await self._require_audit_service().log_event(
            db,
            action=action,
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_user_id,
            session_id=None,
        )
        return await self._package_response_with_test_count(db, package)

    async def update_package(
        self,
        db,
        *,
        employee: EmployeeContext | None,
        current_user_id: int,
        package_id: int,
        data: DiagnosticPackageUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> DiagnosticPackageResponse:
        existing = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if existing is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        if employee is None:
            if existing.created_by_user_id is None or existing.created_by_user_id != current_user_id:
                raise AppError(
                    status_code=403,
                    error_code="FORBIDDEN",
                    message="You do not have permission to perform this action",
                )

        payload = data.model_dump(exclude_none=True)
        payload.pop("no_of_tests", None)
        if "package_name" in payload:
            name = self._normalize(payload["package_name"])
            if name is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["package_name"] = name
        self._validate_package_common_fields(payload)
        self._validate_package_for_field(payload)

        updated = await self._repository.update_package(db, package_id=package_id, data=payload)
        if updated is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        action = "EMPLOYEE_UPDATE_DIAGNOSTIC_PACKAGE" if employee is not None else "USER_UPDATE_DIAGNOSTIC_PACKAGE"
        await self._require_audit_service().log_event(
            db,
            action=action,
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_user_id,
            session_id=None,
        )
        return await self._package_response_with_test_count(db, updated)

    async def list_parameters(
        self,
        db,
        *,
        parameter_type: ParameterType | None = None,
    ) -> list[HealthParameterResponse]:
        orm_type = self._orm_parameter_type(parameter_type) if parameter_type is not None else None
        rows = await self._repository.get_all_parameters(db, parameter_type=orm_type)
        return [self._to_health_parameter_response(row) for row in rows]

    async def get_parameter(self, db, *, test_id: int) -> HealthParameterResponse:
        row = await self._repository.get_parameter_by_id(db, test_id=test_id)
        if row is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_NOT_FOUND", message="Test does not exist")
        return self._to_health_parameter_response(row)

    async def get_health_parameter_by_parameter_key(
        self,
        db,
        *,
        parameter_key: str,
    ) -> HealthParameterResponse | None:
        key = self._normalize_lower(parameter_key)
        if key is None:
            return None
        row = await self._repository.get_parameter_by_parameter_key(db, parameter_key=key)
        if row is None:
            return None
        return self._to_health_parameter_response(row)

    async def create_parameter(
        self,
        db,
        *,
        employee: EmployeeContext,
        data: HealthParameterCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> HealthParameterResponse:
        self._ensure_employee_access(employee)
        payload = data.model_dump(exclude_none=True)
        payload["parameter_type"] = self._orm_parameter_type(payload.get("parameter_type"))
        test_name = self._normalize(payload.get("test_name"))
        if test_name is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        payload["test_name"] = test_name

        if "parameter_key" in payload:
            parameter_key = self._normalize_lower(payload.get("parameter_key"))
            if parameter_key is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["parameter_key"] = parameter_key
        if "unit" in payload:
            payload["unit"] = self._normalize(payload.get("unit"))
        if "meaning" in payload:
            payload["meaning"] = self._normalize(payload.get("meaning"))
        if "causes_when_high" in payload:
            payload["causes_when_high"] = self._normalize(payload.get("causes_when_high"))
        if "causes_when_low" in payload:
            payload["causes_when_low"] = self._normalize(payload.get("causes_when_low"))
        if "effects_when_high" in payload:
            payload["effects_when_high"] = self._normalize(payload.get("effects_when_high"))
        if "effects_when_low" in payload:
            payload["effects_when_low"] = self._normalize(payload.get("effects_when_low"))
        if "what_to_do_when_low" in payload:
            payload["what_to_do_when_low"] = self._normalize(payload.get("what_to_do_when_low"))
        if "what_to_do_when_high" in payload:
            payload["what_to_do_when_high"] = self._normalize(payload.get("what_to_do_when_high"))
        self._validate_optional_gender_field(payload)

        created = await self._repository.create_parameter(db, HealthParameter(**payload))
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_DIAGNOSTIC_TEST",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_health_parameter_response(created)

    async def update_parameter(
        self,
        db,
        *,
        employee: EmployeeContext,
        test_id: int,
        data: HealthParameterUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> HealthParameterResponse:
        self._ensure_employee_access(employee)
        existing = await self._repository.get_parameter_by_id(db, test_id=test_id)
        if existing is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_NOT_FOUND", message="Test does not exist")

        payload = data.model_dump(exclude_none=True)
        if "parameter_type" in payload:
            payload["parameter_type"] = self._orm_parameter_type(payload["parameter_type"])
        if "test_name" in payload:
            test_name = self._normalize(payload.get("test_name"))
            if test_name is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["test_name"] = test_name

        if "parameter_key" in payload:
            parameter_key = self._normalize_lower(payload.get("parameter_key"))
            if parameter_key is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["parameter_key"] = parameter_key
        if "unit" in payload:
            payload["unit"] = self._normalize(payload.get("unit"))
        if "meaning" in payload:
            payload["meaning"] = self._normalize(payload.get("meaning"))
        if "causes_when_high" in payload:
            payload["causes_when_high"] = self._normalize(payload.get("causes_when_high"))
        if "causes_when_low" in payload:
            payload["causes_when_low"] = self._normalize(payload.get("causes_when_low"))
        if "effects_when_high" in payload:
            payload["effects_when_high"] = self._normalize(payload.get("effects_when_high"))
        if "effects_when_low" in payload:
            payload["effects_when_low"] = self._normalize(payload.get("effects_when_low"))
        if "what_to_do_when_low" in payload:
            payload["what_to_do_when_low"] = self._normalize(payload.get("what_to_do_when_low"))
        if "what_to_do_when_high" in payload:
            payload["what_to_do_when_high"] = self._normalize(payload.get("what_to_do_when_high"))
        self._validate_optional_gender_field(payload)

        updated = await self._repository.update_parameter(db, test_id=test_id, data=payload)
        if updated is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_NOT_FOUND", message="Test does not exist")
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_DIAGNOSTIC_TEST",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_health_parameter_response(updated)

    async def delete_parameter(
        self,
        db,
        *,
        employee: EmployeeContext,
        test_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)
        existing = await self._repository.get_parameter_by_id(db, test_id=test_id)
        if existing is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_NOT_FOUND", message="Test does not exist")
        await self._repository.delete_parameter(db, test_id=test_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_TEST",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return {"deleted": True}

    async def get_all_groups(self, db, *, filter_chip: str | None = None, package_for: str | None = None) -> list[TestGroupResponse]:
        chip_key_value = self._normalize(filter_chip)
        rows = await self._repository.get_all_groups(db, filter_chip_key=chip_key_value, package_for=package_for)
        return [self._to_group_response(group, tests=[], test_count=test_count) for group, test_count in rows]

    async def get_group_detail(self, db, *, group_id: int) -> TestGroupResponse:
        group = await self._repository.get_group_by_id(db, group_id=group_id)
        if group is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")
        tests = await self._repository.get_parameters_for_group(db, group_id=group_id)
        chip_links = await self._repository.get_group_filter_chip_links(db, group_id=group_id)
        return self._to_group_response(group, tests=tests, filter_chip_links=chip_links)

    async def create_group(
        self,
        db,
        *,
        employee: EmployeeContext,
        data: TestGroupCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> TestGroupResponse:
        self._ensure_employee_access(employee)
        payload = data.model_dump(exclude_none=True)
        group_name = self._normalize(payload.get("group_name"))
        if group_name is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        payload["group_name"] = group_name
        self._validate_optional_gender_field(payload)
        self._validate_package_for_field(payload)
        payload.setdefault("package_for", "public")

        created = await self._repository.create_group(db, DiagnosticTestGroup(**payload))
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_DIAGNOSTIC_TEST_GROUP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_group_response(created, tests=[], test_count=0)

    async def update_group(
        self,
        db,
        *,
        employee: EmployeeContext,
        group_id: int,
        data: TestGroupUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> TestGroupResponse:
        self._ensure_employee_access(employee)
        existing = await self._repository.get_group_by_id(db, group_id=group_id)
        if existing is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        payload = data.model_dump(exclude_none=True)
        if "group_name" in payload:
            group_name = self._normalize(payload.get("group_name"))
            if group_name is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["group_name"] = group_name
        self._validate_optional_gender_field(payload)
        self._validate_package_for_field(payload)

        updated = await self._repository.update_group(db, group_id=group_id, data=payload)
        if updated is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")
        tests = await self._repository.get_parameters_for_group(db, group_id=group_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_DIAGNOSTIC_TEST_GROUP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_group_response(updated, tests=tests)

    async def delete_group(
        self,
        db,
        *,
        employee: EmployeeContext,
        group_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        existing = await self._repository.get_group_by_id(db, group_id=group_id)
        if existing is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")
        await self._repository.delete_group(db, group_id=group_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_TEST_GROUP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def get_group_tests(self, db, *, group_id: int) -> list[HealthParameterResponse]:
        group = await self._repository.get_group_by_id(db, group_id=group_id)
        if group is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")
        tests = await self._repository.get_parameters_for_group(db, group_id=group_id)
        return [self._to_health_parameter_response(row) for row in tests]

    async def assign_tests_to_group(
        self,
        db,
        *,
        employee: EmployeeContext,
        group_id: int,
        data: AssignTestsToGroupRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> AssignTestsToGroupResponse:
        self._ensure_employee_access(employee)
        group = await self._repository.get_group_by_id(db, group_id=group_id)
        if group is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        invalid_ids: list[int] = []
        for test_id in data.test_ids:
            row = await self._repository.get_parameter_by_id(db, test_id=test_id)
            if row is None:
                invalid_ids.append(test_id)
        if invalid_ids:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"The following test_ids do not exist: {invalid_ids}",
            )

        added_ids, skipped_ids = await self._repository.assign_tests_to_group(
            db,
            group_id=group_id,
            test_ids=data.test_ids,
        )
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_ASSIGN_DIAGNOSTIC_TESTS_TO_GROUP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return AssignTestsToGroupResponse(
            group_id=group_id,
            added_test_ids=added_ids,
            skipped_test_ids=skipped_ids,
        )

    async def remove_test_from_group(
        self,
        db,
        *,
        employee: EmployeeContext,
        group_id: int,
        test_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        group = await self._repository.get_group_by_id(db, group_id=group_id)
        if group is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")
        test = await self._repository.get_parameter_by_id(db, test_id=test_id)
        if test is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_NOT_FOUND", message="Test does not exist")
        deleted = await self._repository.remove_test_from_group(db, group_id=group_id, test_id=test_id)
        if not deleted:
            raise AppError(
                status_code=404,
                error_code="DIAGNOSTIC_TEST_NOT_ASSIGNED_TO_GROUP",
                message="This test is not assigned to this group",
            )
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_REMOVE_DIAGNOSTIC_TEST_FROM_GROUP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def reorder_group_tests(
        self,
        db,
        *,
        employee: EmployeeContext,
        group_id: int,
        data: ReorderGroupTestsRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)
        group = await self._repository.get_group_by_id(db, group_id=group_id)
        if group is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")
        assigned_ids = await self._repository.get_assigned_test_ids_for_group_ordered(db, group_id=group_id)
        ordered_ids = self._validate_exact_assignment_ids(
            requested_ids=data.test_ids,
            assigned_ids=assigned_ids,
            field_name="test_ids",
        )
        await self._repository.reorder_group_tests(db, group_id=group_id, test_ids=ordered_ids)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_REORDER_DIAGNOSTIC_TESTS_IN_GROUP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return {"group_id": group_id, "test_ids": ordered_ids}

    async def assign_groups_to_package(
        self,
        db,
        *,
        employee: EmployeeContext | None,
        current_user_id: int,
        package_id: int,
        data: AssignGroupsToPackageRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> AssignGroupsToPackageResponse:
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        if employee is None:
            if package.created_by_user_id is None or package.created_by_user_id != current_user_id:
                raise AppError(
                    status_code=403,
                    error_code="FORBIDDEN",
                    message="You do not have permission to perform this action",
                )

        invalid_ids: list[int] = []
        for group_id in data.group_ids:
            row = await self._repository.get_group_by_id(db, group_id=group_id)
            if row is None:
                invalid_ids.append(group_id)
        if invalid_ids:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"The following group_ids do not exist: {invalid_ids}",
            )

        added_ids, skipped_ids = await self._repository.assign_groups_to_package(
            db,
            package_id=package_id,
            group_ids=data.group_ids,
        )
        action = (
            "EMPLOYEE_ASSIGN_DIAGNOSTIC_TEST_GROUPS_TO_PACKAGE"
            if employee is not None
            else "USER_ASSIGN_DIAGNOSTIC_TEST_GROUPS_TO_PACKAGE"
        )
        await self._require_audit_service().log_event(
            db,
            action=action,
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_user_id,
            session_id=None,
        )
        return AssignGroupsToPackageResponse(
            diagnostic_package_id=package_id,
            added_group_ids=added_ids,
            skipped_group_ids=skipped_ids,
        )

    async def remove_group_from_package(
        self,
        db,
        *,
        employee: EmployeeContext | None,
        current_user_id: int,
        package_id: int,
        group_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        if employee is not None:
            self._ensure_employee_access(employee)
            audit_user_id = employee.user_id
        else:
            audit_user_id = current_user_id

        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        if employee is None:
            if package.created_by_user_id is None or package.created_by_user_id != current_user_id:
                raise AppError(
                    status_code=403,
                    error_code="FORBIDDEN",
                    message="You do not have permission to perform this action",
                )

        group = await self._repository.get_group_by_id(db, group_id=group_id)
        if group is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")
        deleted = await self._repository.remove_group_from_package(
            db,
            package_id=package_id,
            group_id=group_id,
        )
        if not deleted:
            raise AppError(
                status_code=404,
                error_code="DIAGNOSTIC_GROUP_NOT_ASSIGNED_TO_PACKAGE",
                message="This group is not assigned to this package",
            )
        action = (
            "EMPLOYEE_REMOVE_DIAGNOSTIC_TEST_GROUP_FROM_PACKAGE"
            if employee is not None
            else "USER_REMOVE_DIAGNOSTIC_TEST_GROUP_FROM_PACKAGE"
        )
        await self._require_audit_service().log_event(
            db,
            action=action,
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=audit_user_id,
            session_id=None,
        )

    async def reorder_package_groups(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        data: ReorderPackageGroupsRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")
        assigned_ids = await self._repository.get_assigned_group_ids_for_package_ordered(db, package_id=package_id)
        ordered_ids = self._validate_exact_assignment_ids(
            requested_ids=data.group_ids,
            assigned_ids=assigned_ids,
            field_name="group_ids",
        )
        await self._repository.reorder_package_groups(db, package_id=package_id, group_ids=ordered_ids)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_REORDER_DIAGNOSTIC_GROUPS_IN_PACKAGE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return {"diagnostic_package_id": package_id, "group_ids": ordered_ids}

    async def update_package_status(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        data: DiagnosticPackageStatusUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> DiagnosticPackageResponse:
        self._ensure_employee_access(employee)
        status = self._normalize_lower(data.status)
        if status is None or status not in _ALLOWED_STATUS_VALUES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        existing = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if existing is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        updated = await self._repository.update_package_status(db, package_id=package_id, status=status)
        if updated is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_DIAGNOSTIC_PACKAGE_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return await self._package_response_with_test_count(db, updated)

    async def delete_package(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        existing = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if existing is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")
        await self._repository.delete_package(db, package_id=package_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_PACKAGE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def get_filter_chips(self, db, *, chip_for: str | None = None) -> list[FilterChipResponse]:
        scope = self._normalize_lower(chip_for) if chip_for is not None else "public_package"
        if scope is None:
            scope = "public_package"
        if scope not in _ALLOWED_CHIP_FOR:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        rows = await self._repository.get_all_filter_chips(db, chip_for=scope)
        return [
            FilterChipResponse(
                filter_chip_id=row.filter_chip_id,
                chip_key=row.chip_key,
                display_name=row.display_name,
                display_order=row.display_order,
                chip_for=row.chip_for or "public_package",
                status=row.status,
            )
            for row in rows
        ]

    async def create_filter_chip(
        self,
        db,
        *,
        employee: EmployeeContext,
        data: FilterChipCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> FilterChipResponse:
        self._ensure_employee_access(employee)
        payload = data.model_dump(exclude_none=True, mode="json")
        chip_key = self._normalize(payload.get("chip_key"))
        if chip_key is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        payload["chip_key"] = chip_key
        cf = self._normalize_lower(payload.get("chip_for"))
        if cf is None:
            cf = FilterChipForSchema.PUBLIC_PACKAGE.value
        if cf not in _ALLOWED_CHIP_FOR:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        payload["chip_for"] = cf

        dup = await self._repository.get_filter_chip_by_chip_key(db, chip_key=chip_key)
        if dup is not None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Chip key already exists")

        row = DiagnosticPackageFilterChip(**payload)
        try:
            created = await self._repository.create_filter_chip(db, row)
        except IntegrityError:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request") from None

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_DIAGNOSTIC_FILTER_CHIP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return FilterChipResponse(
            filter_chip_id=created.filter_chip_id,
            chip_key=created.chip_key,
            display_name=created.display_name,
            display_order=created.display_order,
            chip_for=created.chip_for or "public_package",
            status=created.status,
        )

    async def update_filter_chip(
        self,
        db,
        *,
        employee: EmployeeContext,
        filter_chip_id: int,
        data: FilterChipUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> FilterChipResponse:
        self._ensure_employee_access(employee)
        existing = await self._repository.get_filter_chip_by_id(db, filter_chip_id=filter_chip_id)
        if existing is None:
            raise AppError(
                status_code=404, error_code="DIAGNOSTIC_FILTER_CHIP_NOT_FOUND", message="Filter chip does not exist"
            )

        payload = data.model_dump(exclude_none=True, mode="json")
        if "chip_key" in payload:
            ck = self._normalize(payload.get("chip_key"))
            if ck is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["chip_key"] = ck
            other = await self._repository.get_filter_chip_by_chip_key(db, chip_key=ck)
            if other is not None and other.filter_chip_id != filter_chip_id:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Chip key already exists")

        status = self._normalize_lower(payload.get("status"))
        if status is not None and status not in _ALLOWED_STATUS_VALUES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if status is not None:
            payload["status"] = status
        if "chip_for" in payload:
            cf = self._normalize_lower(payload.get("chip_for"))
            if cf is None or cf not in _ALLOWED_CHIP_FOR:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["chip_for"] = cf

        try:
            updated = await self._repository.update_filter_chip(db, filter_chip_id=filter_chip_id, data=payload)
        except IntegrityError:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request") from None
        if updated is None:
            raise AppError(
                status_code=404, error_code="DIAGNOSTIC_FILTER_CHIP_NOT_FOUND", message="Filter chip does not exist"
            )

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_DIAGNOSTIC_FILTER_CHIP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return FilterChipResponse(
            filter_chip_id=updated.filter_chip_id,
            chip_key=updated.chip_key,
            display_name=updated.display_name,
            display_order=updated.display_order,
            chip_for=updated.chip_for or "public_package",
            status=updated.status,
        )

    async def delete_filter_chip(
        self,
        db,
        *,
        employee: EmployeeContext,
        filter_chip_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        existing = await self._repository.get_filter_chip_by_id(db, filter_chip_id=filter_chip_id)
        if existing is None:
            raise AppError(
                status_code=404, error_code="DIAGNOSTIC_FILTER_CHIP_NOT_FOUND", message="Filter chip does not exist"
            )

        await self._repository.delete_filter_chip(db, filter_chip_id=filter_chip_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_FILTER_CHIP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def assign_filter_chip_to_package(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        data: PackageFilterChipAssign,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> PackageFilterChipResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        chip = await self._repository.get_filter_chip_by_id(db, filter_chip_id=data.filter_chip_id)
        if chip is None:
            raise AppError(
                status_code=404, error_code="DIAGNOSTIC_FILTER_CHIP_NOT_FOUND", message="Filter chip does not exist"
            )

        existing_link = await self._repository.get_filter_chip_link(
            db, package_id=package_id, filter_chip_id=data.filter_chip_id
        )
        if existing_link is not None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Filter chip already on package")

        display_order = data.display_order
        if display_order is None:
            max_ord = await self._repository.get_max_filter_chip_link_display_order(db, package_id=package_id)
            display_order = (int(max_ord) if max_ord is not None else 0) + 1

        link = await self._repository.add_filter_chip_link(
            db,
            package_id=package_id,
            filter_chip_id=data.filter_chip_id,
            display_order=display_order,
        )
        link.filter_chip = chip

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_ASSIGN_DIAGNOSTIC_PACKAGE_FILTER_CHIP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return PackageFilterChipResponse(
            filter_chip_id=chip.filter_chip_id,
            chip_key=chip.chip_key,
            display_name=chip.display_name,
            display_order=link.display_order,
        )

    async def remove_filter_chip_from_package(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        filter_chip_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        n = await self._repository.delete_filter_chip_link(
            db, package_id=package_id, filter_chip_id=filter_chip_id
        )
        if n == 0:
            raise AppError(
                status_code=404,
                error_code="DIAGNOSTIC_FILTER_CHIP_LINK_NOT_FOUND",
                message="Filter chip is not assigned to this package",
            )

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_REMOVE_DIAGNOSTIC_PACKAGE_FILTER_CHIP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def assign_filter_chip_to_test_group(
        self,
        db,
        *,
        employee: EmployeeContext,
        group_id: int,
        data: PackageFilterChipAssign,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> PackageFilterChipResponse:
        self._ensure_employee_access(employee)
        group = await self._repository.get_group_by_id(db, group_id=group_id)
        if group is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        chip = await self._repository.get_filter_chip_by_id(db, filter_chip_id=data.filter_chip_id)
        if chip is None:
            raise AppError(
                status_code=404, error_code="DIAGNOSTIC_FILTER_CHIP_NOT_FOUND", message="Filter chip does not exist"
            )

        existing_link = await self._repository.get_group_filter_chip_link(
            db, group_id=group_id, filter_chip_id=data.filter_chip_id
        )
        if existing_link is not None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Filter chip already on group")

        display_order = data.display_order
        if display_order is None:
            max_ord = await self._repository.get_max_group_filter_chip_link_display_order(db, group_id=group_id)
            display_order = (int(max_ord) if max_ord is not None else 0) + 1

        link = await self._repository.add_group_filter_chip_link(
            db,
            group_id=group_id,
            filter_chip_id=data.filter_chip_id,
            display_order=display_order,
        )
        link.filter_chip = chip

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_ASSIGN_DIAGNOSTIC_TEST_GROUP_FILTER_CHIP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return PackageFilterChipResponse(
            filter_chip_id=chip.filter_chip_id,
            chip_key=chip.chip_key,
            display_name=chip.display_name,
            display_order=link.display_order,
        )

    async def remove_filter_chip_from_test_group(
        self,
        db,
        *,
        employee: EmployeeContext,
        group_id: int,
        filter_chip_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        group = await self._repository.get_group_by_id(db, group_id=group_id)
        if group is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        n = await self._repository.delete_group_filter_chip_link(
            db, group_id=group_id, filter_chip_id=filter_chip_id
        )
        if n == 0:
            raise AppError(
                status_code=404,
                error_code="DIAGNOSTIC_FILTER_CHIP_LINK_NOT_FOUND",
                message="Filter chip is not assigned to this group",
            )

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_REMOVE_DIAGNOSTIC_TEST_GROUP_FILTER_CHIP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def create_reason(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        data: ReasonCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> ReasonResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        created = await self._repository.create_reason(
            db,
            package_id=package_id,
            data=DiagnosticPackageReason(**data.model_dump(exclude_none=True)),
        )
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_DIAGNOSTIC_REASON",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_reason_response(created)

    async def update_reason(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        reason_id: int,
        data: ReasonUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> ReasonResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        reason = await self._repository.get_reason_by_id(db, reason_id=reason_id)
        if reason is None or reason.diagnostic_package_id != package_id:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_REASON_NOT_FOUND", message="Reason does not exist")

        updated = await self._repository.update_reason(db, reason_id=reason_id, data=data.model_dump(exclude_none=True))
        if updated is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_REASON_NOT_FOUND", message="Reason does not exist")

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_DIAGNOSTIC_REASON",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_reason_response(updated)

    async def delete_reason(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        reason_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        reason = await self._repository.get_reason_by_id(db, reason_id=reason_id)
        if reason is None or reason.diagnostic_package_id != package_id:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_REASON_NOT_FOUND", message="Reason does not exist")

        await self._repository.delete_reason(db, reason_id=reason_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_REASON",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def create_tag(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        data: TagCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> TagResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        payload = data.model_dump(exclude_none=True)
        payload["tag_name"] = self._normalize(payload.get("tag_name"))
        if payload["tag_name"] is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        created = await self._repository.create_tag(db, package_id=package_id, data=DiagnosticPackageTag(**payload))
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_DIAGNOSTIC_TAG",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_tag_response(created)

    async def delete_tag(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        tag_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        tag = await self._repository.get_tag_by_id(db, tag_id=tag_id)
        if tag is None or tag.diagnostic_package_id != package_id:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TAG_NOT_FOUND", message="Tag does not exist")

        await self._repository.delete_tag(db, tag_id=tag_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_TAG",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )


    async def create_sample(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        data: SampleCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> SampleResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        payload = data.model_dump(exclude_none=True)
        sample_type = self._normalize(payload.get("sample_type"))
        if sample_type is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        payload["sample_type"] = sample_type

        created = await self._repository.create_sample(
            db,
            package_id=package_id,
            data=DiagnosticPackageSample(**payload),
        )
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_DIAGNOSTIC_SAMPLE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_sample_response(created)

    async def update_sample(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        sample_id: int,
        data: SampleUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> SampleResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        sample = await self._repository.get_sample_by_id(db, sample_id=sample_id)
        if sample is None or sample.diagnostic_package_id != package_id:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_SAMPLE_NOT_FOUND", message="Sample does not exist")

        payload = data.model_dump(exclude_none=True)
        if "sample_type" in payload:
            sample_type = self._normalize(payload["sample_type"])
            if sample_type is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["sample_type"] = sample_type

        updated = await self._repository.update_sample(db, sample_id=sample_id, data=payload)
        if updated is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_SAMPLE_NOT_FOUND", message="Sample does not exist")

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_DIAGNOSTIC_SAMPLE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_sample_response(updated)

    async def delete_sample(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        sample_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        sample = await self._repository.get_sample_by_id(db, sample_id=sample_id)
        if sample is None or sample.diagnostic_package_id != package_id:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_SAMPLE_NOT_FOUND", message="Sample does not exist")

        await self._repository.delete_sample(db, sample_id=sample_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_SAMPLE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def create_preparation(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        data: PreparationCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> PreparationResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        payload = data.model_dump(exclude_none=True)
        title = self._normalize(payload.get("preparation_title"))
        if title is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        payload["preparation_title"] = title

        created = await self._repository.create_preparation(
            db,
            package_id=package_id,
            data=DiagnosticPackagePreparation(**payload),
        )
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_DIAGNOSTIC_PREPARATION",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_preparation_response(created)

    async def update_preparation(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        preparation_id: int,
        data: PreparationUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> PreparationResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        preparation = await self._repository.get_preparation_by_id(db, preparation_id=preparation_id)
        if preparation is None or preparation.diagnostic_package_id != package_id:
            raise AppError(
                status_code=404,
                error_code="DIAGNOSTIC_PREPARATION_NOT_FOUND",
                message="Preparation does not exist",
            )

        payload = data.model_dump(exclude_none=True)
        if "preparation_title" in payload:
            title = self._normalize(payload["preparation_title"])
            if title is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["preparation_title"] = title

        updated = await self._repository.update_preparation(
            db,
            preparation_id=preparation_id,
            data=payload,
        )
        if updated is None:
            raise AppError(
                status_code=404,
                error_code="DIAGNOSTIC_PREPARATION_NOT_FOUND",
                message="Preparation does not exist",
            )

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_DIAGNOSTIC_PREPARATION",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_preparation_response(updated)

    async def delete_preparation(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        preparation_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        preparation = await self._repository.get_preparation_by_id(db, preparation_id=preparation_id)
        if preparation is None or preparation.diagnostic_package_id != package_id:
            raise AppError(
                status_code=404,
                error_code="DIAGNOSTIC_PREPARATION_NOT_FOUND",
                message="Preparation does not exist",
            )

        await self._repository.delete_preparation(db, preparation_id=preparation_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_PREPARATION",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
