"""Diagnostics service.

Business rules:
- Routers must stay thin and delegate business logic here.
- All mutations must be audit logged.
- Parent-child integrity checks are enforced before child mutations.
"""

from __future__ import annotations

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.diagnostics.models import (
    DiagnosticPackage,
    DiagnosticPackageFilter,
    DiagnosticPackagePreparation,
    DiagnosticPackageReason,
    DiagnosticPackageSample,
    DiagnosticPackageTag,
    DiagnosticTest,
    DiagnosticTestGroup,
)
from modules.diagnostics.repository import DiagnosticsRepository
from modules.diagnostics.schemas import (
    DiagnosticPackageCreate,
    DiagnosticPackageDetailResponse,
    DiagnosticPackageListItem,
    DiagnosticPackageResponse,
    DiagnosticPackageStatusUpdate,
    DiagnosticPackageUpdate,
    FilterCreate,
    FilterResponse,
    FilterUpdate,
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
    TestCreate,
    TestGroupCreate,
    TestGroupResponse,
    TestGroupUpdate,
    TestResponse,
    TestUpdate,
)
from modules.employee.service import EmployeeContext


_ALLOWED_COLLECTION_TYPES = {"home_collection", "centre_visit"}
_ALLOWED_GENDER_VALUES = {"male", "female", "both"}
_ALLOWED_STATUS_VALUES = {"active", "inactive"}
_ALLOWED_FILTER_TYPES = {"gender", "tag"}


def _discount_percent(price: float | None, original_price: float | None) -> int | None:
    if original_price is None or original_price == 0 or price is None:
        return None
    return round(((original_price - price) / original_price) * 100)


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

    def _to_tag_response(self, row: DiagnosticPackageTag) -> TagResponse:
        return TagResponse(
            tag_id=row.tag_id,
            diagnostic_package_id=row.diagnostic_package_id,
            tag_name=row.tag_name,
            display_order=row.display_order,
        )

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

    def _to_package_response(self, row: DiagnosticPackage) -> DiagnosticPackageResponse:
        price = float(row.price) if row.price is not None else None
        original_price = float(row.original_price) if row.original_price is not None else None
        return DiagnosticPackageResponse(
            diagnostic_package_id=row.diagnostic_package_id,
            reference_id=row.reference_id,
            package_name=row.package_name,
            diagnostic_provider=row.diagnostic_provider,
            no_of_tests=row.no_of_tests,
            report_duration_hours=row.report_duration_hours,
            collection_type=row.collection_type,
            about_text=row.about_text,
            bookings_count=row.bookings_count,
            price=price,
            original_price=original_price,
            is_most_popular=row.is_most_popular,
            gender_suitability=row.gender_suitability,
            status=row.status,
            created_at=row.created_at,
            discount_percent=_discount_percent(price, original_price),
        )

    async def get_packages(self, db, *, gender: str | None, tag: str | None) -> list[DiagnosticPackageListItem]:
        gender_value = self._normalize_lower(gender)
        tag_value = self._normalize(tag)

        if gender_value is not None and gender_value not in _ALLOWED_GENDER_VALUES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        rows = await self._repository.get_all_packages(db, gender=gender_value, tag=tag_value)
        items: list[DiagnosticPackageListItem] = []
        for row in rows:
            price = float(row.price) if row.price is not None else None
            original_price = float(row.original_price) if row.original_price is not None else None
            tags = sorted(
                list(row.tags),
                key=lambda t: (t.display_order is None, t.display_order or 0, t.tag_id),
            )
            items.append(
                DiagnosticPackageListItem(
                    diagnostic_package_id=row.diagnostic_package_id,
                    package_name=row.package_name,
                    no_of_tests=row.no_of_tests,
                    report_duration_hours=row.report_duration_hours,
                    collection_type=row.collection_type,
                    price=price,
                    original_price=original_price,
                    discount_percent=_discount_percent(price, original_price),
                    is_most_popular=row.is_most_popular,
                    gender_suitability=row.gender_suitability,
                    status=row.status,
                    tags=[self._to_tag_response(tag_row) for tag_row in tags],
                )
            )
        return items

    async def get_package_detail(self, db, *, package_id: int) -> DiagnosticPackageDetailResponse:
        row = await self._repository.get_package_by_id(db, package_id=package_id)
        if row is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        package = self._to_package_response(row)
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
        )

    async def get_package_tests(self, db, *, package_id: int) -> list[TestGroupResponse]:
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        groups = await self._repository.get_package_tests(db, package_id=package_id)
        response: list[TestGroupResponse] = []
        for group in groups:
            tests_sorted = sorted(list(group.tests), key=lambda t: (t.display_order is None, t.display_order or 0, t.test_id))
            response.append(
                TestGroupResponse(
                    group_id=group.group_id,
                    diagnostic_package_id=group.diagnostic_package_id,
                    group_name=group.group_name,
                    test_count=group.test_count,
                    display_order=group.display_order,
                    tests=[
                        TestResponse(
                            test_id=test.test_id,
                            group_id=test.group_id,
                            test_name=test.test_name,
                            display_order=test.display_order,
                            is_available=test.is_available,
                        )
                        for test in tests_sorted
                    ],
                )
            )
        return response

    async def create_package(
        self,
        db,
        *,
        employee: EmployeeContext,
        data: DiagnosticPackageCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> DiagnosticPackageResponse:
        self._ensure_employee_access(employee)
        payload = data.model_dump(exclude_none=True)
        self._validate_package_common_fields(payload)
        payload.setdefault("status", "active")

        package = DiagnosticPackage(**payload)
        package = await self._repository.create_package(db, package)

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_DIAGNOSTIC_PACKAGE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_package_response(package)

    async def update_package(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        data: DiagnosticPackageUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> DiagnosticPackageResponse:
        self._ensure_employee_access(employee)
        existing = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if existing is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        payload = data.model_dump(exclude_none=True)
        if "package_name" in payload:
            name = self._normalize(payload["package_name"])
            if name is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["package_name"] = name
        self._validate_package_common_fields(payload)

        updated = await self._repository.update_package(db, package_id=package_id, data=payload)
        if updated is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_DIAGNOSTIC_PACKAGE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return self._to_package_response(updated)

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
        return self._to_package_response(updated)

    async def get_filters(self, db) -> list[FilterResponse]:
        rows = await self._repository.get_all_filters(db)
        return [
            FilterResponse(
                filter_id=row.filter_id,
                filter_key=row.filter_key,
                display_name=row.display_name,
                display_order=row.display_order,
                filter_type=row.filter_type,
                status=row.status,
            )
            for row in rows
        ]

    async def create_filter(
        self,
        db,
        *,
        employee: EmployeeContext,
        data: FilterCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> FilterResponse:
        self._ensure_employee_access(employee)
        payload = data.model_dump(exclude_none=True)
        filter_type = self._normalize_lower(payload.get("filter_type"))
        if filter_type is not None and filter_type not in _ALLOWED_FILTER_TYPES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if filter_type is not None:
            payload["filter_type"] = filter_type

        created = await self._repository.create_filter(db, DiagnosticPackageFilter(**payload))
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_DIAGNOSTIC_FILTER",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return FilterResponse(
            filter_id=created.filter_id,
            filter_key=created.filter_key,
            display_name=created.display_name,
            display_order=created.display_order,
            filter_type=created.filter_type,
            status=created.status,
        )

    async def update_filter(
        self,
        db,
        *,
        employee: EmployeeContext,
        filter_id: int,
        data: FilterUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> FilterResponse:
        self._ensure_employee_access(employee)
        existing = await self._repository.get_filter_by_id(db, filter_id=filter_id)
        if existing is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_FILTER_NOT_FOUND", message="Filter does not exist")

        payload = data.model_dump(exclude_none=True)
        filter_type = self._normalize_lower(payload.get("filter_type"))
        if filter_type is not None and filter_type not in _ALLOWED_FILTER_TYPES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if filter_type is not None:
            payload["filter_type"] = filter_type

        status = self._normalize_lower(payload.get("status"))
        if status is not None and status not in _ALLOWED_STATUS_VALUES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        if status is not None:
            payload["status"] = status

        updated = await self._repository.update_filter(db, filter_id=filter_id, data=payload)
        if updated is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_FILTER_NOT_FOUND", message="Filter does not exist")

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_DIAGNOSTIC_FILTER",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return FilterResponse(
            filter_id=updated.filter_id,
            filter_key=updated.filter_key,
            display_name=updated.display_name,
            display_order=updated.display_order,
            filter_type=updated.filter_type,
            status=updated.status,
        )

    async def delete_filter(
        self,
        db,
        *,
        employee: EmployeeContext,
        filter_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        existing = await self._repository.get_filter_by_id(db, filter_id=filter_id)
        if existing is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_FILTER_NOT_FOUND", message="Filter does not exist")

        await self._repository.delete_filter(db, filter_id=filter_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_FILTER",
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

    async def create_test_group(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        data: TestGroupCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> TestGroupResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        payload = data.model_dump(exclude_none=True)
        group_name = self._normalize(payload.get("group_name"))
        if group_name is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        payload["group_name"] = group_name

        created = await self._repository.create_test_group(
            db,
            package_id=package_id,
            data=DiagnosticTestGroup(**payload),
        )
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_DIAGNOSTIC_TEST_GROUP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return TestGroupResponse(
            group_id=created.group_id,
            diagnostic_package_id=created.diagnostic_package_id,
            group_name=created.group_name,
            test_count=created.test_count,
            display_order=created.display_order,
            tests=[],
        )

    async def update_test_group(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        group_id: int,
        data: TestGroupUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> TestGroupResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        group = await self._repository.get_test_group_by_id(db, group_id=group_id)
        if group is None or group.diagnostic_package_id != package_id:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        payload = data.model_dump(exclude_none=True)
        if "group_name" in payload:
            group_name = self._normalize(payload["group_name"])
            if group_name is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["group_name"] = group_name

        updated = await self._repository.update_test_group(db, group_id=group_id, data=payload)
        if updated is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_DIAGNOSTIC_TEST_GROUP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return TestGroupResponse(
            group_id=updated.group_id,
            diagnostic_package_id=updated.diagnostic_package_id,
            group_name=updated.group_name,
            test_count=updated.test_count,
            display_order=updated.display_order,
            tests=[],
        )

    async def delete_test_group(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        group_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        group = await self._repository.get_test_group_by_id(db, group_id=group_id)
        if group is None or group.diagnostic_package_id != package_id:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        await self._repository.delete_test_group(db, group_id=group_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_TEST_GROUP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def create_test(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        group_id: int,
        data: TestCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> TestResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        group = await self._repository.get_test_group_by_id(db, group_id=group_id)
        if group is None or group.diagnostic_package_id != package_id:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        payload = data.model_dump(exclude_none=True)
        test_name = self._normalize(payload.get("test_name"))
        if test_name is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        payload["test_name"] = test_name

        created = await self._repository.create_test(db, group_id=group_id, data=DiagnosticTest(**payload))
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_DIAGNOSTIC_TEST",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return TestResponse(
            test_id=created.test_id,
            group_id=created.group_id,
            test_name=created.test_name,
            display_order=created.display_order,
            is_available=created.is_available,
        )

    async def update_test(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        group_id: int,
        test_id: int,
        data: TestUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> TestResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        group = await self._repository.get_test_group_by_id(db, group_id=group_id)
        if group is None or group.diagnostic_package_id != package_id:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        test = await self._repository.get_test_by_id(db, test_id=test_id)
        if test is None or test.group_id != group_id:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_NOT_FOUND", message="Test does not exist")

        payload = data.model_dump(exclude_none=True)
        if "test_name" in payload:
            test_name = self._normalize(payload["test_name"])
            if test_name is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["test_name"] = test_name

        updated = await self._repository.update_test(db, test_id=test_id, data=payload)
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
        return TestResponse(
            test_id=updated.test_id,
            group_id=updated.group_id,
            test_name=updated.test_name,
            display_order=updated.display_order,
            is_available=updated.is_available,
        )

    async def delete_test(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        group_id: int,
        test_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        group = await self._repository.get_test_group_by_id(db, group_id=group_id)
        if group is None or group.diagnostic_package_id != package_id:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        test = await self._repository.get_test_by_id(db, test_id=test_id)
        if test is None or test.group_id != group_id:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_NOT_FOUND", message="Test does not exist")

        await self._repository.delete_test(db, test_id=test_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_TEST",
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
