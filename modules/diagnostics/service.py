"""Diagnostics service.

Business rules:
- Routers must stay thin and delegate business logic here.
- All mutations must be audit logged.
- Parent-child integrity checks are enforced before child mutations.
"""

from __future__ import annotations

import logging
import re
from collections import Counter

from sqlalchemy.exc import IntegrityError

from common.listing import sanitize_list_search
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
from modules.diagnostics.external_codes import (
    healthians_product_deal_type_id,
    healthians_profile_deal_type_id,
    is_healthians_diagnostic_provider,
    provider_parameter_key,
)
from modules.diagnostics.healthians import client as healthians_client
from modules.diagnostics.repository import DiagnosticsRepository
from modules.diagnostics.schemas import (
    AssignGroupsToPackageRequest,
    AssignGroupsToPackageResponse,
    ForkTestGroupForPackageResponse,
    AssignTestsToGroupRequest,
    AssignTestsToGroupResponse,
    DiagnosticPackageCreate,
    DiagnosticPackageDetailResponse,
    DiagnosticPackageGroupListItem,
    DiagnosticPackageListItem,
    DiagnosticPackagePeerResponse,
    DiagnosticPackageResponse,
    DiagnosticPackageStatusUpdate,
    DiagnosticPackageUpdate,
    DiagnosticWarning,
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
    ProviderTestDiffCounts,
    ProviderTestDiffMatchedItem,
    ProviderTestDiffNotLinkedItem,
    ProviderTestDiffOnlyAtProviderItem,
    ProviderTestDiffOnlyInPackageItem,
    ProviderTestDiffResponse,
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
    TestGroupPeerResponse,
    TestGroupPackageListItem,
    TestGroupProfileGroupListItem,
    TestGroupResponse,
    TestGroupUpdate,
    PackageFilterChipAssign,
    PackageFilterChipResponse,
    ReorderGroupTestsRequest,
    ReorderPackageGroupsRequest,
)
from modules.employee.service import EmployeeContext

logger = logging.getLogger(__name__)

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

    def _slugify_group_key(self, value: str | None) -> str | None:
        normalized = self._normalize(value)
        if normalized is None:
            return None
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", normalized.lower())
        slug = re.sub(r"_+", "_", slug).strip("_")
        return slug if slug else None

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

    @staticmethod
    def _provider_key(provider: str | None) -> str | None:
        if provider is None:
            return None
        if hasattr(provider, "value"):
            provider = provider.value
        text = str(provider).strip()
        if not text:
            return None
        return text.lower()

    @staticmethod
    def _provider_display_label(provider: str | None) -> str:
        key = (provider or "").strip().lower()
        if key == "orange_health":
            return "Orange Health"
        if key == "healthlabs":
            return "HealthLabs"
        if key in ("healthians",):
            return "Healthians"
        text = (provider or "").strip()
        return text if text else "Lab"

    def _peer_responses(
        self,
        package: DiagnosticPackage,
        members: list[DiagnosticPackage],
    ) -> list[DiagnosticPackagePeerResponse]:
        return [
            DiagnosticPackagePeerResponse(
                diagnostic_package_id=member.diagnostic_package_id,
                package_name=member.package_name,
                diagnostic_provider=member.diagnostic_provider,
                price=float(member.price) if member.price is not None else None,
                status=member.status,
                is_default_for_group=bool(member.is_default_for_group),
            )
            for member in members
            if member.diagnostic_package_id != package.diagnostic_package_id
        ]

    @staticmethod
    def _provider_enum_key(provider: str | None) -> str | None:
        if provider is None:
            return None
        if hasattr(provider, "value"):
            provider = provider.value
        text = str(provider).strip()
        return text if text else None

    def _package_group_map(self, members: list[DiagnosticPackage]) -> dict[str, int]:
        mapping: dict[str, int] = {}
        for member in members:
            key = self._provider_enum_key(member.diagnostic_provider)
            if key is None:
                continue
            mapping[key] = int(member.diagnostic_package_id)
        return mapping

    def _test_group_profile_map(self, members: list[DiagnosticTestGroup]) -> dict[str, int]:
        mapping: dict[str, int] = {}
        for member in members:
            key = self._provider_enum_key(member.diagnostic_provider)
            if key is None:
                continue
            mapping[key] = int(member.group_id)
        return mapping

    def _test_group_peer_responses(
        self,
        group: DiagnosticTestGroup,
        members: list[DiagnosticTestGroup],
    ) -> list[TestGroupPeerResponse]:
        return [
            TestGroupPeerResponse(
                group_id=member.group_id,
                group_name=member.group_name,
                diagnostic_provider=member.diagnostic_provider,
                price=float(member.price) if member.price is not None else None,
                is_default_for_group=bool(member.is_default_for_group),
            )
            for member in members
            if member.group_id != group.group_id
        ]

    async def _ensure_test_group_provider_matches_assigned_packages(
        self,
        db,
        *,
        group_id: int,
        diagnostic_provider: str | None,
    ) -> None:
        incoming_key = self._provider_key(diagnostic_provider)
        if incoming_key is None:
            return
        packages = await self._repository.list_packages_for_test_group(db, group_id=group_id)
        mismatched: list[dict[str, object]] = []
        for pkg in packages:
            package_key = self._provider_key(pkg.diagnostic_provider)
            if package_key is None:
                continue
            if package_key != incoming_key:
                mismatched.append(
                    {
                        "diagnostic_package_id": int(pkg.diagnostic_package_id),
                        "package_name": pkg.package_name,
                        "diagnostic_provider": self._provider_enum_key(pkg.diagnostic_provider),
                    }
                )
        if not mismatched:
            return
        names = ", ".join(
            f"{item['package_name']} ({item['diagnostic_package_id']})" for item in mismatched
        )
        raise AppError(
            status_code=400,
            error_code="DIAGNOSTIC_TEST_GROUP_PACKAGE_PROVIDER_MISMATCH",
            message=(
                f"This test group is assigned to packages with a different lab provider ({names}). "
                "Remove this test group from those packages before setting the provider."
            ),
            details={"packages": mismatched},
        )

    def _build_package_warnings(
        self,
        *,
        has_unmapped: bool,
        has_null_provider_test_groups: bool,
    ) -> list[DiagnosticWarning]:
        warnings: list[DiagnosticWarning] = []
        if has_unmapped:
            warnings.append(
                DiagnosticWarning(code="unmapped_tests", message="Some tests are unmapped")
            )
        if has_null_provider_test_groups:
            warnings.append(
                DiagnosticWarning(
                    code="test_group_provider_missing",
                    message="Some assigned test groups have no lab provider set",
                )
            )
        return warnings

    async def _resolve_same_as_test_group_profile(
        self,
        db,
        *,
        group_id: int,
        provider: str | None,
        same_as_group_id: int,
    ) -> int:
        if same_as_group_id == group_id:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="A test group cannot be linked to itself",
            )
        target = await self._repository.get_group_by_id(db, group_id=same_as_group_id)
        if target is None:
            raise AppError(
                status_code=404,
                error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND",
                message="Linked test group does not exist",
            )

        provider_key = self._provider_key(provider)
        profile_group_id = target.profile_group_id
        if profile_group_id is None:
            profile_group = await self._repository.create_profile_group(db)
            target.profile_group_id = profile_group.profile_group_id
            profile_group_id = profile_group.profile_group_id
            if not target.is_default_for_group:
                target.is_default_for_group = True
            db.add(target)
            await db.flush()

        members = await self._repository.list_test_groups_in_profile_group(
            db, profile_group_id=int(profile_group_id)
        )
        for member in members:
            if member.group_id == group_id:
                continue
            member_key = self._provider_key(member.diagnostic_provider)
            if provider_key is not None and member_key == provider_key:
                label = self._provider_display_label(member.diagnostic_provider)
                raise AppError(
                    status_code=400,
                    error_code="DIAGNOSTIC_TEST_GROUP_PROVIDER_CONFLICT",
                    message=(
                        f"{label} is already linked to {member.group_name}. "
                        "Edit that test group, or pick a different one."
                    ),
                )

        return int(profile_group_id)

    async def _promote_test_group_default_if_needed(
        self,
        db,
        *,
        profile_group_id: int | None,
        exclude_group_id: int | None = None,
    ) -> None:
        if profile_group_id is None:
            return
        if await self._repository.profile_group_has_default(db, profile_group_id=int(profile_group_id)):
            return
        await self._repository.promote_min_price_default_in_profile_group(
            db,
            profile_group_id=int(profile_group_id),
            exclude_group_id=exclude_group_id,
        )

    async def _promote_default_if_needed(
        self,
        db,
        *,
        group_id: int | None,
        exclude_package_id: int | None = None,
    ) -> None:
        if group_id is None:
            return
        if await self._repository.group_has_default(db, group_id=int(group_id)):
            return
        await self._repository.promote_min_price_default_in_group(
            db,
            group_id=int(group_id),
            exclude_package_id=exclude_package_id,
        )

    async def _resolve_same_as_package_group(
        self,
        db,
        *,
        package_id: int,
        provider: str | None,
        same_as_package_id: int,
    ) -> int:
        if same_as_package_id == package_id:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="A package cannot be linked to itself",
            )
        target = await self._repository.get_package_by_id_basic(db, package_id=same_as_package_id)
        if target is None:
            raise AppError(
                status_code=404,
                error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND",
                message="Linked package does not exist",
            )

        provider_key = self._provider_key(provider)
        group_id = target.package_group_id
        if group_id is None:
            group = await self._repository.create_package_group(db)
            target.package_group_id = group.package_group_id
            group_id = group.package_group_id
            db.add(target)
            await db.flush()

        members = await self._repository.list_packages_in_group(db, group_id=int(group_id))
        for member in members:
            if member.diagnostic_package_id == package_id:
                continue
            member_key = self._provider_key(member.diagnostic_provider)
            if provider_key is not None and member_key == provider_key:
                label = self._provider_display_label(member.diagnostic_provider)
                raise AppError(
                    status_code=400,
                    error_code="DIAGNOSTIC_PACKAGE_PROVIDER_CONFLICT",
                    message=(
                        f"{label} is already linked to {member.package_name}. "
                        "Edit that package, or pick a different one."
                    ),
                )

        return int(group_id)

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

    def _filter_chip_responses_from_links(
        self, links: list
    ) -> list[PackageFilterChipResponse]:
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

    def _package_filter_chip_responses(self, package: DiagnosticPackage) -> list[PackageFilterChipResponse]:
        links = sorted(
            [ln for ln in package.filter_chip_links if ln.diagnostic_package_id is not None],
            key=lambda ln: (ln.display_order is None, ln.display_order or 0, ln.link_id),
        )
        return self._filter_chip_responses_from_links(links)

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

    @staticmethod
    def _dec_to_float(val) -> float | None:
        return float(val) if val is not None else None

    def _to_health_parameter_response(self, row: HealthParameter) -> HealthParameterResponse:
        pt = row.parameter_type
        parameter_type = ParameterType(pt.value) if isinstance(pt, ORMParameterType) else ParameterType(pt)
        return HealthParameterResponse(
            test_id=row.test_id,
            parameter_type=parameter_type,
            test_name=row.test_name,
            healthians_parameter_key=row.healthians_parameter_key,
            orangehealth_parameter_key=row.orangehealth_parameter_key,
            parameter_key=row.parameter_key,
            unit=row.unit,
            meaning=row.meaning,
            low_risk_lower_range_male=self._dec_to_float(row.low_risk_lower_range_male),
            low_risk_higher_range_male=self._dec_to_float(row.low_risk_higher_range_male),
            moderate_risk_lower_range_male=self._dec_to_float(row.moderate_risk_lower_range_male),
            moderate_risk_higher_range_male=self._dec_to_float(row.moderate_risk_higher_range_male),
            high_risk_lower_range_male=self._dec_to_float(row.high_risk_lower_range_male),
            high_risk_higher_range_male=self._dec_to_float(row.high_risk_higher_range_male),
            low_risk_lower_range_female=self._dec_to_float(row.low_risk_lower_range_female),
            low_risk_higher_range_female=self._dec_to_float(row.low_risk_higher_range_female),
            moderate_risk_lower_range_female=self._dec_to_float(row.moderate_risk_lower_range_female),
            moderate_risk_higher_range_female=self._dec_to_float(row.moderate_risk_higher_range_female),
            high_risk_lower_range_female=self._dec_to_float(row.high_risk_lower_range_female),
            high_risk_higher_range_female=self._dec_to_float(row.high_risk_higher_range_female),
            causes_when_high=row.causes_when_high,
            causes_when_low=row.causes_when_low,
            effects_when_high=row.effects_when_high,
            effects_when_low=row.effects_when_low,
            what_to_do_when_low=row.what_to_do_when_low,
            what_to_do_when_high=row.what_to_do_when_high,
            is_available=bool(row.is_available),
            display_order=row.display_order,
            price=self._dec_to_float(row.price),
            original_price=self._dec_to_float(row.original_price),
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
        same_test_groups: list[TestGroupPeerResponse] | None = None,
    ) -> TestGroupResponse:
        tests_value = tests or []
        resolved_count = test_count if test_count is not None else len(tests_value)
        price = float(row.price) if row.price is not None else None
        original_price = float(row.original_price) if row.original_price is not None else None
        chip_links = filter_chip_links if filter_chip_links is not None else []
        return TestGroupResponse(
            group_id=row.group_id,
            group_name=row.group_name,
            group_key=row.group_key,
            profile_group_id=row.profile_group_id,
            test_group_image=row.test_group_image,
            diagnostic_provider=row.diagnostic_provider,
            external_test_group_code=row.external_test_group_code,
            is_default_for_group=bool(row.is_default_for_group),
            display_order=row.display_order,
            test_count=resolved_count,
            price=price,
            discount=_discount_percent_label(price, original_price),
            discount_percent=_discount_percent(price, original_price),
            original_price=original_price,
            is_most_popular=bool(row.is_most_popular),
            has_allergy_screening=bool(row.has_allergy_screening),
            has_cancer_screening=bool(row.has_cancer_screening),
            gender_suitability=row.gender_suitability,
            package_for=row.package_for,
            forked_from_group_id=row.forked_from_group_id,
            same_test_groups=same_test_groups or [],
            tests=[self._to_health_parameter_response(test_row) for test_row in tests_value],
            filter_chips=self._group_filter_chip_link_responses(chip_links),
        )

    def _to_package_response(
        self,
        row: DiagnosticPackage,
        *,
        no_of_tests: int | None = None,
        same_packages: list[DiagnosticPackagePeerResponse] | None = None,
        has_allergy_screening: bool = False,
        has_cancer_screening: bool = False,
    ) -> DiagnosticPackageResponse:
        price = float(row.price) if row.price is not None else None
        original_price = float(row.original_price) if row.original_price is not None else None
        min_price = float(row.min_price) if row.min_price is not None else None
        return DiagnosticPackageResponse(
            diagnostic_package_id=row.diagnostic_package_id,
            reference_id=row.reference_id,
            package_name=row.package_name,
            package_image=row.package_image,
            diagnostic_provider=row.diagnostic_provider,
            external_package_code=row.external_package_code,
            created_by_user_id=row.created_by_user_id,
            no_of_tests=no_of_tests,
            report_duration_hours=row.report_duration_hours,
            collection_type=row.collection_type,
            health_areas_covered=row.health_areas_covered,
            about_text=row.about_text,
            bookings_count=row.bookings_count,
            price=price,
            original_price=original_price,
            min_price=min_price,
            is_most_popular=row.is_most_popular,
            is_default_for_group=bool(row.is_default_for_group),
            has_allergy_screening=has_allergy_screening,
            has_cancer_screening=has_cancer_screening,
            complementary_consultation=row.complementary_consultation,
            gender_suitability=row.gender_suitability,
            package_for=row.package_for,
            status=row.status,
            created_at=row.created_at,
            discount_percent=_discount_percent(price, original_price),
            package_group_id=row.package_group_id,
            same_packages=same_packages or [],
        )

    async def _package_response_with_test_count(self, db, row: DiagnosticPackage) -> DiagnosticPackageResponse:
        counts = await self._repository.count_distinct_tests_for_packages(
            db, package_ids=[row.diagnostic_package_id]
        )
        screening = await self._repository.screening_flags_for_packages(
            db, package_ids=[row.diagnostic_package_id]
        )
        allergy, cancer = screening.get(row.diagnostic_package_id, (False, False))
        same_packages: list[DiagnosticPackagePeerResponse] = []
        if row.package_group_id is not None:
            grouped = await self._repository.list_packages_by_group_ids(
                db, group_ids=[int(row.package_group_id)]
            )
            members = grouped.get(int(row.package_group_id), [])
            same_packages = self._peer_responses(row, members)
        return self._to_package_response(
            row,
            no_of_tests=counts.get(row.diagnostic_package_id, 0),
            same_packages=same_packages,
            has_allergy_screening=allergy,
            has_cancer_screening=cancer,
        )

    async def _package_rows_to_list_items(
        self,
        db,
        rows: list[DiagnosticPackage],
    ) -> list[DiagnosticPackageListItem]:
        pkg_ids = [r.diagnostic_package_id for r in rows]
        counts = await self._repository.count_distinct_tests_for_packages(db, package_ids=pkg_ids)
        screening_by_pkg = await self._repository.screening_flags_for_packages(db, package_ids=pkg_ids)
        tags_by_pkg = await self._repository.list_tags_for_package_ids(db, package_ids=pkg_ids)
        chip_links_by_pkg = await self._repository.list_filter_chip_links_for_package_ids(
            db, package_ids=pkg_ids
        )
        items: list[DiagnosticPackageListItem] = []
        for row in rows:
            price = float(row.price) if row.price is not None else None
            original_price = float(row.original_price) if row.original_price is not None else None
            min_price = float(row.min_price) if row.min_price is not None else None
            tags = tags_by_pkg.get(int(row.diagnostic_package_id), [])
            n_tests = counts.get(row.diagnostic_package_id, 0)
            allergy, cancer = screening_by_pkg.get(row.diagnostic_package_id, (False, False))
            items.append(
                DiagnosticPackageListItem(
                    diagnostic_package_id=row.diagnostic_package_id,
                    package_name=row.package_name,
                    package_image=row.package_image,
                    diagnostic_provider=row.diagnostic_provider,
                    display_order=row.display_order,
                    no_of_tests=n_tests,
                    report_duration_hours=row.report_duration_hours,
                    collection_type=row.collection_type,
                    price=price,
                    original_price=original_price,
                    min_price=min_price,
                    discount_percent=_discount_percent(price, original_price),
                    is_most_popular=row.is_most_popular,
                    has_allergy_screening=allergy,
                    has_cancer_screening=cancer,
                    complementary_consultation=row.complementary_consultation,
                    gender_suitability=row.gender_suitability,
                    tags=[self._to_tag_response(tag_row) for tag_row in tags],
                    filter_chips=self._filter_chip_responses_from_links(
                        chip_links_by_pkg.get(int(row.diagnostic_package_id), [])
                    ),
                    package_group_id=row.package_group_id,
                )
            )
        return items

    async def _package_rows_to_group_list_items(
        self,
        db,
        rows: list[DiagnosticPackage],
    ) -> list[DiagnosticPackageGroupListItem]:
        base_items = await self._package_rows_to_list_items(db, rows)
        pkg_ids = [r.diagnostic_package_id for r in rows]
        unmapped_ids = await self._repository.packages_with_unmapped_tests(db, package_ids=pkg_ids)
        group_ids = [int(r.package_group_id) for r in rows if r.package_group_id is not None]
        members_by_group = await self._repository.list_packages_by_group_ids(db, group_ids=group_ids)
        group_items: list[DiagnosticPackageGroupListItem] = []
        for row, item in zip(rows, base_items, strict=True):
            members = (
                members_by_group.get(int(row.package_group_id), [])
                if row.package_group_id is not None
                else [row]
            )
            pkg_id = int(row.diagnostic_package_id)
            has_unmapped = pkg_id in unmapped_ids
            group_items.append(
                DiagnosticPackageGroupListItem(
                    **item.model_dump(),
                    package_group=self._package_group_map(members),
                    has_unmapped_tests=has_unmapped,
                    warnings=self._build_package_warnings(
                        has_unmapped=has_unmapped,
                        has_null_provider_test_groups=False,
                    ),
                )
            )
        return group_items

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
        page: int | None = None,
        limit: int | None = None,
        search: str | None = None,
        diagnostic_provider: str | None = None,
        status: str | None = None,
    ) -> tuple[list[DiagnosticPackageListItem], int | None, dict | None]:
        gender_value = self._normalize_lower(gender)
        tag_value = self._normalize(tag)
        chip_key_value = self._normalize(filter_chip)
        search_value = sanitize_list_search(search)
        provider_value = self._normalize(diagnostic_provider)
        status_value = self._normalize_lower(status)

        if gender_value is not None and gender_value not in _ALLOWED_GENDER_VALUES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        if list_type not in ("public_package", "custom_package"):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        public_only = list_type == "public_package"
        created_by_user_id = None if public_only else requesting_user_id

        resolved_package_for = package_for
        if resolved_package_for is None and public_only and active_only:
            resolved_package_for = "public"

        list_kwargs = {
            "gender": gender_value,
            "tag": tag_value,
            "filter_chip": chip_key_value,
            "active_only": active_only,
            "public_only": public_only,
            "created_by_user_id": created_by_user_id,
            "package_for": resolved_package_for,
            "search": search_value,
            "diagnostic_provider": provider_value,
            "status": status_value,
        }

        paginate = page is not None and limit is not None
        extra_meta: dict | None = None
        total: int | None = None
        if paginate:
            total = await self._repository.count_packages(db, **list_kwargs)
            tag_names = await self._repository.list_distinct_package_tag_names(db, **list_kwargs)
            extra_meta = {"filter_options": {"tags": tag_names}}

        rows = await self._repository.get_all_packages(
            db,
            **list_kwargs,
            page=page if paginate else None,
            limit=limit if paginate else None,
        )
        items = await self._package_rows_to_list_items(db, rows)
        if paginate:
            return items, total, extra_meta
        return items, None, None

    async def get_package_groups(
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
        page: int | None = None,
        limit: int | None = None,
        search: str | None = None,
        diagnostic_provider: str | None = None,
        status: str | None = None,
    ) -> tuple[list[DiagnosticPackageGroupListItem], int | None, dict | None]:
        gender_value = self._normalize_lower(gender)
        tag_value = self._normalize(tag)
        chip_key_value = self._normalize(filter_chip)
        search_value = sanitize_list_search(search)
        provider_value = self._normalize(diagnostic_provider)
        status_value = self._normalize_lower(status)

        if gender_value is not None and gender_value not in _ALLOWED_GENDER_VALUES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        if list_type not in ("public_package", "custom_package"):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        public_only = list_type == "public_package"
        created_by_user_id = None if public_only else requesting_user_id

        resolved_package_for = package_for
        if resolved_package_for is None and public_only and active_only:
            resolved_package_for = "public"

        list_kwargs = {
            "gender": gender_value,
            "tag": tag_value,
            "filter_chip": chip_key_value,
            "active_only": active_only,
            "public_only": public_only,
            "created_by_user_id": created_by_user_id,
            "package_for": resolved_package_for,
            "search": search_value,
            "diagnostic_provider": provider_value,
            "status": status_value,
            "default_for_group_only": True,
        }

        paginate = page is not None and limit is not None
        extra_meta: dict | None = None
        total: int | None = None
        if paginate:
            total = await self._repository.count_packages(db, **list_kwargs)
            tag_names = await self._repository.list_distinct_package_tag_names(db, **list_kwargs)
            extra_meta = {"filter_options": {"tags": tag_names}}

        rows = await self._repository.get_all_packages(
            db,
            **list_kwargs,
            page=page if paginate else None,
            limit=limit if paginate else None,
        )
        items = await self._package_rows_to_group_list_items(db, rows)
        if paginate:
            return items, total, extra_meta
        return items, None, None

    async def diagnostic_packages_for_form_bootstrap(
        self,
        db,
        items: list[DiagnosticPackageListItem],
    ) -> list[dict]:
        """Minimal picker shape for engagement form bootstrap (includes lab code)."""
        if not items:
            return []
        ids = [int(item.diagnostic_package_id) for item in items]
        codes = await self._repository.external_package_codes_for_ids(db, package_ids=ids)
        return [
            {
                "diagnostic_package_id": item.diagnostic_package_id,
                "package_name": item.package_name,
                "gender_suitability": item.gender_suitability,
                "diagnostic_provider": item.diagnostic_provider,
                "external_package_code": codes.get(int(item.diagnostic_package_id)),
                "complementary_consultation": item.complementary_consultation,
            }
            for item in items
        ]

    async def get_package_detail(self, db, *, package_id: int) -> DiagnosticPackageDetailResponse:
        row = await self._repository.get_package_by_id(db, package_id=package_id)
        if row is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        counts = await self._repository.count_distinct_tests_for_packages(
            db, package_ids=[row.diagnostic_package_id]
        )
        screening = await self._repository.screening_flags_for_packages(
            db, package_ids=[row.diagnostic_package_id]
        )
        n_tests = counts.get(row.diagnostic_package_id, 0)
        allergy, cancer = screening.get(row.diagnostic_package_id, (False, False))
        same_packages: list[DiagnosticPackagePeerResponse] = []
        if row.package_group_id is not None:
            grouped = await self._repository.list_packages_by_group_ids(
                db, group_ids=[int(row.package_group_id)]
            )
            same_packages = self._peer_responses(row, grouped.get(int(row.package_group_id), []))
        package = self._to_package_response(
            row,
            no_of_tests=n_tests,
            same_packages=same_packages,
            has_allergy_screening=allergy,
            has_cancer_screening=cancer,
        )
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

    async def _fetch_healthians_product(
        self,
        *,
        deal_type: str,
        external_code: str,
        log_context: str,
    ) -> dict:
        try:
            token = await healthians_client.get_access_token()
            if deal_type == "profile":
                deal_type_id = healthians_profile_deal_type_id(external_code)
            else:
                deal_type_id = healthians_product_deal_type_id(external_code)
            return await healthians_client.get_product_details(token, deal_type_id, deal_type=deal_type)
        except Exception as exc:
            logger.exception("Healthians API error during %s", log_context)
            is_blocked = "403" in str(exc) or "blocked" in str(exc).lower()
            raise AppError(
                status_code=502,
                error_code="HEALTHIANS_IP_BLOCKED" if is_blocked else "HEALTHIANS_API_ERROR",
                message=(
                    "Healthians is blocking requests from this server. "
                    "The server IP needs to be whitelisted by Healthians."
                    if is_blocked
                    else f"Failed to fetch data from Healthians: {exc}"
                ),
            ) from exc

    async def _build_provider_test_diff(
        self,
        db,
        *,
        diagnostic_provider: str | None,
        external_code: str,
        product: dict,
        tests_by_id: dict[int, tuple[HealthParameter, list[str]]],
    ) -> ProviderTestDiffResponse:
        constituent_by_id: dict[str, str] = {}
        for raw in product.get("constituents", []):
            cid = str(raw.get("id", "")).strip()
            if cid:
                constituent_by_id[cid] = str(raw.get("name", "")).strip()

        matched: list[ProviderTestDiffMatchedItem] = []
        only_in_package: list[ProviderTestDiffOnlyInPackageItem] = []
        not_linked: list[ProviderTestDiffNotLinkedItem] = []
        package_provider_ids: set[str] = set()

        for test_id in sorted(tests_by_id.keys(), key=lambda tid: tests_by_id[tid][0].test_name or ""):
            test, group_names = tests_by_id[test_id]
            sorted_groups = sorted(group_names)
            mapped_key = provider_parameter_key(test, diagnostic_provider)
            if mapped_key is None:
                not_linked.append(
                    ProviderTestDiffNotLinkedItem(
                        test_id=test_id,
                        test_name=test.test_name,
                        group_names=sorted_groups,
                    )
                )
                continue

            package_provider_ids.add(mapped_key)
            if mapped_key in constituent_by_id:
                matched.append(
                    ProviderTestDiffMatchedItem(
                        test_id=test_id,
                        test_name=test.test_name,
                        group_names=sorted_groups,
                        provider_id=mapped_key,
                        provider_name=constituent_by_id[mapped_key],
                    )
                )
            else:
                only_in_package.append(
                    ProviderTestDiffOnlyInPackageItem(
                        test_id=test_id,
                        test_name=test.test_name,
                        group_names=sorted_groups,
                        provider_id=mapped_key,
                    )
                )

        shared_ids = {pid for pid, count in Counter(item.provider_id for item in matched).items() if count > 1}
        if shared_ids:
            matched = [
                item.model_copy(update={"shared_provider_id": item.provider_id in shared_ids}) for item in matched
            ]

        only_at_provider_ids = sorted(
            set(constituent_by_id.keys()) - package_provider_ids,
            key=lambda pid: constituent_by_id.get(pid, pid).lower(),
        )
        catalog_by_key = await self._repository.map_parameters_by_healthians_keys(db, keys=only_at_provider_ids)
        only_at_provider: list[ProviderTestDiffOnlyAtProviderItem] = []
        for provider_id in only_at_provider_ids:
            catalog_param = catalog_by_key.get(provider_id)
            only_at_provider.append(
                ProviderTestDiffOnlyAtProviderItem(
                    provider_id=provider_id,
                    provider_name=constituent_by_id[provider_id],
                    catalog_test_id=catalog_param.test_id if catalog_param else None,
                    catalog_test_name=catalog_param.test_name if catalog_param else None,
                )
            )

        counts = ProviderTestDiffCounts(
            matched=len(matched),
            only_in_package=len(only_in_package),
            only_at_provider=len(only_at_provider),
            not_linked=len(not_linked),
        )
        in_sync = counts.only_in_package == 0 and counts.only_at_provider == 0 and counts.not_linked == 0
        return ProviderTestDiffResponse(
            status="in_sync" if in_sync else "out_of_sync",
            healthians_package_name=product.get("name"),
            external_package_code=external_code,
            counts=counts,
            matched=matched,
            only_in_package=only_in_package,
            not_linked=not_linked,
            only_at_provider=only_at_provider,
        )

    async def get_provider_test_diff(self, db, *, package_id: int) -> ProviderTestDiffResponse:
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        if not is_healthians_diagnostic_provider(package.diagnostic_provider):
            raise AppError(
                status_code=400,
                error_code="PROVIDER_NOT_SUPPORTED",
                message="Provider test comparison is only available for Healthians packages.",
            )

        external_code = (package.external_package_code or "").strip()
        if not external_code:
            raise AppError(
                status_code=400,
                error_code="EXTERNAL_PACKAGE_CODE_REQUIRED",
                message="Set the Healthians package code on this package before comparing tests.",
            )

        product = await self._fetch_healthians_product(
            deal_type="package",
            external_code=external_code,
            log_context="provider test diff",
        )

        group_rows = await self._repository.get_package_test_groups(db, package_id=package_id)
        tests_by_id: dict[int, tuple[HealthParameter, list[str]]] = {}
        for group_row, tests in group_rows:
            for test in tests:
                if test.test_id not in tests_by_id:
                    tests_by_id[test.test_id] = (test, [])
                group_names = tests_by_id[test.test_id][1]
                if group_row.group_name not in group_names:
                    group_names.append(group_row.group_name)

        return await self._build_provider_test_diff(
            db,
            diagnostic_provider=package.diagnostic_provider,
            external_code=external_code,
            product=product,
            tests_by_id=tests_by_id,
        )

    async def get_test_group_provider_test_diff(
        self,
        db,
        *,
        group_id: int,
        preview_external_test_group_code: str | None = None,
    ) -> ProviderTestDiffResponse:
        group = await self._repository.get_group_by_id(db, group_id=group_id)
        if group is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        if not is_healthians_diagnostic_provider(group.diagnostic_provider) and not preview_external_test_group_code:
            raise AppError(
                status_code=400,
                error_code="PROVIDER_NOT_SUPPORTED",
                message="Provider test comparison is only available for Healthians test groups.",
            )

        external_code = (preview_external_test_group_code or group.external_test_group_code or "").strip()
        if not external_code:
            raise AppError(
                status_code=400,
                error_code="EXTERNAL_TEST_GROUP_CODE_REQUIRED",
                message="Set the Healthians test group code before comparing tests.",
            )

        product = await self._fetch_healthians_product(
            deal_type="profile",
            external_code=external_code,
            log_context="test group provider test diff",
        )
        tests = await self._repository.get_parameters_for_group(db, group_id=group_id)
        tests_by_id = {int(test.test_id): (test, [group.group_name]) for test in tests}
        provider = group.diagnostic_provider if not preview_external_test_group_code else "healthians"
        return await self._build_provider_test_diff(
            db,
            diagnostic_provider=provider,
            external_code=external_code,
            product=product,
            tests_by_id=tests_by_id,
        )

    async def create_package(
        self,
        db,
        *,
        employee: EmployeeContext | None,
        current_user_id: int | None,
        data: DiagnosticPackageCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> DiagnosticPackageResponse:
        is_custom = data.custom
        payload = data.model_dump(exclude_none=True)
        payload.pop("no_of_tests", None)
        payload.pop("custom", None)
        same_as_package_id = payload.pop("same_as_package_id", None)
        copy_test_groups_from_package_id = payload.pop("copy_test_groups_from_package_id", None)
        payload.pop("is_default_for_group", None)

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
        if "complementary_consultation" not in payload or payload["complementary_consultation"] is None:
            payload["complementary_consultation"] = {}

        package = DiagnosticPackage(**payload)
        package = await self._repository.create_package(db, package)

        if same_as_package_id is not None:
            package.package_group_id = await self._resolve_same_as_package_group(
                db,
                package_id=package.diagnostic_package_id,
                provider=package.diagnostic_provider,
                same_as_package_id=int(same_as_package_id),
            )
            if await self._repository.group_has_default(db, group_id=int(package.package_group_id)):
                package.is_default_for_group = False
            else:
                package.is_default_for_group = True
        else:
            group = await self._repository.create_package_group(db)
            package.package_group_id = group.package_group_id
            package.is_default_for_group = True
        db.add(package)
        await db.flush()

        if copy_test_groups_from_package_id is not None:
            if same_as_package_id is None:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="copy_test_groups_from_package_id requires same_as_package_id",
                )
            source_id = int(copy_test_groups_from_package_id)
            source = await self._repository.get_package_by_id_basic(db, package_id=source_id)
            if source is None:
                raise AppError(
                    status_code=404,
                    error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND",
                    message="Source package for test group copy does not exist",
                )
            if (
                package.package_group_id is None
                or source.package_group_id is None
                or int(source.package_group_id) != int(package.package_group_id)
            ):
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="Test groups can only be copied from a package in the same group",
                )
            await self._repository.copy_test_groups_from_package(
                db,
                source_package_id=source_id,
                target_package_id=int(package.diagnostic_package_id),
            )
            await self._require_audit_service().log_event(
                db,
                action="EMPLOYEE_COPY_PACKAGE_TEST_GROUPS",
                endpoint=endpoint,
                ip_address=ip_address,
                user_agent=user_agent,
                user_id=current_user_id,
                session_id=None,
            )

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
        current_user_id: int | None,
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

        old_group_id = existing.package_group_id
        was_default = bool(existing.is_default_for_group)
        fields_set = data.model_fields_set
        payload = data.model_dump(exclude_none=True)
        payload.pop("no_of_tests", None)

        want_set_default = False
        if "is_default_for_group" in fields_set:
            if data.is_default_for_group is False and was_default and old_group_id is not None:
                members = await self._repository.list_packages_in_group(db, group_id=int(old_group_id))
                if len(members) > 1 and not any(
                    member.diagnostic_package_id != package_id and member.is_default_for_group
                    for member in members
                ):
                    raise AppError(
                        status_code=400,
                        error_code="INVALID_INPUT",
                        message="Cannot unset the only catalog default for this package group",
                    )
            if data.is_default_for_group is True:
                want_set_default = True
            elif data.is_default_for_group is False:
                payload["is_default_for_group"] = False
            payload.pop("is_default_for_group", None)

        new_group_id: int | None = old_group_id
        group_membership_changed = False
        if "same_as_package_id" in fields_set:
            payload.pop("same_as_package_id", None)
            group_membership_changed = True
            if data.same_as_package_id is None:
                group = await self._repository.create_package_group(db)
                new_group_id = int(group.package_group_id)
                payload["package_group_id"] = new_group_id
                payload["is_default_for_group"] = True
            else:
                merged_provider = payload.get("diagnostic_provider", existing.diagnostic_provider)
                new_group_id = await self._resolve_same_as_package_group(
                    db,
                    package_id=package_id,
                    provider=merged_provider,
                    same_as_package_id=int(data.same_as_package_id),
                )
                payload["package_group_id"] = new_group_id
                payload["is_default_for_group"] = not await self._repository.group_has_default(
                    db, group_id=int(new_group_id)
                )
        if "package_name" in payload:
            name = self._normalize(payload["package_name"])
            if name is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["package_name"] = name
        self._validate_package_common_fields(payload)
        self._validate_package_for_field(payload)

        if "diagnostic_provider" in payload and "same_as_package_id" not in fields_set:
            group_id = existing.package_group_id
            if group_id is not None:
                provider_key = self._provider_key(payload.get("diagnostic_provider"))
                members = await self._repository.list_packages_in_group(db, group_id=int(group_id))
                for member in members:
                    if member.diagnostic_package_id == package_id:
                        continue
                    if provider_key is not None and self._provider_key(member.diagnostic_provider) == provider_key:
                        label = self._provider_display_label(member.diagnostic_provider)
                        raise AppError(
                            status_code=400,
                            error_code="DIAGNOSTIC_PACKAGE_PROVIDER_CONFLICT",
                            message=(
                                f"{label} is already linked to {member.package_name}. "
                                "Edit that package, or pick a different one."
                            ),
                        )

        updated = await self._repository.update_package(db, package_id=package_id, data=payload)
        if updated is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        if (
            group_membership_changed
            and old_group_id is not None
            and int(old_group_id) != int(updated.package_group_id or 0)
            and was_default
        ):
            await self._promote_default_if_needed(
                db,
                group_id=int(old_group_id),
                exclude_package_id=package_id,
            )

        if want_set_default and updated.package_group_id is not None:
            await self._repository.set_package_as_group_default(
                db,
                package_id=package_id,
                group_id=int(updated.package_group_id),
            )
            refreshed = await self._repository.get_package_by_id_basic(db, package_id=package_id)
            if refreshed is not None:
                updated = refreshed

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

    async def duplicate_package(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> DiagnosticPackageResponse:
        self._ensure_employee_access(employee)
        duplicated = await self._repository.duplicate_package(db, package_id=package_id)
        if duplicated is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DUPLICATE_DIAGNOSTIC_PACKAGE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return await self._package_response_with_test_count(db, duplicated)

    async def reorder_packages(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_ids: list[int],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)
        await self._repository.reorder_packages(db, package_ids=package_ids)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_REORDER_DIAGNOSTIC_PACKAGES",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return {"reordered": True}

    async def reorder_filter_chips(
        self,
        db,
        *,
        employee: EmployeeContext,
        filter_chip_ids: list[int],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)
        await self._repository.reorder_filter_chips(db, filter_chip_ids=filter_chip_ids)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_REORDER_DIAGNOSTIC_FILTER_CHIPS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return {"reordered": True}

    async def reorder_package_reasons(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        reason_ids: list[int],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)
        pkg = await self._repository.get_package_by_id(db, package_id=package_id)
        if pkg is None:
            raise AppError(status_code=404, error_code="PACKAGE_NOT_FOUND", message="Package does not exist")
        await self._repository.reorder_package_reasons(
            db, package_id=package_id, reason_ids=reason_ids
        )
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_REORDER_DIAGNOSTIC_PACKAGE_REASONS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return {"reordered": True}

    async def list_parameters(
        self,
        db,
        *,
        parameter_type: ParameterType | None = None,
        page: int | None = None,
        limit: int | None = None,
        search: str | None = None,
    ) -> tuple[list[HealthParameterResponse], int | None]:
        orm_type = self._orm_parameter_type(parameter_type) if parameter_type is not None else None
        search_value = sanitize_list_search(search)
        paginate = page is not None and limit is not None
        total: int | None = None
        if paginate:
            total = await self._repository.count_parameters(
                db, parameter_type=orm_type, search=search_value
            )
        rows = await self._repository.get_all_parameters(
            db,
            parameter_type=orm_type,
            search=search_value,
            page=page if paginate else None,
            limit=limit if paginate else None,
        )
        items = [self._to_health_parameter_response(row) for row in rows]
        return items, total

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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
            session_id=None,
        )
        return {"deleted": True}

    async def get_all_groups(
        self,
        db,
        *,
        filter_chip: str | None = None,
        package_for: str | None = None,
        page: int | None = None,
        limit: int | None = None,
        search: str | None = None,
    ) -> tuple[list[TestGroupResponse], int | None]:
        chip_key_value = self._normalize(filter_chip)
        search_value = sanitize_list_search(search)
        paginate = page is not None and limit is not None
        total: int | None = None
        if paginate:
            total = await self._repository.count_groups(
                db,
                filter_chip_key=chip_key_value,
                package_for=package_for,
                search=search_value,
            )
        rows = await self._repository.get_all_groups(
            db,
            filter_chip_key=chip_key_value,
            package_for=package_for,
            search=search_value,
            page=page if paginate else None,
            limit=limit if paginate else None,
        )
        items = [self._to_group_response(group, tests=[], test_count=test_count) for group, test_count in rows]
        return items, total

    async def get_test_group_profiles(
        self,
        db,
        *,
        filter_chip: str | None = None,
        package_for: str | None = None,
        page: int | None = None,
        limit: int | None = None,
        search: str | None = None,
    ) -> tuple[list[TestGroupProfileGroupListItem], int | None]:
        chip_key_value = self._normalize(filter_chip)
        search_value = sanitize_list_search(search)
        paginate = page is not None and limit is not None
        total: int | None = None
        if paginate:
            total = await self._repository.count_catalog_test_groups(
                db,
                filter_chip_key=chip_key_value,
                package_for=package_for,
                search=search_value,
            )
        rows = await self._repository.get_catalog_test_groups(
            db,
            filter_chip_key=chip_key_value,
            package_for=package_for,
            search=search_value,
            page=page if paginate else None,
            limit=limit if paginate else None,
        )
        profile_ids = [
            int(group.profile_group_id) for group, _count in rows if group.profile_group_id is not None
        ]
        members_by_profile = await self._repository.list_test_groups_by_profile_group_ids(
            db, profile_group_ids=profile_ids
        )
        items: list[TestGroupProfileGroupListItem] = []
        for group, test_count in rows:
            price = float(group.price) if group.price is not None else None
            original_price = float(group.original_price) if group.original_price is not None else None
            members = (
                members_by_profile.get(int(group.profile_group_id), [group])
                if group.profile_group_id is not None
                else [group]
            )
            items.append(
                TestGroupProfileGroupListItem(
                    group_id=group.group_id,
                    group_name=group.group_name,
                    test_count=test_count,
                    price=price,
                    discount=_discount_percent_label(price, original_price),
                    discount_percent=_discount_percent(price, original_price),
                    gender_suitability=group.gender_suitability,
                    profile_group_id=group.profile_group_id,
                    diagnostic_provider=group.diagnostic_provider,
                    profile_group=self._test_group_profile_map(members),
                )
            )
        return items, total

    async def get_group_detail(self, db, *, group_id: int) -> TestGroupResponse:
        group = await self._repository.get_group_by_id(db, group_id=group_id)
        if group is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")
        tests = await self._repository.get_parameters_for_group(db, group_id=group_id)
        chip_links = await self._repository.get_group_filter_chip_links(db, group_id=group_id)
        same_test_groups: list[TestGroupPeerResponse] = []
        if group.profile_group_id is not None:
            members = await self._repository.list_test_groups_in_profile_group(
                db, profile_group_id=int(group.profile_group_id)
            )
            same_test_groups = self._test_group_peer_responses(group, members)
        return self._to_group_response(
            group,
            tests=tests,
            filter_chip_links=chip_links,
            same_test_groups=same_test_groups,
        )

    async def list_packages_for_test_group(self, db, *, group_id: int) -> list[TestGroupPackageListItem]:
        group = await self._repository.get_group_by_id(db, group_id=group_id)
        if group is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")
        packages = await self._repository.list_packages_for_test_group(db, group_id=group_id)
        return [
            TestGroupPackageListItem(
                diagnostic_package_id=int(pkg.diagnostic_package_id),
                package_name=pkg.package_name,
                diagnostic_provider=self._provider_enum_key(pkg.diagnostic_provider),
                status=pkg.status,
            )
            for pkg in packages
        ]

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
        same_as_group_id = payload.pop("same_as_group_id", None)
        copy_tests_from_group_id = payload.pop("copy_tests_from_group_id", None)
        payload.pop("is_default_for_group", None)
        group_name = self._normalize(payload.get("group_name"))
        if group_name is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        payload["group_name"] = group_name
        group_key = self._slugify_group_key(payload.get("group_key"))
        if group_key is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        payload["group_key"] = group_key
        dup = await self._repository.get_group_by_group_key(db, group_key=group_key)
        if dup is not None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Group key already exists")
        self._validate_optional_gender_field(payload)
        self._validate_package_for_field(payload)
        payload.setdefault("package_for", "public")
        if "external_test_group_code" in payload:
            payload["external_test_group_code"] = self._normalize(payload.get("external_test_group_code"))

        try:
            created = await self._repository.create_group(db, DiagnosticTestGroup(**payload))
        except IntegrityError:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request") from None

        if same_as_group_id is not None:
            created.profile_group_id = await self._resolve_same_as_test_group_profile(
                db,
                group_id=created.group_id,
                provider=created.diagnostic_provider,
                same_as_group_id=int(same_as_group_id),
            )
            if await self._repository.profile_group_has_default(
                db, profile_group_id=int(created.profile_group_id)
            ):
                created.is_default_for_group = False
            else:
                created.is_default_for_group = True
        else:
            profile_group = await self._repository.create_profile_group(db)
            created.profile_group_id = profile_group.profile_group_id
            created.is_default_for_group = True
        db.add(created)
        await db.flush()

        if copy_tests_from_group_id is not None:
            if same_as_group_id is None:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="copy_tests_from_group_id requires same_as_group_id",
                )
            source_id = int(copy_tests_from_group_id)
            source = await self._repository.get_group_by_id(db, group_id=source_id)
            if source is None:
                raise AppError(
                    status_code=404,
                    error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND",
                    message="Source test group for copy does not exist",
                )
            if (
                created.profile_group_id is None
                or source.profile_group_id is None
                or int(source.profile_group_id) != int(created.profile_group_id)
            ):
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="Tests can only be copied from a test group in the same profile group",
                )
            await self._repository.copy_tests_and_chips_between_groups(
                db,
                source_group_id=source_id,
                target_group_id=int(created.group_id),
            )

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_DIAGNOSTIC_TEST_GROUP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
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

        old_profile_group_id = existing.profile_group_id
        was_default = bool(existing.is_default_for_group)
        fields_set = data.model_fields_set
        payload = data.model_dump(exclude_none=True)

        want_set_default = False
        if "is_default_for_group" in fields_set:
            if data.is_default_for_group is False and was_default and old_profile_group_id is not None:
                members = await self._repository.list_test_groups_in_profile_group(
                    db, profile_group_id=int(old_profile_group_id)
                )
                if len(members) > 1 and not any(
                    member.group_id != group_id and member.is_default_for_group for member in members
                ):
                    raise AppError(
                        status_code=400,
                        error_code="INVALID_INPUT",
                        message="Cannot unset the only catalog default for this profile group",
                    )
            if data.is_default_for_group is True:
                want_set_default = True
            elif data.is_default_for_group is False:
                payload["is_default_for_group"] = False
            payload.pop("is_default_for_group", None)

        new_profile_group_id: int | None = old_profile_group_id
        profile_membership_changed = False
        if "same_as_group_id" in fields_set:
            payload.pop("same_as_group_id", None)
            profile_membership_changed = True
            if data.same_as_group_id is None:
                profile_group = await self._repository.create_profile_group(db)
                new_profile_group_id = int(profile_group.profile_group_id)
                payload["profile_group_id"] = new_profile_group_id
                payload["is_default_for_group"] = True
            else:
                merged_provider = payload.get("diagnostic_provider", existing.diagnostic_provider)
                new_profile_group_id = await self._resolve_same_as_test_group_profile(
                    db,
                    group_id=group_id,
                    provider=merged_provider,
                    same_as_group_id=int(data.same_as_group_id),
                )
                payload["profile_group_id"] = new_profile_group_id
                payload["is_default_for_group"] = not await self._repository.profile_group_has_default(
                    db, profile_group_id=int(new_profile_group_id)
                )

        if "group_name" in payload:
            group_name = self._normalize(payload.get("group_name"))
            if group_name is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["group_name"] = group_name
        if "group_key" in payload:
            group_key = self._slugify_group_key(payload.get("group_key"))
            if group_key is None:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            payload["group_key"] = group_key
            other = await self._repository.get_group_by_group_key(db, group_key=group_key)
            if other is not None and other.group_id != group_id:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Group key already exists")
        self._validate_optional_gender_field(payload)
        self._validate_package_for_field(payload)
        if "external_test_group_code" in fields_set:
            payload["external_test_group_code"] = self._normalize(data.external_test_group_code)

        if "diagnostic_provider" in payload and "same_as_group_id" not in fields_set:
            incoming_provider = payload.get("diagnostic_provider")
            incoming_key = self._provider_key(incoming_provider)
            existing_key = self._provider_key(existing.diagnostic_provider)
            if incoming_key is not None and incoming_key != existing_key:
                await self._ensure_test_group_provider_matches_assigned_packages(
                    db,
                    group_id=group_id,
                    diagnostic_provider=incoming_provider,
                )
            if existing.profile_group_id is not None:
                provider_key = self._provider_key(payload.get("diagnostic_provider"))
                members = await self._repository.list_test_groups_in_profile_group(
                    db, profile_group_id=int(existing.profile_group_id)
                )
                for member in members:
                    if member.group_id == group_id:
                        continue
                    if provider_key is not None and self._provider_key(member.diagnostic_provider) == provider_key:
                        label = self._provider_display_label(member.diagnostic_provider)
                        raise AppError(
                            status_code=400,
                            error_code="DIAGNOSTIC_TEST_GROUP_PROVIDER_CONFLICT",
                            message=(
                                f"{label} is already linked to {member.group_name}. "
                                "Edit that test group, or pick a different one."
                            ),
                        )

        try:
            updated = await self._repository.update_group(db, group_id=group_id, data=payload)
        except IntegrityError:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request") from None
        if updated is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        if (
            profile_membership_changed
            and old_profile_group_id is not None
            and int(old_profile_group_id) != int(updated.profile_group_id or 0)
            and was_default
        ):
            await self._promote_test_group_default_if_needed(
                db,
                profile_group_id=int(old_profile_group_id),
                exclude_group_id=group_id,
            )

        if want_set_default and updated.profile_group_id is not None:
            await self._repository.set_test_group_as_profile_default(
                db,
                group_id=group_id,
                profile_group_id=int(updated.profile_group_id),
            )
            refreshed = await self._repository.get_group_by_id(db, group_id=group_id)
            if refreshed is not None:
                updated = refreshed

        tests = await self._repository.get_parameters_for_group(db, group_id=group_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_DIAGNOSTIC_TEST_GROUP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        same_test_groups: list[TestGroupPeerResponse] = []
        if updated.profile_group_id is not None:
            members = await self._repository.list_test_groups_in_profile_group(
                db, profile_group_id=int(updated.profile_group_id)
            )
            same_test_groups = self._test_group_peer_responses(updated, members)
        return self._to_group_response(updated, tests=tests, same_test_groups=same_test_groups)

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
        if existing.is_default_for_group and existing.profile_group_id is not None:
            await self._repository.promote_min_price_default_in_profile_group(
                db,
                profile_group_id=int(existing.profile_group_id),
                exclude_group_id=group_id,
            )
        await self._repository.delete_group(db, group_id=group_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_TEST_GROUP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
            session_id=None,
        )
        return {"group_id": group_id, "test_ids": ordered_ids}

    async def assign_groups_to_package(
        self,
        db,
        *,
        employee: EmployeeContext | None,
        current_user_id: int | None,
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
        mismatch_ids: list[int] = []
        package_provider_key = self._provider_key(package.diagnostic_provider)
        for group_id in data.group_ids:
            row = await self._repository.get_group_by_id(db, group_id=group_id)
            if row is None:
                invalid_ids.append(group_id)
                continue
            group_provider_key = self._provider_key(row.diagnostic_provider)
            if (
                package_provider_key is not None
                and group_provider_key is not None
                and package_provider_key != group_provider_key
            ):
                mismatch_ids.append(group_id)
        if invalid_ids:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"The following group_ids do not exist: {invalid_ids}",
            )
        if mismatch_ids:
            raise AppError(
                status_code=400,
                error_code="DIAGNOSTIC_TEST_GROUP_PROVIDER_MISMATCH",
                message=f"Test group provider does not match package provider for group_ids: {mismatch_ids}",
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

    async def fork_test_group_for_package(
        self,
        db,
        *,
        employee: EmployeeContext,
        package_id: int,
        group_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> ForkTestGroupForPackageResponse:
        self._ensure_employee_access(employee)
        package = await self._repository.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_PACKAGE_NOT_FOUND", message="Package does not exist")

        group = await self._repository.get_group_by_id(db, group_id=group_id)
        if group is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        if not await self._repository.package_has_group_assignment(
            db, package_id=package_id, group_id=group_id
        ):
            raise AppError(
                status_code=404,
                error_code="DIAGNOSTIC_TEST_GROUP_NOT_ASSIGNED_TO_PACKAGE",
                message="This test group is not assigned to the package",
            )

        if group.forked_from_group_id is not None:
            return ForkTestGroupForPackageResponse(group_id=group.group_id, forked=False)

        existing_fork = await self._repository.get_package_assigned_fork(
            db, package_id=package_id, catalog_group_id=group_id
        )
        if existing_fork is not None:
            return ForkTestGroupForPackageResponse(group_id=existing_fork.group_id, forked=False)

        catalog_group_id = group.group_id
        copy = await self._repository.duplicate_test_group_for_package(
            db,
            source_group_id=catalog_group_id,
            forked_from_group_id=catalog_group_id,
            package_id=package_id,
        )
        if copy is None:
            raise AppError(status_code=404, error_code="DIAGNOSTIC_TEST_GROUP_NOT_FOUND", message="Group does not exist")

        replaced = await self._repository.replace_package_group_assignment(
            db,
            package_id=package_id,
            old_group_id=catalog_group_id,
            new_group_id=copy.group_id,
        )
        if not replaced:
            raise AppError(
                status_code=404,
                error_code="DIAGNOSTIC_TEST_GROUP_NOT_ASSIGNED_TO_PACKAGE",
                message="This test group is not assigned to the package",
            )

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_FORK_DIAGNOSTIC_TEST_GROUP_FOR_PACKAGE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return ForkTestGroupForPackageResponse(group_id=copy.group_id, forked=True)

    async def remove_group_from_package(
        self,
        db,
        *,
        employee: EmployeeContext | None,
        current_user_id: int | None,
        package_id: int,
        group_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        if employee is not None:
            self._ensure_employee_access(employee)
            audit_user_id = None
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
            user_id=None,
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
            user_id=None,
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
        if existing.is_default_for_group and existing.package_group_id is not None:
            await self._repository.promote_min_price_default_in_group(
                db,
                group_id=int(existing.package_group_id),
                exclude_package_id=package_id,
            )
        await self._repository.delete_package(db, package_id=package_id)
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_DIAGNOSTIC_PACKAGE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

    async def get_filter_chips(
        self,
        db,
        *,
        chip_for: str | None = None,
        active_only: bool = True,
        page: int | None = None,
        limit: int | None = None,
        search: str | None = None,
    ) -> tuple[list[FilterChipResponse], int | None]:
        scope = self._normalize_lower(chip_for) if chip_for is not None else "public_package"
        if scope is None:
            scope = "public_package"
        if scope not in _ALLOWED_CHIP_FOR:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        search_value = sanitize_list_search(search)
        paginate = page is not None and limit is not None
        total: int | None = None
        if paginate:
            total = await self._repository.count_filter_chips(
                db, chip_for=scope, active_only=active_only, search=search_value
            )
        rows = await self._repository.get_all_filter_chips(
            db,
            chip_for=scope,
            active_only=active_only,
            search=search_value,
            page=page if paginate else None,
            limit=limit if paginate else None,
        )
        items = [
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
        return items, total

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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
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
            user_id=None,
            session_id=None,
        )
