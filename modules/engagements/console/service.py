"""Engagement console service."""

from __future__ import annotations

import logging
import re
from datetime import date, time
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from common.masking import mask_email, mask_phone
from core.config import settings
from core.exceptions import AppError
from modules.diagnostics.healthians import client as healthians_client
from modules.diagnostics.healthians.sync_log import finalize_healthians_sync_log, log_healthians_call
from modules.diagnostics.models import DiagnosticPackage
from modules.employee.access_control import (
    ensure_console_access,
    ensure_employee_present,
    ensure_engagement_running,
    ensure_participant_department_access,
    resolve_org_manager_scope_for_organization,
)
from modules.employee.models import EmployeeRole
from modules.employee.service import EmployeeContext
from modules.organizations.models import Organization
from modules.assessments.package_questions_service import AssessmentPackageCategoriesService
from modules.assessments.repository import AssessmentsRepository
from modules.engagements.models import Engagement, EngagementParticipant
from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import _participant_enrollment_to_dict
from modules.geocoding.client import search_places
from modules.metsights.sync_service import MetsightsSyncService
from modules.questionnaire.service import QuestionnaireService
from modules.users.models import User
from modules.users.repository import UsersRepository

logger = logging.getLogger(__name__)

_CONSOLE_PARTICIPANT_FIELDS = (
    "engagement_participant_id",
    "engagement_id",
    "user_id",
    "first_name",
    "last_name",
    "phone",
    "email",
    "age",
    "address",
    "pin_code",
    "city",
    "slot_start_time",
    "engagement_date",
    "participants_employee_id",
    "participant_department",
    "participant_blood_group",
    "consultations",
    "barcode",
    "booking_id",
)


def _console_participant_to_dict(row: tuple) -> dict[str, Any]:
    """Slim + PII-masked participant payload for console list only."""
    full = _participant_enrollment_to_dict(row)
    slim = {key: full.get(key) for key in _CONSOLE_PARTICIPANT_FIELDS}
    slim["phone"] = mask_phone(slim.get("phone"))
    slim["email"] = mask_email(slim.get("email"))
    return slim


def _to_healthians_gender(raw: str | None) -> str | None:
    v = (raw or "").strip()
    if not v:
        return None
    if v in {"1", "M", "m"}:
        return "M"
    if v in {"2", "F", "f"}:
        return "F"
    lowered = v.lower()
    if lowered.startswith("m"):
        return "M"
    if lowered.startswith("f"):
        return "F"
    return None


def _format_healthians_dob(user: User) -> str | None:
    if user.date_of_birth is not None:
        return user.date_of_birth.strftime("%d/%m/%Y")
    return None


def _assessment_instance_to_dict(instance, package) -> dict:
    return {
        "assessment_instance_id": instance.assessment_instance_id,
        "package_id": instance.package_id,
        "package_code": getattr(package, "package_code", None) if package is not None else None,
        "package_display_name": getattr(package, "display_name", None) if package is not None else None,
        "assessment_type_code": getattr(package, "assessment_type_code", None) if package is not None else None,
        "engagement_id": instance.engagement_id,
        "status": instance.status,
        "metsights_record_id": instance.metsights_record_id,
        "assigned_at": instance.assigned_at,
        "completed_at": instance.completed_at,
    }


class ConsoleService:
    def __init__(
        self,
        repository: EngagementsRepository,
        users_repository: UsersRepository | None = None,
        assessments_repository: AssessmentsRepository | None = None,
        categories_service: AssessmentPackageCategoriesService | None = None,
        questionnaire_service: QuestionnaireService | None = None,
        metsights_sync_service: MetsightsSyncService | None = None,
    ):
        self._repository = repository
        self._users_repository = users_repository or UsersRepository()
        self._assessments_repository = assessments_repository or AssessmentsRepository()
        self._categories_service = categories_service
        self._questionnaire_service = questionnaire_service
        self._metsights_sync_service = metsights_sync_service

    async def _resolve_console_participant_department_slugs(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement: Engagement,
    ) -> set[str] | None:
        if employee.role != EmployeeRole.organization_manager or engagement.organization_id is None:
            return None
        organization = await db.get(Organization, engagement.organization_id)
        if organization is None:
            return set()
        scope = resolve_org_manager_scope_for_organization(organization, employee.user_id)
        if scope is None:
            return set()
        return scope.participant_department_slugs_for_city(engagement.city)

    async def _ensure_console_participant_access(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
    ) -> EngagementParticipant:
        await ensure_console_access(db, employee, engagement_id, repository=self._repository)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        participant = await self._repository.get_participant_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        if participant is None:
            raise AppError(
                status_code=404,
                error_code="PARTICIPANT_NOT_FOUND",
                message="Participant is not enrolled in this engagement",
            )

        if employee.role == EmployeeRole.organization_manager and engagement.organization_id is not None:
            organization = await db.get(Organization, engagement.organization_id)
            if organization is not None:
                scope = resolve_org_manager_scope_for_organization(organization, employee.user_id)
                if scope is not None:
                    ensure_participant_department_access(
                        scope,
                        engagement_city=engagement.city,
                        participant_department=participant.participant_department,
                    )
        return participant

    async def _ensure_console_participant_instance(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
        assessment_instance_id: int,
    ):
        row = await self._assessments_repository.get_instance_for_user(
            db,
            assessment_instance_id=assessment_instance_id,
            user_id=user_id,
        )
        if row is None:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_NOT_FOUND",
                message="Assessment does not exist",
            )
        instance, _package = row
        if int(instance.engagement_id or 0) != int(engagement_id):
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_NOT_FOUND",
                message="Assessment does not exist",
            )
        return row

    async def _ensure_console_write_access(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> Engagement:
        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )
        ensure_engagement_running(engagement)
        return engagement

    def _require_categories_service(self) -> AssessmentPackageCategoriesService:
        if self._categories_service is None:
            raise AppError(
                status_code=500,
                error_code="CONFIG_ERROR",
                message="Assessment categories service is not configured",
            )
        return self._categories_service

    def _require_questionnaire_service(self) -> QuestionnaireService:
        if self._questionnaire_service is None:
            raise AppError(
                status_code=500,
                error_code="CONFIG_ERROR",
                message="Questionnaire service is not configured",
            )
        return self._questionnaire_service

    def _require_metsights_sync_service(self) -> MetsightsSyncService:
        if self._metsights_sync_service is None:
            raise AppError(
                status_code=500,
                error_code="CONFIG_ERROR",
                message="Metsights sync service is not configured",
            )
        return self._metsights_sync_service

    @staticmethod
    def _engagement_to_console_dict(engagement: Engagement, *, participant_count: int) -> dict:
        return {
            "engagement_id": engagement.engagement_id,
            "engagement_name": engagement.engagement_name,
            "engagement_code": engagement.engagement_code,
            "start_date": engagement.start_date,
            "end_date": engagement.end_date,
            "status": engagement.status,
            "participant_count": participant_count,
            "blood_collection_type": engagement.blood_collection_type.value if engagement.blood_collection_type else None,
            "consultation_mode": engagement.consultation_mode.value if engagement.consultation_mode else None,
        }

    async def list_console_engagements(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
    ) -> list[dict]:
        ensure_employee_present(employee)
        if employee.role == EmployeeRole.admin:
            engagements = await self._repository.list_running_engagements(db)
        elif employee.role == EmployeeRole.onboarding_assistant:
            engagements = await self._repository.list_running_engagements_for_assigned_employee(
                db, employee_id=employee.employee_id
            )
        elif employee.role == EmployeeRole.organization_manager:
            assigned = await self._repository.list_engagements_for_assigned_org_contact_person(
                db,
                employee_id=employee.employee_id,
                user_id=employee.user_id,
            )
            engagements = []
            for engagement in assigned:
                if engagement.organization_id is None:
                    continue
                organization = await db.get(Organization, engagement.organization_id)
                if organization is None:
                    continue
                scope = resolve_org_manager_scope_for_organization(organization, employee.user_id)
                if scope is not None and scope.can_access_engagement_city(engagement.city):
                    engagements.append(engagement)
        else:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )
        engagement_ids = [int(e.engagement_id) for e in engagements]
        counts_by_id = await self._repository.count_distinct_participants_by_engagement_ids(
            db,
            engagement_ids=engagement_ids,
        )
        return [
            self._engagement_to_console_dict(
                e,
                participant_count=counts_by_id.get(int(e.engagement_id), 0),
            )
            for e in engagements
        ]

    async def get_engagement_for_console(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
    ) -> dict:
        await ensure_console_access(db, employee, engagement_id, repository=self._repository)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        department_slugs = await self._resolve_console_participant_department_slugs(
            db,
            employee=employee,
            engagement=engagement,
        )
        participant_count = await self._repository.count_distinct_participants_for_engagement(
            db,
            engagement_id=engagement_id,
            participant_department_slugs=department_slugs,
        )
        return self._engagement_to_console_dict(engagement, participant_count=participant_count)

    async def list_participants_for_console(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        page: int,
        limit: int,
    ) -> tuple[list[dict], int]:
        await ensure_console_access(db, employee, engagement_id, repository=self._repository)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        department_slugs = await self._resolve_console_participant_department_slugs(
            db,
            employee=employee,
            engagement=engagement,
        )

        participants = await self._repository.list_participants_by_engagement_id(
            db,
            engagement_id=engagement_id,
            page=page,
            limit=limit,
            participant_department_slugs=department_slugs,
        )
        total = await self._repository.count_distinct_participants_for_engagement(
            db,
            engagement_id=engagement_id,
            participant_department_slugs=department_slugs,
        )

        result = [_console_participant_to_dict(row) for row in participants]
        return result, total

    async def book_participant(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        barcode: str,
    ) -> dict:
        await ensure_console_access(db, employee, engagement_id, repository=self._repository)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )
        ensure_engagement_running(engagement)

        participant = await self._repository.get_participant_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        if participant is None:
            raise AppError(
                status_code=404,
                error_code="PARTICIPANT_NOT_FOUND",
                message="Participant is not enrolled in this engagement",
            )

        if employee.role == EmployeeRole.organization_manager and engagement.organization_id is not None:
            organization = await db.get(Organization, engagement.organization_id)
            if organization is not None:
                scope = resolve_org_manager_scope_for_organization(organization, employee.user_id)
                if scope is not None:
                    ensure_participant_department_access(
                        scope,
                        engagement_city=engagement.city,
                        participant_department=participant.participant_department,
                    )

        if participant.booking_id:
            raise AppError(
                status_code=409,
                error_code="BOOKING_ALREADY_EXISTS",
                message="A Healthians booking already exists for this participant",
            )

        if engagement.diagnostic_package_id is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Engagement has no diagnostic package configured",
            )

        diagnostic_package = (
            await db.execute(
                select(DiagnosticPackage).where(
                    DiagnosticPackage.diagnostic_package_id == engagement.diagnostic_package_id
                )
            )
        ).scalar_one_or_none()
        if diagnostic_package is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Diagnostic package does not exist",
            )

        provider = (diagnostic_package.diagnostic_provider or "").strip()
        if provider.lower() != "healthians":
            raise AppError(
                status_code=422,
                error_code="INVALID_DIAGNOSTIC_PROVIDER",
                message="Engagement diagnostic provider must be Healthians",
            )

        if engagement.external_camp_id is None or diagnostic_package.external_package_id is None:
            raise AppError(
                status_code=422,
                error_code="MISSING_DIAGNOSTIC_CONFIG",
                message="Engagement is missing external camp ID or diagnostic package is missing external package ID",
            )

        if (
            engagement.latitude is None
            or engagement.longitude is None
            or not (engagement.pincode or "").strip()
        ):
            raise AppError(
                status_code=422,
                error_code="MISSING_LOCATION",
                message="Engagement is missing latitude, longitude, or pincode",
            )

        if not settings.HEALTHIANS_CHECKSUM_KEY:
            raise AppError(
                status_code=500,
                error_code="CONFIG_ERROR",
                message="Healthians checksum key is not configured",
            )

        user = await self._users_repository.get_user_by_id(db, user_id=user_id)
        if user is None:
            raise AppError(
                status_code=404,
                error_code="USER_NOT_FOUND",
                message="User does not exist",
            )

        first_name = (user.first_name or "").strip()
        last_name = (user.last_name or "").strip()
        phone = (user.phone or "").strip()
        gender = _to_healthians_gender(user.gender)
        dob = _format_healthians_dob(user)
        if not first_name or not last_name or not phone or gender is None or (user.age is None and dob is None):
            raise AppError(
                status_code=422,
                error_code="INCOMPLETE_PARTICIPANT_PROFILE",
                message="Participant profile is missing required fields (name, phone, gender, age or date of birth)",
            )

        customer_name = f"{first_name} {last_name}".strip().upper()
        lat = str(engagement.latitude)
        long = str(engagement.longitude)
        zipcode = str(engagement.pincode).strip()
        relation = (user.relationship or "self").strip() or "self"
        vendor_billing_user_id = str(participant.booked_by_user_id)
        provider_label = (provider or "healthians").lower()

        access_token = await healthians_client.get_access_token()

        serviceability_url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/checkServiceabilityByLocation_v2"
        serviceability_payload = {
            "lat": lat,
            "long": long,
            "zipcode": zipcode,
            "is_ppmc_booking": 0,
        }
        serviceability_log = await log_healthians_call(
            db,
            engagement_id=engagement_id,
            user_id=user_id,
            provider=provider_label,
            api_url=serviceability_url,
            request_payload=serviceability_payload,
            status="pending",
        )

        try:
            serviceability_response = await healthians_client.check_serviceability_by_location_v2(
                access_token,
                lat=lat,
                long=long,
                zipcode=zipcode,
                is_ppmc_booking=0,
            )
            await finalize_healthians_sync_log(
                db,
                sync_log_id=serviceability_log.sync_log_id,
                status="success" if serviceability_response.get("status") else "failed",
                response_payload=serviceability_response,
            )
        except Exception as exc:
            await finalize_healthians_sync_log(
                db,
                sync_log_id=serviceability_log.sync_log_id,
                status="failed",
                error_message=str(exc),
            )
            raise AppError(
                status_code=502,
                error_code="HEALTHIANS_SERVICEABILITY_FAILED",
                message=str(exc),
            ) from exc

        if not serviceability_response.get("status"):
            message = serviceability_response.get("message") or "Location is not serviceable"
            raise AppError(
                status_code=422,
                error_code="LOCATION_NOT_SERVICEABLE",
                message=message,
            )

        zone_data = serviceability_response.get("data") or {}
        zone_id_raw = zone_data.get("zone_id")
        if zone_id_raw is None:
            raise AppError(
                status_code=422,
                error_code="LOCATION_NOT_SERVICEABLE",
                message="Serviceability response did not include zone_id",
            )
        zone_id = int(zone_id_raw)

        booking_payload = {
            "customer": [
                {
                    "customer_id": str(user_id),
                    "customer_name": customer_name,
                    "relation": relation,
                    "age": user.age,
                    "dob": dob or "",
                    "gender": gender,
                    "contact_number": phone,
                    "email": (user.email or "").strip(),
                    "barcode": barcode.strip(),
                }
            ],
            "camp_id": engagement.external_camp_id,
            "slot": {"slot_id": ""},
            "sample_collected": "y",
            "package": [{"deal_id": [f"package_{diagnostic_package.external_package_id}"]}],
            "customer_calling_number": phone,
            "billing_cust_name": customer_name,
            "gender": gender,
            "mobile": phone,
            "email": (user.email or "").strip(),
            "sub_locality": (engagement.sub_locality or "").strip(),
            "latitude": lat,
            "longitude": long,
            "address": (engagement.address or "").strip(),
            "zipcode": zipcode,
            "hard_copy": 0,
            "vendor_billing_user_id": vendor_billing_user_id,
            "payment_option": "prepaid",
            "discounted_price": 0,
            "zone_id": zone_id,
        }

        booking_url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/createBooking_v3"
        booking_log = await log_healthians_call(
            db,
            engagement_id=engagement_id,
            user_id=user_id,
            provider=provider_label,
            api_url=booking_url,
            request_payload=booking_payload,
            status="pending",
        )

        try:
            booking_response = await healthians_client.create_booking_v3(
                access_token,
                booking_payload,
                checksum_key=settings.HEALTHIANS_CHECKSUM_KEY,
            )
            if not booking_response.get("status"):
                await finalize_healthians_sync_log(
                    db,
                    sync_log_id=booking_log.sync_log_id,
                    status="failed",
                    response_payload=booking_response,
                    error_message=booking_response.get("message"),
                )
                raise AppError(
                    status_code=422,
                    error_code="HEALTHIANS_BOOKING_FAILED",
                    message=booking_response.get("message") or "Healthians booking failed",
                )

            healthians_booking_id = booking_response.get("booking_id")
            if not healthians_booking_id:
                await finalize_healthians_sync_log(
                    db,
                    sync_log_id=booking_log.sync_log_id,
                    status="failed",
                    response_payload=booking_response,
                    error_message="Missing booking_id in Healthians response",
                )
                raise AppError(
                    status_code=502,
                    error_code="HEALTHIANS_BOOKING_FAILED",
                    message="Healthians response did not include booking_id",
                )

            await finalize_healthians_sync_log(
                db,
                sync_log_id=booking_log.sync_log_id,
                status="success",
                response_payload=booking_response,
            )
        except AppError:
            raise
        except Exception as exc:
            await finalize_healthians_sync_log(
                db,
                sync_log_id=booking_log.sync_log_id,
                status="failed",
                error_message=str(exc),
            )
            raise AppError(
                status_code=502,
                error_code="HEALTHIANS_BOOKING_FAILED",
                message=str(exc),
            ) from exc

        await self._repository.update_participant_healthians_booking(
            db,
            engagement_participant_id=participant.engagement_participant_id,
            barcode=barcode.strip(),
            booking_id=str(healthians_booking_id),
        )

        return {
            "status": booking_response.get("status"),
            "message": booking_response.get("message"),
            "lead_id": booking_response.get("lead_id"),
            "booking_id": str(healthians_booking_id),
            "resCode": booking_response.get("resCode"),
            "tatDetail": booking_response.get("tatDetail"),
            "barcode": barcode.strip(),
            "engagement_participant_id": participant.engagement_participant_id,
            "user_id": user_id,
        }

    async def cancel_participant_booking(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        remarks: str,
    ) -> dict:
        from modules.bookings.service import cancel_healthians_participant_booking

        await ensure_console_access(db, employee, engagement_id, repository=self._repository)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )
        ensure_engagement_running(engagement)

        participant = await self._repository.get_participant_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        if participant is None:
            raise AppError(
                status_code=404,
                error_code="PARTICIPANT_NOT_FOUND",
                message="Participant is not enrolled in this engagement",
            )

        result = await cancel_healthians_participant_booking(
            db,
            participant=participant,
            engagement=engagement,
            remarks=remarks,
            repository=self._repository,
        )
        return result

    async def list_participant_assessments(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
    ) -> list[dict]:
        await self._ensure_console_participant_access(
            db,
            employee=employee,
            engagement_id=engagement_id,
            user_id=user_id,
        )

        instances = await self._assessments_repository.list_instances_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        data: list[dict] = []
        for instance in instances:
            package = await self._assessments_repository.get_package_by_id(
                db,
                package_id=int(instance.package_id),
            )
            data.append(_assessment_instance_to_dict(instance, package))
        return data

    async def get_participant_assessment_status(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        assessment_instance_id: int,
        category_of: str = "supershyft",
    ) -> list[dict]:
        await self._ensure_console_participant_access(
            db,
            employee=employee,
            engagement_id=engagement_id,
            user_id=user_id,
        )
        await self._ensure_console_participant_instance(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
            assessment_instance_id=assessment_instance_id,
        )

        return await self._require_categories_service().list_category_completion_for_assessment_instance(
            db,
            user_id=user_id,
            assessment_instance_id=assessment_instance_id,
            category_of=category_of,
        )

    async def get_participant_questionnaire(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        assessment_instance_id: int,
        category_id: int,
    ) -> dict:
        await self._ensure_console_participant_access(
            db,
            employee=employee,
            engagement_id=engagement_id,
            user_id=user_id,
        )
        await self._ensure_console_participant_instance(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
            assessment_instance_id=assessment_instance_id,
        )

        return await self._require_questionnaire_service().get_questionnaire_for_user(
            db,
            user_id=user_id,
            assessment_instance_id=assessment_instance_id,
            category_id=category_id,
            question_filter="all",
        )

    async def upsert_participant_questionnaire_responses(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        assessment_instance_id: int,
        category_id: int,
        responses: list[dict],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        await self._ensure_console_participant_access(
            db,
            employee=employee,
            engagement_id=engagement_id,
            user_id=user_id,
        )
        await self._ensure_console_write_access(db, engagement_id=engagement_id)
        await self._ensure_console_participant_instance(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
            assessment_instance_id=assessment_instance_id,
        )

        await self._require_questionnaire_service().upsert_responses_for_user(
            db,
            user_id=user_id,
            assessment_instance_id=assessment_instance_id,
            category_id=category_id,
            responses=responses,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

    async def submit_participant_assessment_category(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        assessment_instance_id: int,
        category: str,
        category_of: str,
    ) -> dict:
        await self._ensure_console_participant_access(
            db,
            employee=employee,
            engagement_id=engagement_id,
            user_id=user_id,
        )
        await self._ensure_console_write_access(db, engagement_id=engagement_id)
        await self._ensure_console_participant_instance(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
            assessment_instance_id=assessment_instance_id,
        )

        return await self._require_metsights_sync_service().submit_category_to_metsights(
            db,
            assessment_instance_id=assessment_instance_id,
            user_id=user_id,
            category_key=category,
            category_of=category_of,
        )

    # ── Home-collection booking flow ──────────────────────────────────────

    async def _load_healthians_package_for_engagement(
        self,
        db: AsyncSession,
        engagement: Engagement,
    ) -> DiagnosticPackage:
        if engagement.diagnostic_package_id is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Engagement has no diagnostic package configured",
            )
        pkg = (
            await db.execute(
                select(DiagnosticPackage).where(
                    DiagnosticPackage.diagnostic_package_id == engagement.diagnostic_package_id
                )
            )
        ).scalar_one_or_none()
        if pkg is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Diagnostic package does not exist",
            )
        if (pkg.diagnostic_provider or "").strip().lower() != "healthians":
            raise AppError(
                status_code=422,
                error_code="INVALID_DIAGNOSTIC_PROVIDER",
                message="Diagnostic provider is not Healthians",
            )
        return pkg

    @staticmethod
    def _parse_slot_time(slot_str: str) -> time:
        text = (slot_str or "").strip()
        if not text:
            raise ValueError("Empty slot time")
        meridiem_match = re.search(r"\b(AM|PM)\b", text, re.IGNORECASE)
        meridiem = meridiem_match.group(1).upper() if meridiem_match else None
        time_part = re.sub(r"\s*(AM|PM)\s*", "", text, flags=re.IGNORECASE).strip()
        segments = time_part.split(":")
        if len(segments) < 2:
            raise ValueError(f"Invalid slot time format: {slot_str}")
        hour = int(segments[0])
        minute = int(segments[1])
        if meridiem == "PM" and hour != 12:
            hour += 12
        elif meridiem == "AM" and hour == 12:
            hour = 0
        return time(hour, minute)

    async def check_home_collection_service_availability(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        address_line: str,
        landmark: str | None,
        city: str,
        pincode: str,
    ) -> dict:
        await ensure_console_access(db, employee, engagement_id, repository=self._repository)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")
        ensure_engagement_running(engagement)

        participant = await self._repository.get_participant_for_user_engagement(
            db, user_id=user_id, engagement_id=engagement_id,
        )
        if participant is None:
            raise AppError(status_code=404, error_code="PARTICIPANT_NOT_FOUND", message="Participant is not enrolled in this engagement")
        if participant.booking_id:
            raise AppError(status_code=409, error_code="BOOKING_ALREADY_EXISTS", message="A booking already exists for this participant")

        pkg = await self._load_healthians_package_for_engagement(db, engagement)

        results = await search_places(f"{city.strip()} {pincode.strip()}", limit=1)
        geocoded = results[0] if results else {}
        latitude = geocoded.get("latitude")
        longitude = geocoded.get("longitude")
        if latitude is None or longitude is None:
            raise AppError(status_code=422, error_code="GEOCODE_FAILED", message="Could not geocode address")

        participant.address = address_line.strip()
        participant.sub_locality = address_line.strip()
        participant.landmark = landmark.strip() if landmark else None
        participant.pincode = pincode.strip()
        participant.city = city.strip()
        participant.state = geocoded.get("state")
        participant.country = geocoded.get("country")
        participant.latitude = latitude
        participant.longitude = longitude
        await db.flush()

        lat = str(latitude)
        lng = str(longitude)
        zipcode = pincode.strip()
        access_token = await healthians_client.get_access_token()

        serviceability_url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/checkServiceabilityByLocation_v2"
        serviceability_payload = {"lat": lat, "long": lng, "zipcode": zipcode, "is_ppmc_booking": 0}

        try:
            resp = await healthians_client.check_serviceability_by_location_v2(
                access_token, lat=lat, long=lng, zipcode=zipcode, is_ppmc_booking=0,
            )
        except Exception as exc:
            logger.exception("Healthians serviceability check failed for user %s", user_id)
            await log_healthians_call(
                db, engagement_id=engagement_id, user_id=user_id, provider="healthians",
                api_url=serviceability_url, request_payload=serviceability_payload,
                status="failed", error_message=str(exc),
            )
            raise AppError(status_code=502, error_code="HEALTHIANS_SERVICEABILITY_FAILED", message=str(exc)) from exc

        await log_healthians_call(
            db, engagement_id=engagement_id, user_id=user_id, provider="healthians",
            api_url=serviceability_url, request_payload=serviceability_payload,
            response_payload=resp, status="success" if resp.get("status") else "failed",
        )

        if not resp.get("status"):
            raise AppError(
                status_code=422,
                error_code="LOCATION_NOT_SERVICEABLE",
                message=resp.get("message") or "This location is not serviceable.",
            )

        zone_id = (resp.get("data") or {}).get("zone_id")
        participant.healthians_zone_id = str(zone_id) if zone_id else None
        await db.flush()

        return {
            "status": "serviceable",
            "message": resp.get("message", "Serviceable"),
            "zone_id": zone_id,
            "engagement_id": engagement_id,
            "user_id": user_id,
        }

    async def get_home_collection_available_slots(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        blood_collection_date: date,
    ) -> dict:
        await ensure_console_access(db, employee, engagement_id, repository=self._repository)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")
        ensure_engagement_running(engagement)

        participant = await self._repository.get_participant_for_user_engagement(
            db, user_id=user_id, engagement_id=engagement_id,
        )
        if participant is None:
            raise AppError(status_code=404, error_code="PARTICIPANT_NOT_FOUND", message="Participant is not enrolled in this engagement")

        if participant.latitude is None or participant.longitude is None or not (participant.pincode or "").strip():
            raise AppError(status_code=422, error_code="MISSING_LOCATION", message="Service availability has not been checked yet")

        pkg = await self._load_healthians_package_for_engagement(db, engagement)

        user_result = await db.execute(select(User).where(User.user_id == user_id))
        user = user_result.scalar_one_or_none()
        has_female = 1 if user and (user.gender or "").strip().lower().startswith("f") else 0

        amount = float(pkg.original_price) if pkg.original_price else 0
        external_package_id = pkg.external_package_id or 0

        payload: dict[str, Any] = {
            "slot_date": blood_collection_date.isoformat(),
            "zone_id": str(participant.healthians_zone_id or ""),
            "lat": str(participant.latitude or ""),
            "long": str(participant.longitude or ""),
            "zipcode": participant.pincode or "",
            "get_ppmc_slots": 0,
            "has_female_patient": has_female,
            "amount": amount,
            "package": [{"deal_id": [f"package_{external_package_id}"]}],
        }

        access_token = await healthians_client.get_access_token()
        api_url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/getSlotsByLocation"

        try:
            resp = await healthians_client.get_slots_by_location(access_token, payload)
        except Exception as exc:
            logger.exception("Healthians getSlotsByLocation failed for user %s", user_id)
            await log_healthians_call(
                db, engagement_id=engagement_id, user_id=user_id, provider="healthians",
                api_url=api_url, request_payload=payload,
                status="failed", error_message=str(exc),
            )
            raise AppError(status_code=502, error_code="HEALTHIANS_SLOTS_FAILED", message=str(exc)) from exc

        await log_healthians_call(
            db, engagement_id=engagement_id, user_id=user_id, provider="healthians",
            api_url=api_url, request_payload=payload,
            response_payload=resp, status="success" if resp.get("status") else "failed",
        )

        if not resp.get("status"):
            raise AppError(
                status_code=422,
                error_code="SLOTS_FETCH_FAILED",
                message=resp.get("message", "Failed to fetch slots"),
            )

        raw_slots = resp.get("data", []) or []
        slim_slots = [
            {
                "end_time": s.get("end_time"),
                "slot_date": s.get("slot_date"),
                "slot_time": s.get("slot_time"),
                "stm_id": s.get("stm_id"),
            }
            for s in raw_slots
            if isinstance(s, dict)
        ]

        return {
            "status": "success",
            "slots": slim_slots,
            "engagement_id": engagement_id,
            "user_id": user_id,
        }

    async def lock_home_collection_slot(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        blood_collection_date: date,
        blood_collection_time_slot_id: str,
        blood_collection_time_slot: str,
    ) -> dict:
        await ensure_console_access(db, employee, engagement_id, repository=self._repository)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")
        ensure_engagement_running(engagement)

        participant = await self._repository.get_participant_for_user_engagement(
            db, user_id=user_id, engagement_id=engagement_id,
        )
        if participant is None:
            raise AppError(status_code=404, error_code="PARTICIPANT_NOT_FOUND", message="Participant is not enrolled in this engagement")

        pkg = await self._load_healthians_package_for_engagement(db, engagement)

        vendor_billing_user_id = str(participant.booked_by_user_id)
        access_token = await healthians_client.get_access_token()
        api_url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/freezeSlot_v1"
        req_payload = {"slot_id": blood_collection_time_slot_id, "vendor_billing_user_id": vendor_billing_user_id}

        try:
            resp = await healthians_client.freeze_slot_v1(
                access_token,
                slot_id=blood_collection_time_slot_id,
                vendor_billing_user_id=vendor_billing_user_id,
            )
        except Exception as exc:
            logger.exception("Healthians freezeSlot_v1 failed for user %s", user_id)
            await log_healthians_call(
                db, engagement_id=engagement_id, user_id=user_id, provider="healthians",
                api_url=api_url, request_payload=req_payload,
                status="failed", error_message=str(exc),
            )
            raise AppError(status_code=502, error_code="HEALTHIANS_LOCK_FAILED", message=str(exc)) from exc

        await log_healthians_call(
            db, engagement_id=engagement_id, user_id=user_id, provider="healthians",
            api_url=api_url, request_payload=req_payload,
            response_payload=resp,
            status="success" if resp.get("status") and resp.get("resCode") == "RES0001" else "failed",
        )

        if not resp.get("status") or resp.get("resCode") != "RES0001":
            raise AppError(
                status_code=422,
                error_code="SLOT_LOCK_FAILED",
                message=resp.get("message", "Slot not available"),
            )

        try:
            slot_start_time = self._parse_slot_time(blood_collection_time_slot)
        except ValueError as exc:
            raise AppError(status_code=422, error_code="INVALID_SLOT_TIME", message=str(exc)) from exc

        participant.blood_collection_time_slot_id = blood_collection_time_slot_id
        participant.engagement_date = blood_collection_date
        participant.slot_start_time = slot_start_time
        await db.flush()

        return {
            "status": "success",
            "message": resp.get("message", "Slot locked"),
            "slot_id": (resp.get("data") or {}).get("slot_id", blood_collection_time_slot_id),
            "freeze_time": (resp.get("data") or {}).get("freeze_time"),
            "engagement_id": engagement_id,
            "user_id": user_id,
        }

    async def book_home_collection(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
    ) -> dict:
        await ensure_console_access(db, employee, engagement_id, repository=self._repository)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")
        ensure_engagement_running(engagement)

        participant = await self._repository.get_participant_for_user_engagement(
            db, user_id=user_id, engagement_id=engagement_id,
        )
        if participant is None:
            raise AppError(status_code=404, error_code="PARTICIPANT_NOT_FOUND", message="Participant is not enrolled in this engagement")
        if participant.booking_id:
            raise AppError(status_code=409, error_code="BOOKING_ALREADY_EXISTS", message="A booking already exists for this participant")
        if not participant.blood_collection_time_slot_id:
            raise AppError(status_code=422, error_code="SLOT_NOT_LOCKED", message="Blood collection slot is not locked")

        pkg = await self._load_healthians_package_for_engagement(db, engagement)

        if not settings.HEALTHIANS_CHECKSUM_KEY:
            raise AppError(status_code=500, error_code="CONFIG_ERROR", message="Healthians checksum key is not configured")

        user = await self._users_repository.get_user_by_id(db, user_id=user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        first_name = (user.first_name or "").strip()
        last_name = (user.last_name or "").strip()
        phone = (user.phone or "").strip()
        gender = _to_healthians_gender(user.gender)
        dob = _format_healthians_dob(user)
        if not first_name or not last_name or not phone or gender is None or (user.age is None and dob is None):
            raise AppError(
                status_code=422,
                error_code="INCOMPLETE_PARTICIPANT_PROFILE",
                message="Participant profile is missing required fields (name, phone, gender, age or date of birth)",
            )

        customer_name = f"{first_name} {last_name}".strip().upper()
        relation = (user.relationship or "self").strip() or "self"
        vendor_billing_user_id = str(participant.booked_by_user_id)
        external_package_id = pkg.external_package_id or 0
        slot_id = participant.blood_collection_time_slot_id or ""

        booking_payload: dict[str, Any] = {
            "customer": [{
                "customer_id": str(user_id),
                "customer_name": customer_name,
                "relation": relation,
                "age": user.age,
                "dob": dob or "",
                "gender": gender,
                "contact_number": phone,
                "email": (user.email or "").strip(),
            }],
            "slot": {"slot_id": slot_id},
            "package": [{"deal_id": [f"package_{external_package_id}"]}],
            "customer_calling_number": phone,
            "billing_cust_name": customer_name,
            "gender": gender,
            "mobile": phone,
            "email": (user.email or "").strip(),
            "sub_locality": (participant.sub_locality or "").strip(),
            "latitude": str(participant.latitude or ""),
            "longitude": str(participant.longitude or ""),
            "address": (participant.address or "").strip(),
            "zipcode": (participant.pincode or "").strip(),
            "landmark": (participant.landmark or "").strip(),
            "hard_copy": 0,
            "vendor_billing_user_id": vendor_billing_user_id,
            "payment_option": "prepaid",
            "discounted_price": 0,
            "zone_id": int(participant.healthians_zone_id) if participant.healthians_zone_id else 0,
            "is_ppmc_booking": 0,
        }

        access_token = await healthians_client.get_access_token()
        booking_url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/createBooking_v3"
        booking_log = await log_healthians_call(
            db, engagement_id=engagement_id, user_id=user_id, provider="healthians",
            api_url=booking_url, request_payload=booking_payload, status="pending",
        )

        try:
            booking_response = await healthians_client.create_booking_v3(
                access_token, booking_payload, checksum_key=settings.HEALTHIANS_CHECKSUM_KEY,
            )
            if not booking_response.get("status"):
                await finalize_healthians_sync_log(
                    db, sync_log_id=booking_log.sync_log_id,
                    status="failed", response_payload=booking_response,
                    error_message=booking_response.get("message"),
                )
                raise AppError(
                    status_code=422,
                    error_code="HEALTHIANS_BOOKING_FAILED",
                    message=booking_response.get("message") or "Healthians booking failed",
                )

            healthians_booking_id = booking_response.get("booking_id")
            if not healthians_booking_id:
                await finalize_healthians_sync_log(
                    db, sync_log_id=booking_log.sync_log_id,
                    status="failed", response_payload=booking_response,
                    error_message="Missing booking_id in Healthians response",
                )
                raise AppError(
                    status_code=502,
                    error_code="HEALTHIANS_BOOKING_FAILED",
                    message="Healthians response did not include booking_id",
                )

            await finalize_healthians_sync_log(
                db, sync_log_id=booking_log.sync_log_id,
                status="success", response_payload=booking_response,
            )
        except AppError:
            raise
        except Exception as exc:
            await finalize_healthians_sync_log(
                db, sync_log_id=booking_log.sync_log_id,
                status="failed", error_message=str(exc),
            )
            raise AppError(status_code=502, error_code="HEALTHIANS_BOOKING_FAILED", message=str(exc)) from exc

        participant.booking_id = str(healthians_booking_id)
        await db.flush()

        return {
            "status": booking_response.get("status"),
            "message": booking_response.get("message"),
            "lead_id": booking_response.get("lead_id"),
            "booking_id": str(healthians_booking_id),
            "resCode": booking_response.get("resCode"),
            "tatDetail": booking_response.get("tatDetail"),
            "engagement_participant_id": participant.engagement_participant_id,
            "user_id": user_id,
            "engagement_id": engagement_id,
        }
