"""Engagement console service."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from modules.diagnostics.healthians import client as healthians_client
from modules.diagnostics.healthians.sync_log import finalize_healthians_sync_log, log_healthians_call
from modules.diagnostics.models import DiagnosticPackage
from modules.employee.access_control import (
    ensure_console_access,
    ensure_employee_present,
    ensure_engagement_running,
)
from modules.employee.models import EmployeeRole
from modules.employee.service import EmployeeContext
from modules.engagements.models import Engagement
from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import _participant_enrollment_to_dict
from modules.users.models import User
from modules.users.repository import UsersRepository


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


class ConsoleService:
    def __init__(
        self,
        repository: EngagementsRepository,
        users_repository: UsersRepository | None = None,
    ):
        self._repository = repository
        self._users_repository = users_repository or UsersRepository()

    @staticmethod
    def _engagement_to_console_dict(engagement: Engagement) -> dict:
        return {
            "engagement_id": engagement.engagement_id,
            "engagement_name": engagement.engagement_name,
            "engagement_code": engagement.engagement_code,
            "start_date": engagement.start_date,
            "end_date": engagement.end_date,
            "status": engagement.status,
            "participant_count": engagement.participant_count,
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
            engagements = await self._repository.list_engagements_for_assigned_org_contact_person(
                db,
                employee_id=employee.employee_id,
                user_id=employee.user_id,
            )
        else:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )
        return [self._engagement_to_console_dict(e) for e in engagements]

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

        return self._engagement_to_console_dict(engagement)

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

        participants = await self._repository.list_participants_by_engagement_id(
            db,
            engagement_id=engagement_id,
            page=page,
            limit=limit,
        )
        total = await self._repository.count_distinct_participants_for_engagement(
            db,
            engagement_id=engagement_id,
        )

        result = [_participant_enrollment_to_dict(row) for row in participants]
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

        if diagnostic_package.external_camp_id is None or diagnostic_package.external_package_id is None:
            raise AppError(
                status_code=422,
                error_code="MISSING_DIAGNOSTIC_CONFIG",
                message="Diagnostic package is missing external camp ID or external package ID",
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
                    "relation": "self",
                    "age": user.age,
                    "dob": dob or "",
                    "gender": gender,
                    "contact_number": phone,
                    "email": (user.email or "").strip(),
                    "barcode": barcode.strip(),
                }
            ],
            "camp_id": diagnostic_package.external_camp_id,
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
            "vendor_billing_user_id": str(user_id),
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
