"""Inbound webhook handling."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, time
from typing import Any, Literal
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from modules.assessments.repository import AssessmentsRepository
from modules.audit.cron_sync_logging import (
    finalize_integration_call,
    finalize_integration_sync_log_isolated,
    log_integration_call,
    persist_integration_sync_log_isolated,
)
from db.transaction import release_request_transaction
from modules.diagnostics.healthians.sync_log import (
    finalize_healthians_sync_log_isolated,
    persist_healthians_sync_log_isolated,
)
from modules.diagnostics.orange_health.sync_log import persist_orange_health_sync_log_isolated
from modules.diagnostics.orange_health.webhook_payload import (
    build_sync_log_request_payload,
    parse_orange_health_webhook_body,
    resolve_webhook_engagement_context,
)
from modules.diagnostics.orange_health.webhook_verify import verify_orange_health_webhook_signature
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagements.blood_bookings_access import get_current_row, read_schedule_from_row
from modules.engagements.blood_bookings_repository import BloodBookingsRepository
from modules.engagements.models import EngagementParticipant
from modules.engagements.repository import EngagementsRepository
from modules.notifications.dedup import should_skip_notification
from modules.notifications.pretest_reminders import format_blood_collection_slot
from modules.notifications.schemas import DispatchRequest, SessionDetails
from modules.notifications.service import NotificationsService
from modules.reports.models import IndividualHealthReport
from modules.reports.repository import ReportsRepository
from modules.webhooks.receiver.schemas import AuraeWebhookPayload, HealthiansWebhookPayload
from modules.webhooks.sender.service import WebhookSenderService

logger = logging.getLogger(__name__)

_PROVIDER_AURAE = "aurae"
_VIFC = "vifc"
_HEALTHIANS_NEW_BOOKING_STATUS = "BS005"
_HEALTHIANS_COLLECTED_BOOKING_STATUS = "BS007"
_IST = ZoneInfo("Asia/Kolkata")
_BOOKING_CONFIRMATION_SERVICE_KEYS: tuple[str, ...] = (
    "booking-confirmation-whatsapp",
    "booking-confirmation-email",
)
_PHLEBO_EVENT_SERVICE_KEYS: dict[str, tuple[str, str]] = {
    "phlebo_assigned": ("phlebo-assigned-whatsapp", "phlebo-assigned-email"),
    "phlebo_reassigned": ("phlebo-reassigned-whatsapp", "phlebo-reassigned-email"),
    "phlebo_enroute": ("phlebo-enroute-whatsapp", "phlebo-enroute-email"),
    "phlebo_delay_notification": ("phlebo-delay-whatsapp", "phlebo-delay-email"),
}
_PHLEBO_EVENT_CODES: dict[str, str] = {
    "phlebo_assigned": "bookings.phlebo_assigned",
    "phlebo_reassigned": "bookings.phlebo_reassigned",
    "phlebo_enroute": "bookings.phlebo_enroute",
    "phlebo_delay_notification": "bookings.phlebo_delayed",
}
_PHLEBO_EVENTS_WITH_SESSION = frozenset({"phlebo_assigned", "phlebo_reassigned"})
AuraeEvent = Literal["results", "report"]


def _is_healthians_new_booking_webhook(payload: dict) -> bool:
    """True for status_updated with booking_status/customer_status BS005."""
    if str(payload.get("type") or "").strip() != "status_updated":
        return False
    data = payload.get("data")
    if not isinstance(data, dict):
        return False
    status = str(
        data.get("booking_status") or data.get("customer_status") or ""
    ).strip().upper()
    return status == _HEALTHIANS_NEW_BOOKING_STATUS


def _normalize_healthians_sample_collected_time_text(raw: object) -> str:
    """Healthians may send ``07:19: AM`` (extra colon before AM/PM)."""
    text = str(raw or "").strip()
    if not text:
        return ""
    return re.sub(r":\s*(AM|PM)\b", r" \1", text, flags=re.IGNORECASE)


def parse_healthians_sample_collected_time(raw: object) -> datetime | None:
    """Parse ``sample_collected_time`` into an IST-aware datetime."""
    text = _normalize_healthians_sample_collected_time_text(raw)
    if not text:
        return None
    for fmt in (
        "%Y-%m-%d %I:%M %p",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %I:%M:%S %p",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            naive = datetime.strptime(text, fmt)
            return naive.replace(tzinfo=_IST)
        except ValueError:
            continue
    return None


def _is_healthians_sample_collected_webhook(payload: dict) -> bool:
    event_type = str(payload.get("type") or "").strip()
    if event_type == "sample_collected_confirmation":
        return True
    if event_type != "status_updated":
        return False
    data = payload.get("data")
    if not isinstance(data, dict):
        return False
    status = str(
        data.get("booking_status") or data.get("customer_status") or ""
    ).strip().upper()
    return status == _HEALTHIANS_COLLECTED_BOOKING_STATUS


def _parse_healthians_collection_date(raw: object) -> date | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _format_collection_slot_range(*, start: str, end: str = "") -> str:
    """Format collection window as ``10:00 AM to 11:00 AM`` when both ends exist."""
    start_text = (start or "").strip()
    end_text = (end or "").strip()
    if start_text and end_text:
        return f"{start_text} to {end_text}"
    return start_text or end_text


def session_details_for_healthians_booking(
    *,
    payload_data: dict,
    engagement_date: date | None,
    slot_start_time: time | None,
    cabin: str | None,
) -> SessionDetails | None:
    """Build session_details from Healthians BS005 payload, falling back to participant.

    ``slot`` is a time window (``start to end``) when Healthians provides both
    ``start_time`` and ``end_time``; otherwise a single start time.
    """
    collection_date = _parse_healthians_collection_date(
        payload_data.get("sample_collection_date")
    )
    if collection_date is None:
        collection_date = engagement_date

    start = str(payload_data.get("start_time") or "").strip()
    end = str(payload_data.get("end_time") or "").strip()
    if not start:
        start = format_blood_collection_slot(slot_start_time)

    slot = _format_collection_slot_range(start=start, end=end)

    if collection_date is None or not slot:
        return None

    cabin_value = (cabin or "").strip() or None
    return SessionDetails(
        want=True,
        date=collection_date,
        slot=slot,
        expert_type="blood_collection",
        cabin=cabin_value,
    )


def _healthians_webhook_type(payload: dict) -> str:
    return str(payload.get("type") or "").strip()


def _build_phlebo_participant_details(event_type: str, data: dict) -> dict[str, Any] | None:
    if not isinstance(data, dict):
        return None

    phlebo_name = str(data.get("phlebo_name") or "").strip()
    if not phlebo_name:
        return None

    details: dict[str, Any] = {
        "event_type": event_type,
        "phlebo_name": phlebo_name,
    }
    masked_number = str(data.get("masked_number") or "").strip()
    if masked_number:
        details["masked_number"] = masked_number

    if event_type in _PHLEBO_EVENTS_WITH_SESSION:
        message = str(data.get("message") or "").strip()
        if message:
            details["message"] = message
        tracking_url = str(data.get("url") or "").strip()
        if tracking_url:
            details["tracking_url"] = tracking_url
    elif event_type == "phlebo_enroute":
        tracking_url = str(data.get("tracking_link") or "").strip()
        if not tracking_url:
            return None
        details["tracking_url"] = tracking_url
        vendor_booking_id = str(data.get("vendor_booking_id") or "").strip()
        if vendor_booking_id:
            details["vendor_booking_id"] = vendor_booking_id
    elif event_type == "phlebo_delay_notification":
        eta_raw = data.get("etaInMinutes")
        if eta_raw is None or str(eta_raw).strip() == "":
            return None
        try:
            details["eta_in_minutes"] = int(eta_raw)
        except (TypeError, ValueError):
            return None

    return details


def session_details_for_phlebo_assigned(
    *,
    payload_data: dict,
    engagement_date: date | None,
    slot_start_time: time | None,
    cabin: str | None,
) -> SessionDetails | None:
    """Build session_details for phlebo_assigned / phlebo_reassigned webhooks."""
    return session_details_for_healthians_booking(
        payload_data=payload_data,
        engagement_date=engagement_date,
        slot_start_time=slot_start_time,
        cabin=cabin,
    )

class WebhooksReceiverService:
    """Process inbound provider webhooks."""

    def __init__(
        self,
        *,
        engagements_repository: EngagementsRepository,
        sender_service: WebhookSenderService,
        assessments_repository: AssessmentsRepository | None = None,
        reports_repository: ReportsRepository | None = None,
        notifications_service: NotificationsService | None = None,
        engagement_notifications_repository: EngagementNotificationsRepository | None = None,
    ) -> None:
        self._engagements_repository = engagements_repository
        self._sender_service = sender_service
        self._assessments_repository = assessments_repository or AssessmentsRepository()
        self._reports_repository = reports_repository or ReportsRepository()
        self._notifications_service = notifications_service
        self._en_repo = engagement_notifications_repository or EngagementNotificationsRepository()

    async def _resolve_participant(
        self,
        db: AsyncSession,
        payload: dict,
    ) -> EngagementParticipant | None:
        booking_ids: list[str] = []

        primary = str(payload.get("booking_id") or "").strip()
        if primary:
            booking_ids.append(primary)

        data = payload.get("data")
        if isinstance(data, dict):
            ref_booking_id = data.get("ref_booking_id")
            ref = str(ref_booking_id or "").strip()
            if ref and ref != "0" and ref not in booking_ids:
                booking_ids.append(ref)

        for booking_id in booking_ids:
            participant = await self._engagements_repository.get_participant_by_booking_id(
                db,
                booking_id=booking_id,
            )
            if participant is not None:
                return participant

        return None

    async def _maybe_stamp_collected_at_from_healthians_webhook(
        self,
        db: AsyncSession,
        *,
        participant: EngagementParticipant,
        payload: dict,
    ) -> None:
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        collected_at = parse_healthians_sample_collected_time(data.get("sample_collected_time"))
        if collected_at is None:
            collected_at = datetime.now(_IST)

        blood_repo = BloodBookingsRepository()
        booking_id = str(payload.get("booking_id") or "").strip()
        row = None
        if booking_id:
            row = await blood_repo.get_active_by_booking_id(db, booking_id=booking_id)
        if row is None:
            row = await blood_repo.get_current_collection(
                db,
                engagement_participant_id=participant.engagement_participant_id,
            )
        await blood_repo.set_collected_at_if_empty(db, row=row, collected_at=collected_at)

    async def _maybe_insert_resample_booking(
        self,
        db: AsyncSession,
        *,
        participant: EngagementParticipant,
        payload: dict,
    ) -> None:
        """When Healthians sends a new booking_id with ref_booking_id, record a resample row."""
        primary = str(payload.get("booking_id") or "").strip()
        data = payload.get("data")
        if not primary or not isinstance(data, dict):
            return
        ref = str(data.get("ref_booking_id") or "").strip()
        if not ref or ref == "0" or ref == primary:
            return
        blood_repo = BloodBookingsRepository()
        if await blood_repo.get_by_booking_id(db, booking_id=primary) is not None:
            return
        parent = await blood_repo.get_by_booking_id(db, booking_id=ref)
        if parent is None:
            return
        await blood_repo.insert_resample(
            db,
            engagement_participant_id=participant.engagement_participant_id,
            booking_id=primary,
            parent_booking_id=ref,
        )

    @staticmethod
    async def _participant_collection_schedule(
        db: AsyncSession,
        participant: EngagementParticipant,
    ) -> tuple[date | None, time | None, str | None]:
        sched = read_schedule_from_row(await get_current_row(db, participant))
        cabin = (sched.get("blood_collection_cabin") or "").strip() or None
        return sched.get("engagement_date"), sched.get("slot_start_time"), cabin

    async def _dispatch_booking_confirmation_notifications(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
        session_details: SessionDetails,
    ) -> list[dict[str, Any]]:
        """Dispatch booking confirmation services for a Healthians BS005 webhook."""
        if self._notifications_service is None:
            return []

        from modules.notifications.engine import get_notification_engine

        try:
            ids = await get_notification_engine().emit(
                db,
                event_code="bookings.confirmed",
                engagement_id=engagement_id,
                context={
                    "user_id": user_id,
                    "session_details": session_details,
                    "fallback_service_keys": list(_BOOKING_CONFIRMATION_SERVICE_KEYS),
                },
            )
            if ids:
                return [{"action": "dispatched", "notification_ids": ids}]
        except Exception as exc:
            logger.warning(
                "Healthians booking confirmation failed: user=%s engagement=%s: %s",
                user_id,
                engagement_id,
                exc,
            )
            return [{"action": "failed", "reason": str(exc)[:200]}]
        return [{"action": "skipped", "reason": "no_bindings"}]

    async def _dispatch_phlebo_event_notifications(
        self,
        db: AsyncSession,
        *,
        event_type: str,
        user_id: int,
        engagement_id: int,
        participant_details: dict[str, Any],
        session_details: SessionDetails | None = None,
    ) -> list[dict[str, Any]]:
        """Dispatch WhatsApp + email for a Healthians phlebo webhook event."""
        if self._notifications_service is None:
            return []

        event_code = _PHLEBO_EVENT_CODES.get(event_type)
        service_keys = _PHLEBO_EVENT_SERVICE_KEYS.get(event_type)
        if not event_code or not service_keys:
            return []

        from modules.notifications.engine import get_notification_engine

        try:
            ids = await get_notification_engine().emit(
                db,
                event_code=event_code,
                engagement_id=engagement_id,
                context={
                    "user_id": user_id,
                    "participant_details": participant_details,
                    "session_details": session_details,
                    "fallback_service_keys": list(service_keys),
                },
            )
            if ids:
                return [{"action": "dispatched", "event_code": event_code, "notification_ids": ids}]
        except Exception as exc:
            logger.warning(
                "Healthians phlebo notification failed: event=%s user=%s engagement=%s: %s",
                event_type,
                user_id,
                engagement_id,
                exc,
            )
            return [{"action": "failed", "reason": str(exc)[:200]}]
        return [{"action": "skipped", "reason": "no_bindings"}]

    async def handle_healthians_webhook(
        self,
        db: AsyncSession,
        *,
        payload: HealthiansWebhookPayload,
        api_endpoint_url: str,
    ) -> dict:
        payload_dict = payload.model_dump(mode="json")

        participant = await self._resolve_participant(db, payload_dict)
        if participant is not None:
            if _is_healthians_sample_collected_webhook(payload_dict):
                await self._maybe_stamp_collected_at_from_healthians_webhook(
                    db,
                    participant=participant,
                    payload=payload_dict,
                )
            await self._maybe_insert_resample_booking(
                db,
                participant=participant,
                payload=payload_dict,
            )
        engagement_id = participant.engagement_id if participant else None
        user_id = participant.user_id if participant else None
        # Snapshot before release_request_transaction expires ORM attrs.
        if participant is not None:
            (
                participant_engagement_date,
                participant_slot_start_time,
                participant_cabin,
            ) = await self._participant_collection_schedule(db, participant)
        else:
            participant_engagement_date = None
            participant_slot_start_time = None
            participant_cabin = None

        await release_request_transaction(db)

        sync_log_id = await persist_healthians_sync_log_isolated(
            engagement_id=engagement_id,
            user_id=user_id,
            provider="healthians",
            api_url=api_endpoint_url,
            request_payload=payload_dict,
            status="pending",
        )

        forwards = await self._sender_service.forward_payload(
            db,
            payload=payload_dict,
            engagement_id=engagement_id,
            user_id=user_id,
        )

        notifications_dispatched: list[dict[str, Any]] = []
        if (
            engagement_id is not None
            and user_id is not None
            and _is_healthians_new_booking_webhook(payload_dict)
        ):
            data = payload_dict.get("data") if isinstance(payload_dict.get("data"), dict) else {}
            session_details = session_details_for_healthians_booking(
                payload_data=data,
                engagement_date=participant_engagement_date,
                slot_start_time=participant_slot_start_time,
                cabin=participant_cabin,
            )
            if session_details is None:
                logger.info(
                    "Healthians booking confirmation skipped: missing session_details "
                    "for user=%s engagement=%s booking_id=%s",
                    user_id,
                    engagement_id,
                    payload_dict.get("booking_id"),
                )
                notifications_dispatched.append(
                    {
                        "action": "skipped",
                        "reason": "missing session_details (date/slot)",
                    }
                )
            else:
                notifications_dispatched = await self._dispatch_booking_confirmation_notifications(
                    db,
                    user_id=user_id,
                    engagement_id=engagement_id,
                    session_details=session_details,
                )
        elif engagement_id is not None and user_id is not None:
            event_type = _healthians_webhook_type(payload_dict)
            if event_type in _PHLEBO_EVENT_SERVICE_KEYS:
                data = (
                    payload_dict.get("data")
                    if isinstance(payload_dict.get("data"), dict)
                    else {}
                )
                participant_details = _build_phlebo_participant_details(event_type, data)
                if participant_details is None:
                    logger.info(
                        "Healthians phlebo notification skipped: missing participant_details "
                        "for event=%s user=%s engagement=%s booking_id=%s",
                        event_type,
                        user_id,
                        engagement_id,
                        payload_dict.get("booking_id"),
                    )
                    notifications_dispatched.append(
                        {
                            "event_type": event_type,
                            "action": "skipped",
                            "reason": "missing required participant_details fields",
                        }
                    )
                else:
                    session_details: SessionDetails | None = None
                    if event_type in _PHLEBO_EVENTS_WITH_SESSION:
                        session_details = session_details_for_phlebo_assigned(
                            payload_data=data,
                            engagement_date=participant_engagement_date,
                            slot_start_time=participant_slot_start_time,
                            cabin=participant_cabin,
                        )
                        if session_details is None:
                            logger.info(
                                "Healthians phlebo notification skipped: missing session_details "
                                "for event=%s user=%s engagement=%s booking_id=%s",
                                event_type,
                                user_id,
                                engagement_id,
                                payload_dict.get("booking_id"),
                            )
                            notifications_dispatched.append(
                                {
                                    "event_type": event_type,
                                    "action": "skipped",
                                    "reason": "missing session_details (date/slot)",
                                }
                            )
                        else:
                            notifications_dispatched = (
                                await self._dispatch_phlebo_event_notifications(
                                    db,
                                    event_type=event_type,
                                    user_id=user_id,
                                    engagement_id=engagement_id,
                                    participant_details=participant_details,
                                    session_details=session_details,
                                )
                            )
                    else:
                        notifications_dispatched = await self._dispatch_phlebo_event_notifications(
                            db,
                            event_type=event_type,
                            user_id=user_id,
                            engagement_id=engagement_id,
                            participant_details=participant_details,
                        )

        response_data = {
            "received": True,
            "sync_log_id": sync_log_id,
            "forwards": forwards,
        }
        if notifications_dispatched:
            response_data["notifications"] = notifications_dispatched

        await finalize_healthians_sync_log_isolated(
            sync_log_id=sync_log_id,
            status="success",
            response_payload=response_data,
        )

        return response_data

    @staticmethod
    def _expected_aurae_webhook_api_key() -> str:
        webhook_key = (settings.AURAE_WEBHOOK_API_KEY or "").strip()
        if webhook_key:
            return webhook_key
        return (settings.AURAE_API_KEY or "").strip()

    @classmethod
    def _verify_aurae_api_key(cls, api_key: str | None) -> None:
        expected = cls._expected_aurae_webhook_api_key()
        provided = (api_key or "").strip()
        if not expected or provided != expected:
            raise AppError(
                status_code=401,
                error_code="AUTH_FAILED",
                message="Invalid or missing x-api-key",
            )

    @staticmethod
    def _detect_aurae_event(payload: AuraeWebhookPayload) -> AuraeEvent:
        reports = payload.reports
        if isinstance(reports, dict):
            vifc_urls = reports.get("VIFC")
            if isinstance(vifc_urls, list) and any(str(u or "").strip() for u in vifc_urls):
                return "report"

        data = payload.data
        if isinstance(data, dict) and data:
            return "results"

        raise AppError(
            status_code=422,
            error_code="INVALID_INPUT",
            message="Aurae webhook must include results data or VIFC report URLs",
        )

    @staticmethod
    def _parse_assessment_instance_id(api_customer_id: str | None) -> int:
        raw = str(api_customer_id or "").strip()
        if not raw:
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message="api_customer_id is required",
            )
        try:
            assessment_instance_id = int(raw)
        except ValueError as exc:
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message="api_customer_id must be a numeric assessment_instance_id",
            ) from exc
        if assessment_instance_id <= 0:
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message="api_customer_id must be a positive assessment_instance_id",
            )
        return assessment_instance_id

    @staticmethod
    def _first_vifc_report_url(reports: dict[str, list[Any]] | None) -> str:
        if not isinstance(reports, dict):
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message="reports.VIFC is required for report webhooks",
            )
        urls = reports.get("VIFC")
        if not isinstance(urls, list):
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message="reports.VIFC must be a list of URLs",
            )
        for url in urls:
            cleaned = str(url or "").strip()
            if cleaned:
                return cleaned
        raise AppError(
            status_code=422,
            error_code="INVALID_INPUT",
            message="reports.VIFC must contain at least one URL",
        )

    async def _soft_validate_vifc_package(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        assessment_instance_id: int,
    ) -> None:
        package = await self._assessments_repository.get_package_by_id(db, package_id=package_id)
        if package is None:
            logger.warning(
                "Aurae webhook: package %s missing for assessment_instance_id=%s",
                package_id,
                assessment_instance_id,
            )
            return
        package_code = (package.package_code or "").strip().lower()
        type_code = (package.assessment_type_code or "").strip().lower()
        if package_code != _VIFC or type_code != _VIFC:
            logger.warning(
                "Aurae webhook: assessment_instance_id=%s package_code=%s type_code=%s "
                "(expected vifc); storing anyway",
                assessment_instance_id,
                package_code,
                type_code,
            )

    async def _dispatch_report_ready_notifications(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
        assessment_instance_id: int,
    ) -> list[dict[str, Any]]:
        """Dispatch configured report_ready notification services for the engagement."""
        if self._notifications_service is None:
            return []

        from modules.notifications.engine import get_notification_engine

        try:
            ids = await get_notification_engine().emit(
                db,
                event_code="reports.face_scan_ready",
                engagement_id=engagement_id,
                context={
                    "user_id": user_id,
                    "assessment_instance_id": assessment_instance_id,
                },
            )
            if ids:
                return [{"action": "dispatched", "notification_ids": ids}]
        except Exception as exc:
            logger.warning(
                "Aurae report_ready notification failed: user=%s engagement=%s: %s",
                user_id,
                engagement_id,
                exc,
            )
            return [{"action": "failed", "reason": str(exc)[:200]}]
        return [{"action": "skipped", "reason": "no_bindings"}]

    async def handle_aurae_webhook(
        self,
        db: AsyncSession,
        *,
        payload: AuraeWebhookPayload,
        api_endpoint_url: str,
        api_key: str | None,
    ) -> dict[str, Any]:
        self._verify_aurae_api_key(api_key)

        payload_dict = payload.model_dump(mode="json")

        # Resolve identity early so failed validations still leave a sync log.
        assessment_instance_id: int | None = None
        try:
            assessment_instance_id = self._parse_assessment_instance_id(payload.api_customer_id)
        except AppError:
            assessment_instance_id = None

        instance = None
        if assessment_instance_id is not None:
            instance = await self._assessments_repository.get_instance_by_id(
                db, assessment_instance_id=assessment_instance_id
            )

        engagement_id = int(instance.engagement_id) if instance is not None else None
        user_id = int(instance.user_id) if instance is not None else None

        sync_log_id = await persist_integration_sync_log_isolated(
            provider=_PROVIDER_AURAE,
            api_url=api_endpoint_url,
            engagement_id=engagement_id,
            user_id=user_id,
            request_payload=payload_dict,
        )

        try:
            event = self._detect_aurae_event(payload)
            if assessment_instance_id is None:
                assessment_instance_id = self._parse_assessment_instance_id(payload.api_customer_id)
        except AppError as exc:
            await finalize_integration_sync_log_isolated(
                sync_log_id=sync_log_id,
                status="failed",
                error_message=exc.message,
            )
            raise

        if instance is None:
            await finalize_integration_sync_log_isolated(
                sync_log_id=sync_log_id,
                status="failed",
                error_message=f"Assessment instance {assessment_instance_id} not found",
            )
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_NOT_FOUND",
                message="Assessment does not exist",
            )

        await self._soft_validate_vifc_package(
            db,
            package_id=int(instance.package_id),
            assessment_instance_id=assessment_instance_id,
        )

        existing = await self._reports_repository.get_individual_report_by_assessment(
            db,
            assessment_instance_id=assessment_instance_id,
        )

        if event == "results":
            data = payload.data if isinstance(payload.data, dict) else {}
            if existing is None:
                existing = await self._reports_repository.get_or_create_individual_report_by_assessment(
                    db,
                    user_id=user_id,
                    engagement_id=engagement_id,
                    assessment_instance_id=assessment_instance_id,
                )
                existing.reports = data
                existing = await self._reports_repository.update_individual_report(db, existing)
            else:
                existing.reports = data
                existing.assessment_instance_id = assessment_instance_id
                existing = await self._reports_repository.update_individual_report(db, existing)
        else:
            report_url = self._first_vifc_report_url(payload.reports)
            if existing is None:
                existing = await self._reports_repository.get_or_create_individual_report_by_assessment(
                    db,
                    user_id=user_id,
                    engagement_id=engagement_id,
                    assessment_instance_id=assessment_instance_id,
                )
                existing.report_url = report_url
                existing = await self._reports_repository.update_individual_report(db, existing)
            else:
                existing.report_url = report_url
                existing.assessment_instance_id = assessment_instance_id
                existing = await self._reports_repository.update_individual_report(db, existing)

        await release_request_transaction(db)

        notifications_dispatched: list[dict[str, Any]] = []
        if event == "report" and engagement_id is not None and user_id is not None:
            notifications_dispatched = await self._dispatch_report_ready_notifications(
                db,
                user_id=user_id,
                engagement_id=engagement_id,
                assessment_instance_id=assessment_instance_id,
            )

        response_data: dict[str, Any] = {
            "received": True,
            "event": event,
            "assessment_instance_id": assessment_instance_id,
            "report_id": existing.report_id,
            "sync_log_id": sync_log_id,
        }
        if notifications_dispatched:
            response_data["notifications"] = notifications_dispatched

        await finalize_integration_sync_log_isolated(
            sync_log_id=sync_log_id,
            status="success",
            response_payload=response_data,
        )

        return response_data

    async def handle_orange_health_webhook(
        self,
        db: AsyncSession,
        *,
        raw_body: bytes,
        api_endpoint_url: str,
        signature: str | None,
        x_oh_event_id: str | None,
    ) -> dict[str, Any]:
        """Verify signature, log webhook to integration_sync_logs only."""
        verify_orange_health_webhook_signature(
            raw_body,
            signature,
            settings.ORANGE_HEALTH_WEBHOOK_SECRET,
        )

        body = parse_orange_health_webhook_body(raw_body)
        engagement_id, user_id = await resolve_webhook_engagement_context(db, body)
        request_payload = build_sync_log_request_payload(
            body=body,
            x_oh_event_id=x_oh_event_id,
        )
        event_name = str(request_payload.get("event") or "").strip()
        event_id = str(request_payload.get("x_oh_event_id") or "").strip()

        await release_request_transaction(db)

        sync_log_id = await persist_orange_health_sync_log_isolated(
            engagement_id=engagement_id,
            user_id=user_id,
            api_url=api_endpoint_url,
            request_payload=request_payload,
            status="success",
        )

        return {
            "received": True,
            "sync_log_id": sync_log_id,
            "event": event_name or None,
            "x_oh_event_id": event_id or None,
        }
