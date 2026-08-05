"""Inbound webhook handling."""

from __future__ import annotations

import logging
from typing import Any, Literal

from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from modules.assessments.repository import AssessmentsRepository
from modules.audit.cron_sync_logging import finalize_integration_call, log_integration_call
from modules.diagnostics.healthians.sync_log import finalize_healthians_sync_log, log_healthians_call
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagements.models import EngagementParticipant
from modules.engagements.repository import EngagementsRepository
from modules.notifications.dedup import should_skip_notification
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService
from modules.reports.models import IndividualHealthReport
from modules.reports.repository import ReportsRepository
from modules.webhooks.receiver.schemas import AuraeWebhookPayload, HealthiansWebhookPayload
from modules.webhooks.sender.service import WebhookSenderService

logger = logging.getLogger(__name__)

_PROVIDER_AURAE = "aurae"
_VIFC = "vifc"
AuraeEvent = Literal["results", "report"]


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

    async def handle_healthians_webhook(
        self,
        db: AsyncSession,
        *,
        payload: HealthiansWebhookPayload,
        api_endpoint_url: str,
    ) -> dict:
        payload_dict = payload.model_dump(mode="json")

        participant = await self._resolve_participant(db, payload_dict)
        engagement_id = participant.engagement_id if participant else None
        user_id = participant.user_id if participant else None

        sync_log = await log_healthians_call(
            db,
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

        response_data = {
            "received": True,
            "sync_log_id": sync_log.sync_log_id,
            "forwards": forwards,
        }

        await finalize_healthians_sync_log(
            db,
            sync_log_id=sync_log.sync_log_id,
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

        service_keys = await self._en_repo.get_services_for_engagement_event(
            db, engagement_id=engagement_id, event_code="report_ready",
        )
        if not service_keys:
            return []

        dispatched: list[dict[str, Any]] = []
        for service_key in service_keys:
            try:
                skip_reason = await should_skip_notification(
                    db,
                    service_key=service_key,
                    user_id=user_id,
                    engagement_id=engagement_id,
                )
                if skip_reason:
                    logger.info(
                        "Aurae report_ready notification skipped: service_key=%s user=%s "
                        "engagement=%s reason=%s",
                        service_key, user_id, engagement_id, skip_reason,
                    )
                    dispatched.append({
                        "service_key": service_key,
                        "action": "skipped",
                        "reason": skip_reason,
                    })
                    continue

                result = await self._notifications_service.dispatch(
                    db,
                    payload=DispatchRequest(
                        service_key=service_key,
                        user_ids=[user_id],
                        engagement_id=engagement_id,
                        assessment_instance_id=assessment_instance_id,
                    ),
                    triggered_by_user_id=None,
                )
                dispatched.append({
                    "service_key": service_key,
                    "action": "dispatched",
                    "notification_id": result.get("notification_id"),
                })
            except Exception as exc:
                logger.warning(
                    "Aurae report_ready notification failed: service_key=%s user=%s "
                    "engagement=%s: %s",
                    service_key, user_id, engagement_id, exc,
                )
                dispatched.append({
                    "service_key": service_key,
                    "action": "failed",
                    "reason": str(exc)[:200],
                })
        return dispatched

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

        sync_log = await log_integration_call(
            db,
            provider=_PROVIDER_AURAE,
            api_url=api_endpoint_url,
            engagement_id=engagement_id,
            user_id=user_id,
            request_payload=payload_dict,
            status="pending",
        )

        try:
            event = self._detect_aurae_event(payload)
            if assessment_instance_id is None:
                assessment_instance_id = self._parse_assessment_instance_id(payload.api_customer_id)
        except AppError as exc:
            await finalize_integration_call(
                db,
                sync_log_id=sync_log.sync_log_id,
                status="failed",
                error_message=exc.message,
            )
            raise

        if instance is None:
            await finalize_integration_call(
                db,
                sync_log_id=sync_log.sync_log_id,
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
                report = IndividualHealthReport(
                    user_id=user_id,
                    engagement_id=engagement_id,
                    assessment_instance_id=assessment_instance_id,
                    reports=data,
                    blood_parameters=None,
                )
                existing = await self._reports_repository.create_individual_report(db, report)
            else:
                existing.reports = data
                existing.assessment_instance_id = assessment_instance_id
                existing = await self._reports_repository.update_individual_report(db, existing)
        else:
            report_url = self._first_vifc_report_url(payload.reports)
            if existing is None:
                report = IndividualHealthReport(
                    user_id=user_id,
                    engagement_id=engagement_id,
                    assessment_instance_id=assessment_instance_id,
                    reports=None,
                    blood_parameters=None,
                    report_url=report_url,
                )
                existing = await self._reports_repository.create_individual_report(db, report)
            else:
                existing.report_url = report_url
                existing.assessment_instance_id = assessment_instance_id
                existing = await self._reports_repository.update_individual_report(db, existing)

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
            "sync_log_id": sync_log.sync_log_id,
        }
        if notifications_dispatched:
            response_data["notifications"] = notifications_dispatched

        await finalize_integration_call(
            db,
            sync_log_id=sync_log.sync_log_id,
            status="success",
            response_payload=response_data,
        )

        return response_data
