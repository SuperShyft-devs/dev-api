"""Inbound webhook handling."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from modules.diagnostics.healthians.sync_log import finalize_healthians_sync_log, log_healthians_call
from modules.engagements.models import EngagementParticipant
from modules.engagements.repository import EngagementsRepository
from modules.webhooks.receiver.schemas import HealthiansWebhookPayload
from modules.webhooks.sender.service import WebhookSenderService


class WebhooksReceiverService:
    """Process inbound provider webhooks."""

    def __init__(
        self,
        *,
        engagements_repository: EngagementsRepository,
        sender_service: WebhookSenderService,
    ) -> None:
        self._engagements_repository = engagements_repository
        self._sender_service = sender_service

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
