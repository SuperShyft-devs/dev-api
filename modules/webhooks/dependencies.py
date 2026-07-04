"""Webhooks module dependencies."""

from __future__ import annotations

from modules.engagements.repository import EngagementsRepository
from modules.webhooks.receiver.service import WebhooksReceiverService
from modules.webhooks.sender.service import WebhookSenderService


def get_webhooks_receiver_service() -> WebhooksReceiverService:
    return WebhooksReceiverService(
        engagements_repository=EngagementsRepository(),
        sender_service=WebhookSenderService(),
    )
