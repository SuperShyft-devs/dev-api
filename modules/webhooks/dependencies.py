"""Webhooks module dependencies."""

from __future__ import annotations

from modules.assessments.repository import AssessmentsRepository
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagements.repository import EngagementsRepository
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.reports.repository import ReportsRepository
from modules.webhooks.receiver.service import WebhooksReceiverService
from modules.webhooks.sender.service import WebhookSenderService


def get_webhooks_receiver_service() -> WebhooksReceiverService:
    return WebhooksReceiverService(
        engagements_repository=EngagementsRepository(),
        sender_service=WebhookSenderService(),
        assessments_repository=AssessmentsRepository(),
        reports_repository=ReportsRepository(),
        notifications_service=NotificationsService(
            repository=NotificationsRepository(),
            metsights_service=MetsightsService(client=MetsightsClient()),
        ),
        engagement_notifications_repository=EngagementNotificationsRepository(),
    )
