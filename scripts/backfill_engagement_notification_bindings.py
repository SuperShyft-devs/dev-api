"""Backfill engagement notification binding slots from global defaults (per engagement profile)."""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select

from db.session import async_session_factory
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagements.models import Engagement
from modules.notifications.event_applicability import profile_from_engagement

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    repo = EngagementNotificationsRepository()
    async with async_session_factory() as db:
        result = await db.execute(select(Engagement).order_by(Engagement.engagement_id.asc()))
        engagements = list(result.scalars().all())
        total_slots = 0
        for engagement in engagements:
            profile = await profile_from_engagement(db, engagement)
            rows = await repo.seed_from_global_defaults(
                db, engagement_id=engagement.engagement_id, profile=profile
            )
            if rows:
                logger.info(
                    "engagement_id=%s code=%s seeded_slots=%s",
                    engagement.engagement_id,
                    engagement.engagement_code,
                    len(rows),
                )
                total_slots += len(rows)
        await db.commit()
    logger.info("Done. total_new_slots=%s", total_slots)


if __name__ == "__main__":
    asyncio.run(main())
