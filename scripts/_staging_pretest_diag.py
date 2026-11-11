"""One-off staging diagnostic: why pretest cron matched=0."""
from __future__ import annotations

import asyncio
from datetime import timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text

from db.session import AsyncSessionLocal

_IST = ZoneInfo("Asia/Kolkata")


async def main() -> None:
    from datetime import datetime

    today = datetime.now(_IST).date()
    tomorrow = today + timedelta(days=1)
    async with AsyncSessionLocal() as db:
        for eid in (1095,):
            row = (
                await db.execute(
                    text(
                        """
                        SELECT e.engagement_id, e.status, e.start_date,
                               ep.user_id, ep.paid, pbb.collection_date, pbb.status AS pbb_status
                        FROM engagements e
                        JOIN engagement_participants ep ON ep.engagement_id = e.engagement_id
                        LEFT JOIN participant_blood_bookings pbb
                          ON pbb.engagement_participant_id = ep.engagement_participant_id
                        WHERE e.engagement_id = :eid
                        ORDER BY pbb.id DESC NULLS LAST
                        LIMIT 5
                        """
                    ),
                    {"eid": eid},
                )
            ).mappings().all()
            print("engagement", eid, "today_ist", today, "tomorrow", tomorrow)
            for r in row:
                print(dict(r))
            en = (
                await db.execute(
                    text(
                        """
                        SELECT event_code, notification_services IS NOT NULL AS has_services
                        FROM engagement_notifications
                        WHERE engagement_id = :eid AND event_code LIKE 'reminders.%'
                        """
                    ),
                    {"eid": eid},
                )
            ).mappings().all()
            print("reminder bindings", [dict(x) for x in en])


if __name__ == "__main__":
    asyncio.run(main())
