#!/usr/bin/env bash
set -euo pipefail
cd /var/www/staging-api
source venv/bin/activate
set -a
source .env
set +a
EID="${1:-}"
export STAGING_EID="$EID"
python - <<'PY'
import asyncio
import os
import sys
from sqlalchemy import text

from db.session import AsyncSessionLocal
from modules.engagement_notifications.repository import EngagementNotificationsRepository

async def main() -> None:
    eid_arg = os.environ.get("STAGING_EID") or (sys.argv[1] if len(sys.argv) > 1 else None)
    async with AsyncSessionLocal() as db:
        if eid_arg:
            eid = int(eid_arg)
        else:
            eid = int(
                (
                    await db.execute(
                        text("SELECT engagement_id FROM engagements ORDER BY engagement_id DESC LIMIT 1")
                    )
                ).scalar_one()
            )
        etype = int(
            (
                await db.execute(
                    text("SELECT engagement_type FROM engagements WHERE engagement_id = :e"),
                    {"e": eid},
                )
            ).scalar_one()
        )
        repo = EngagementNotificationsRepository()
        rows = await repo.populate_from_defaults(
            db, engagement_id=eid, engagement_type_id=etype
        )
        await db.commit()
        print(f"populated engagement_id={eid} events={len(rows)}")

asyncio.run(main())
PY
python -m db.jobs.dispatch_pretest_reminders --yes
python -m db.jobs.dispatch_questionnaire_reminders --yes
