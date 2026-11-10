"""Dispatch all active WhatsApp notification services to one phone on staging (run on server)."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from sqlalchemy import select

from db.session import AsyncSessionLocal
from modules.notifications.models import Notification, NotificationService
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService

PHONE = "8762830757"
OTP = "111222"


def _sample_participant() -> dict:
    return {
        "booker_name": "E2E Test",
        "booker_phone": PHONE,
        "recipient_name": "Pratheek",
        "relationship": "self",
        "gift_message": "staging e2e",
    }


def _sample_session() -> dict:
    today = datetime.now(timezone.utc).date().isoformat()
    return {
        "want": True,
        "date": today,
        "slot": "10:00 AM",
        "expert_type": "general",
        "cabin": "A1",
    }


async def main() -> None:
    service = NotificationsService(NotificationsRepository())
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(NotificationService).where(
                NotificationService.channel == "whatsapp",
                NotificationService.is_active.is_(True),
            )
        )
        services = list(result.scalars().all())
        print(f"Found {len(services)} active WhatsApp services")
        summary: list[dict] = []

        for svc in sorted(services, key=lambda s: s.service_key or ""):
            row: dict = {"service_key": svc.service_key, "template_name": svc.template_name}
            try:
                extra: dict = {}
                if svc.require_otp:
                    extra["otp"] = OTP
                if svc.require_participant_detail:
                    extra["participant_details"] = _sample_participant()
                if svc.require_session_details:
                    from modules.notifications.schemas import SessionDetails

                    extra["session_details"] = SessionDetails(**_sample_session())

                out = await service.dispatch_to_contacts(
                    db,
                    service_key=svc.service_key,
                    contacts=[
                        {
                            "first_name": "Pratheek",
                            "last_name": "Bedre",
                            "phone": PHONE,
                            "email": "",
                            **({"otp": OTP} if svc.require_otp else {}),
                        }
                    ],
                    **extra,
                )
                await db.commit()
                row["status"] = out.get("status")
                row["queue_id"] = out.get("queue_id")
                row["message"] = out.get("message")
            except Exception as exc:
                await db.rollback()
                row["status"] = "error"
                row["error"] = str(exc)[:500]
            summary.append(row)
            print(json.dumps(row, default=str))

        q = await db.execute(
            select(Notification)
            .where(Notification.queue_id.isnot(None))
            .order_by(Notification.notification_id.desc())
            .limit(30)
        )
        recent = [
            {"notification_id": n.notification_id, "service_key": n.service_key, "queue_id": n.queue_id, "status": n.status}
            for n in q.scalars().all()
        ]
        print("--- recent queue_ids ---")
        print(json.dumps(recent, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
