"""Dispatch WhatsApp notification services on staging (run on server)."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone

from sqlalchemy import select

from db.session import AsyncSessionLocal
from modules.notifications.models import Notification, NotificationService
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import SessionDetails
from modules.notifications.service import NotificationsService
from modules.notifications.staging_e2e_context import (
    e2e_skip_reason,
    merge_e2e_contact,
    service_keys_for_mode,
)

PHONE = "8762830757"
EMAIL = "pratheek.fitnastic@gmail.com"
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Staging WhatsApp notification E2E dispatch")
    parser.add_argument(
        "--mode",
        choices=("all", "smoke"),
        default="all",
        help="all: send fillable services; smoke: OTP + pretest + welcome-whatsapp only",
    )
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()
    mode_keys = service_keys_for_mode(args.mode)
    participant = _sample_participant()
    session = SessionDetails(**_sample_session())

    service = NotificationsService(NotificationsRepository())
    counts = {"sent": 0, "skipped": 0, "error": 0}

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(NotificationService).where(
                NotificationService.channel == "whatsapp",
                NotificationService.is_active.is_(True),
            )
        )
        services = list(result.scalars().all())
        print(f"Found {len(services)} active WhatsApp services (mode={args.mode})")
        summary: list[dict] = []

        for svc in sorted(services, key=lambda s: s.service_key or ""):
            if mode_keys is not None and svc.service_key not in mode_keys:
                continue

            row: dict = {"service_key": svc.service_key, "template_name": svc.template_name}
            skip = e2e_skip_reason(svc, phone=PHONE, otp=OTP, participant=participant)
            if skip:
                row["status"] = "skipped"
                row["reason"] = skip
                counts["skipped"] += 1
                summary.append(row)
                print(json.dumps(row, default=str))
                continue

            try:
                base_contact = {
                    "first_name": "Pratheek",
                    "last_name": "Bedre",
                    "phone": PHONE,
                    "email": EMAIL,
                }
                contact = merge_e2e_contact(
                    base_contact,
                    svc,
                    phone=PHONE,
                    otp=OTP,
                    participant=participant,
                    session_details=_sample_session() if svc.require_session_details else None,
                )
                extra: dict = {}
                if svc.require_otp:
                    extra["otp"] = OTP
                if svc.require_participant_detail:
                    extra["participant_details"] = participant
                if svc.require_session_details:
                    extra["session_details"] = session

                out = await service.dispatch_to_contacts(
                    db,
                    service_key=svc.service_key,
                    contacts=[contact],
                    **extra,
                )
                await db.commit()
                row["status"] = out.get("status")
                row["queue_id"] = out.get("queue_id")
                row["message"] = out.get("message")
                counts["sent"] += 1
            except Exception as exc:
                await db.rollback()
                row["status"] = "error"
                row["error"] = str(exc)[:500]
                counts["error"] += 1
            summary.append(row)
            print(json.dumps(row, default=str))

        print("--- summary ---")
        print(json.dumps(counts, indent=2))

        q = await db.execute(
            select(Notification)
            .where(Notification.queue_id.isnot(None))
            .order_by(Notification.notification_id.desc())
            .limit(30)
        )
        recent = [
            {
                "notification_id": n.notification_id,
                "service_key": n.service_key,
                "queue_id": n.queue_id,
                "status": n.status,
            }
            for n in q.scalars().all()
        ]
        print("--- recent queue_ids ---")
        print(json.dumps(recent, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
