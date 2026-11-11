"""Staging notification E2E: seed test participants and run reminder/report crons.

Run on the staging API host (needs DB + .env):

    cd /var/www/staging-api && source venv/bin/activate
    python scripts/staging_notification_e2e.py --yes

Optional phases:

    python scripts/staging_notification_e2e.py --yes --phase onboard,pretest,questionnaire
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
import asyncio
import json
import sys
import urllib.error
import urllib.request
from datetime import date, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select, text

from db.session import AsyncSessionLocal
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagements.models import EngagementParticipant

_IST = ZoneInfo("Asia/Kolkata")
_STAGING_API = "https://staging-api.supershyft.com"


def _today_ist() -> date:
    return date.today()


def _http_json(method: str, path: str, body: dict | None = None) -> dict[str, Any]:
    url = f"{_STAGING_API}{path}"
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _onboard_payload(
    *,
    suffix: str,
    collection_date: date,
    email: str,
) -> dict[str, Any]:
    phone = f"87628{suffix.zfill(5)}"[-10:]
    return {
        "age": 30,
        "first_name": "Notify",
        "last_name": f"E2E{suffix}",
        "email": email,
        "phone": phone,
        "gender": "male",
        "address": "27th Main Road, HSR Layout",
        "pincode": "560102",
        "city": "Bengaluru",
        "blood_collection_date": collection_date.isoformat(),
        "blood_collection_time_slot": "10:00",
    }


async def _ensure_engagement_notifications(db, engagement_id: int, engagement_type_id: int) -> None:
    repo = EngagementNotificationsRepository()
    await repo.populate_from_defaults(db, engagement_id=engagement_id, engagement_type_id=engagement_type_id)


async def _run_onboard_phases(*, yes: bool) -> list[dict[str, Any]]:
    if not yes:
        return [{"phase": "onboard", "skipped": "pass --yes"}]
    today = _today_ist()
    tomorrow = today + timedelta(days=1)
    yesterday = today - timedelta(days=1)
    stamp = str(int(asyncio.get_event_loop().time() * 1000))[-6:]
    scenarios = [
        ("pretest", tomorrow, f"e2e-pretest-{stamp}@fitnastic.testmail.app"),
        ("qr-before", tomorrow, f"e2e-qrb-{stamp}@fitnastic.testmail.app"),
        ("qr-after", yesterday, f"e2e-qra-{stamp}@fitnastic.testmail.app"),
    ]
    results: list[dict[str, Any]] = []
    for label, coll_date, email in scenarios:
        payload = _onboard_payload(suffix=f"{label}-{stamp}", collection_date=coll_date, email=email)
        try:
            resp = _http_json("POST", "/users/public/onboard", payload)
            data = resp.get("data") or {}
            engagement_id = int(data["engagement_id"])
            user_id = int(data["user_id"])
            async with AsyncSessionLocal() as db:
                row = await db.execute(
                    select(EngagementParticipant).where(
                        EngagementParticipant.engagement_id == engagement_id,
                        EngagementParticipant.user_id == user_id,
                    )
                )
                ep = row.scalar_one_or_none()
                eng_type = (
                    await db.execute(
                        text(
                            "SELECT engagement_type FROM engagements WHERE engagement_id = :eid"
                        ),
                        {"eid": engagement_id},
                    )
                ).scalar_one_or_none()
                if ep is not None and eng_type is not None:
                    await _ensure_engagement_notifications(db, engagement_id, int(eng_type))
                await db.commit()
            results.append(
                {
                    "label": label,
                    "user_id": user_id,
                    "engagement_id": engagement_id,
                    "collection_date": coll_date.isoformat(),
                    "phone": payload["phone"],
                    "status": "onboarded",
                }
            )
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            results.append({"label": label, "status": "error", "http": exc.code, "body": body[:500]})
        except Exception as exc:
            results.append({"label": label, "status": "error", "error": str(exc)[:500]})
    return results


def _run_job(module: str, extra: list[str]) -> dict[str, Any]:
    import subprocess

    cmd = [sys.executable, "-m", module, "--yes", *extra]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd="/var/www/staging-api")
    return {
        "module": module,
        "exit_code": proc.returncode,
        "stdout": (proc.stdout or "")[-4000:],
        "stderr": (proc.stderr or "")[-2000:],
    }


async def _backfill_engagement_notifications() -> dict[str, Any]:
    from modules.engagements.models import Engagement

    updated = 0
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Engagement.engagement_id, Engagement.engagement_type))
        rows = list(result.all())
        repo = EngagementNotificationsRepository()
        for engagement_id, engagement_type in rows:
            if engagement_type is None:
                continue
            try:
                await repo.populate_from_defaults(
                    db,
                    engagement_id=int(engagement_id),
                    engagement_type_id=int(engagement_type),
                )
                updated += 1
            except Exception as exc:
                await db.rollback()
                logging.warning(
                    "populate_from_defaults skipped engagement_id=%s: %s",
                    engagement_id,
                    exc,
                )
        await db.commit()
    return {"engagements_backfilled": updated}


async def main() -> int:
    parser = argparse.ArgumentParser(description="Staging notification E2E runner")
    parser.add_argument("--yes", action="store_true", help="Perform onboard + cron dispatches")
    parser.add_argument(
        "--phase",
        default="onboard,pretest,questionnaire,consultation",
        help="Comma list: onboard,pretest,questionnaire,consultation,blood,bioai",
    )
    parser.add_argument("--as-of", default=None, help="YYYY-MM-DD for cron as-of (IST context)")
    args = parser.parse_args()
    phases = {p.strip() for p in args.phase.split(",") if p.strip()}
    report: dict[str, Any] = {"phases": phases, "as_of": args.as_of or _today_ist().isoformat()}

    if "sync" in phases and args.yes:
        report["sync_engagement_notifications"] = await _backfill_engagement_notifications()

    if "onboard" in phases:
        report["onboard"] = await _run_onboard_phases(yes=args.yes)

    extra_as_of = ["--as-of", args.as_of] if args.as_of else []

    if args.yes:
        if "pretest" in phases:
            report["pretest_cron"] = _run_job(
                "db.jobs.dispatch_pretest_reminders", extra_as_of
            )
        if "questionnaire" in phases:
            report["questionnaire_cron"] = _run_job(
                "db.jobs.dispatch_questionnaire_reminders", extra_as_of
            )
        if "consultation" in phases:
            report["consultation_ready_cron"] = _run_job(
                "db.jobs.dispatch_consultation_notifications", extra_as_of
            )
            report["consultation_reminder_cron"] = _run_job(
                "db.jobs.dispatch_consultation_remainder_notifications", extra_as_of
            )
        if "blood" in phases:
            report["load_blood"] = _run_job("db.jobs.all_engagements.load_blood_reports", [])
        if "bioai" in phases:
            report["load_bioai"] = _run_job("db.jobs.all_engagements.load_bioai_reports", [])

    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
