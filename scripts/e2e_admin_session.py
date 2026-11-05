"""Obtain admin session for local E2E (writes JSON path to stdout only)."""
from __future__ import annotations

import asyncio
import json
import re
import sys
from pathlib import Path

import requests
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.phone import phone_lookup_candidates
from core.config import settings
from db.session import AsyncSessionLocal
from modules.employee.models import Employee, EmployeeRole

BASE = "http://127.0.0.1:8000"
OUT = Path(__file__).resolve().parents[2] / "dev-admin" / ".e2e-session.json"


def _otp_for_phone(phone: str) -> str | None:
    if settings.ALLOW_BYPASS_OTP and settings.BYPASS_OTP:
        return settings.BYPASS_OTP
    index = settings.get_bypass_otp_by_phone()
    if not index:
        return None
    for candidate in phone_lookup_candidates(phone, strict=True):
        if candidate in index:
            return index[candidate]
    return None


async def _first_admin_phone() -> str | None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Employee.phone)
            .where(
                Employee.status == "active",
                Employee.role == EmployeeRole.admin,
                Employee.phone.is_not(None),
            )
            .order_by(Employee.employee_id)
            .limit(1)
        )
        row = result.scalar_one_or_none()
        return row


def main() -> int:
    phone = asyncio.run(_first_admin_phone())
    if not phone:
        print("NO_ADMIN_PHONE")
        return 1
    otp = _otp_for_phone(phone)
    if not otp:
        print("NO_BYPASS_OTP")
        return 1
    digits = re.sub(r"\D", "", phone)
    if len(digits) >= 10:
        login_phone = digits[-10:]
    else:
        login_phone = digits

    send = requests.post(
        f"{BASE}/employees/auth/send-otp",
        json={"phone": login_phone},
        timeout=60,
    )
    if not send.ok:
        print("SEND_FAILED", send.status_code)
        return 1
    body = send.json()
    sid = body.get("session_id") or (body.get("data") or {}).get("session_id")
    verify = requests.post(
        f"{BASE}/employees/auth/verify-otp",
        json={"phone": login_phone, "otp": otp, "session_id": sid},
        timeout=60,
    )
    if not verify.ok:
        print("VERIFY_FAILED", verify.status_code)
        return 1
    payload = verify.json()
    data = payload.get("data") or payload
    tokens = data.get("tokens") or {}
    access = tokens.get("access_token") or payload.get("access_token")
    refresh = tokens.get("refresh_token") or payload.get("refresh_token")
    if not access:
        print("NO_TOKEN")
        return 1
    me = requests.get(
        f"{BASE}/employees/auth/me",
        headers={"Authorization": f"Bearer {access}"},
        timeout=60,
    )
    profile = me.json().get("data") if me.ok else None
    OUT.write_text(
        json.dumps(
            {
                "access_token": access,
                "refresh_token": refresh,
                "profile": profile,
            }
        ),
        encoding="utf-8",
    )
    print("SESSION_WRITTEN", OUT.name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
