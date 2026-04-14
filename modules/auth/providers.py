"""OTP provider abstraction.

We use an interface so we can switch to a real SMS provider later.
For now we ship a stub sender.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from core.exceptions import AppError


logger = logging.getLogger(__name__)


class OtpSender:
    """OTP sender interface."""

    async def send_otp(self, phone: str, otp: str) -> None:  # pragma: no cover
        raise NotImplementedError


@dataclass
class StubOtpSender(OtpSender):
    """Fake OTP sender.

    It does not call any external service.
    """

    async def send_otp(self, phone: str, otp: str) -> None:
        return


@dataclass
class DevelopmentOtpSender(OtpSender):
    """Development OTP sender that logs OTP to terminal.
    
    This sender prints the OTP to the terminal with visual highlighting
    for easy identification during development and testing.
    
    ⚠️  NEVER use this in production!
    """

    async def send_otp(self, phone: str, otp: str) -> None:
        # ANSI color codes for highlighting
        BOLD = "\033[1m"
        GREEN = "\033[92m"
        YELLOW = "\033[93m"
        CYAN = "\033[96m"
        RESET = "\033[0m"
        BG_BLUE = "\033[44m"
        
        # Create a visually distinct separator
        separator = "=" * 70
        
        # Log with highlighting
        print(f"\n{YELLOW}{separator}{RESET}")
        print(f"{BG_BLUE}{BOLD}  🔐 DEVELOPMENT OTP  {RESET}")
        print(f"{YELLOW}{separator}{RESET}")
        print(f"{CYAN}Phone:{RESET} {phone}")
        print(f"{GREEN}{BOLD}OTP Code:{RESET} {BOLD}{otp}{RESET}")
        print(f"{YELLOW}{separator}{RESET}\n")
        
        # Also log it normally so it appears in log files
        logger.info(f"Development OTP sent for phone {phone[-4:].rjust(len(phone), '*')}")


@dataclass
class WhatApiOtpSender(OtpSender):
    """OTP sender backed by whatapi webhook."""

    webhook_url: str
    country_code: str = "91"
    timeout_seconds: float = 10.0

    def _normalize_number(self, phone: str) -> str:
        digits_only = "".join(ch for ch in phone if ch.isdigit())
        if not digits_only:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid phone number")

        cc = self.country_code
        if len(digits_only) == 10:
            return f"{cc}{digits_only}"

        if digits_only.startswith(cc) and len(digits_only) == len(cc) + 10:
            return digits_only

        if digits_only.startswith(cc):
            return digits_only

        return f"{cc}{digits_only}"

    async def send_otp(self, phone: str, otp: str) -> None:
        number = self._normalize_number(phone)
        params = {"number": number, "message": f"otp,{otp}"}

        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.get(self.webhook_url, params=params)
        except httpx.HTTPError as exc:
            logger.exception("WhatApi OTP webhook request failed")
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Unable to send OTP at the moment",
            ) from exc

        if response.status_code >= 400:
            logger.error(
                "WhatApi OTP webhook rejected request",
                extra={"status_code": response.status_code, "body": response.text[:500]},
            )
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Unable to send OTP at the moment",
            )

        payload: Any
        try:
            payload = response.json()
        except ValueError as exc:
            logger.error("WhatApi OTP webhook returned non-JSON response", extra={"body": response.text[:500]})
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Unable to send OTP at the moment",
            ) from exc

        if not isinstance(payload, dict) or payload.get("accepted") is not True:
            logger.error("WhatApi OTP webhook response not accepted", extra={"payload": payload})
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Unable to send OTP at the moment",
            )
