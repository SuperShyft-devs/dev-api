"""OTP provider abstraction.

We use an interface so we can switch to a real SMS provider later.
For now we ship a stub sender.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass


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
