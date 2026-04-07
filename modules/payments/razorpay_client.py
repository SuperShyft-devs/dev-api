"""Razorpay SDK client (server-side only; secret never leaves this process)."""

from __future__ import annotations

import razorpay

from core.config import settings


def get_razorpay_client() -> razorpay.Client:
    """Build and return a Razorpay client using environment variables."""

    if not settings.RAZORPAY_KEY_ID or not settings.RAZORPAY_KEY_SECRET:
        raise RuntimeError("RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET must be set")
    return razorpay.Client(auth=(settings.RAZORPAY_KEY_ID, settings.RAZORPAY_KEY_SECRET))
