"""Rate-limiting configuration backed by slowapi."""

from __future__ import annotations

from slowapi import Limiter
from slowapi.util import get_remote_address

from core.network import get_client_ip

limiter = Limiter(key_func=get_client_ip, default_limits=[])
