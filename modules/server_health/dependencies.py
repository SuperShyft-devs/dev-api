"""Server health module dependencies."""

from __future__ import annotations

from modules.server_health.repository import ServerHealthRepository
from modules.server_health.service import ServerHealthService


def get_server_health_service() -> ServerHealthService:
    return ServerHealthService(ServerHealthRepository())
