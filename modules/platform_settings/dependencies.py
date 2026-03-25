"""FastAPI dependencies for platform settings."""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.platform_settings.repository import PlatformSettingsRepository
from modules.platform_settings.service import PlatformSettingsService


def get_platform_settings_service() -> PlatformSettingsService:
    return PlatformSettingsService(
        repository=PlatformSettingsRepository(),
        audit_service=AuditService(AuditRepository()),
    )


def get_platform_settings_service_readonly() -> PlatformSettingsService:
    """For code paths that update audit elsewhere (e.g. public onboard)."""

    return PlatformSettingsService(repository=PlatformSettingsRepository(), audit_service=None)
