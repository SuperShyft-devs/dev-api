"""Persistence for platform settings."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.platform_settings.models import PlatformSettings

_PLATFORM_SETTINGS_PK = 1


class PlatformSettingsRepository:
    async def get_by_id(self, db: AsyncSession) -> PlatformSettings | None:
        result = await db.execute(select(PlatformSettings).where(PlatformSettings.settings_id == _PLATFORM_SETTINGS_PK))
        return result.scalar_one_or_none()

    async def upsert(
        self,
        db: AsyncSession,
        *,
        assessment_package_id: int,
        diagnostic_package_id: int,
        updated_by_user_id: int | None,
    ) -> PlatformSettings:
        existing = await self.get_by_id(db)
        if existing is None:
            row = PlatformSettings(
                settings_id=_PLATFORM_SETTINGS_PK,
                b2c_default_assessment_package_id=assessment_package_id,
                b2c_default_diagnostic_package_id=diagnostic_package_id,
                updated_by_user_id=updated_by_user_id,
            )
            db.add(row)
            await db.flush()
            return row

        existing.b2c_default_assessment_package_id = assessment_package_id
        existing.b2c_default_diagnostic_package_id = diagnostic_package_id
        existing.updated_by_user_id = updated_by_user_id
        await db.flush()
        return existing
