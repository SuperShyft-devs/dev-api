"""Persistence for platform settings."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.platform_settings.models import PlatformSettings

_PLATFORM_SETTINGS_PK = 1


def parse_comma_separated_employee_ids(raw: str | None) -> list[int]:
    if not raw:
        return []
    ids: list[int] = []
    seen: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            emp_id = int(part)
        except ValueError:
            continue
        if emp_id <= 0 or emp_id in seen:
            continue
        seen.add(emp_id)
        ids.append(emp_id)
    return ids


def serialize_comma_separated_employee_ids(employee_ids: list[int]) -> str | None:
    if not employee_ids:
        return None
    seen: set[int] = set()
    normalized: list[str] = []
    for raw in employee_ids:
        if not isinstance(raw, int) or raw <= 0 or raw in seen:
            continue
        seen.add(raw)
        normalized.append(str(raw))
    return ",".join(normalized) if normalized else None


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

    async def upsert_notification_defaults(
        self,
        db: AsyncSession,
        *,
        default_onboarding_notification: str | None,
        default_pretest_guidelines_notification: str | None,
        default_questionnaire_reminder_1: str | None,
        default_questionnaire_reminder_2: str | None,
        default_blood_report_notification: str | None,
        default_bioai_report_notification: str | None,
        updated_by_user_id: int | None,
        assessment_package_id: int,
        diagnostic_package_id: int,
    ) -> PlatformSettings:
        existing = await self.get_by_id(db)
        if existing is None:
            row = PlatformSettings(
                settings_id=_PLATFORM_SETTINGS_PK,
                b2c_default_assessment_package_id=assessment_package_id,
                b2c_default_diagnostic_package_id=diagnostic_package_id,
                default_onboarding_notification=default_onboarding_notification,
                default_pretest_guidelines_notification=default_pretest_guidelines_notification,
                default_questionnaire_reminder_1=default_questionnaire_reminder_1,
                default_questionnaire_reminder_2=default_questionnaire_reminder_2,
                default_blood_report_notification=default_blood_report_notification,
                default_bioai_report_notification=default_bioai_report_notification,
                updated_by_user_id=updated_by_user_id,
            )
            db.add(row)
            await db.flush()
            return row

        existing.default_onboarding_notification = default_onboarding_notification
        existing.default_pretest_guidelines_notification = default_pretest_guidelines_notification
        existing.default_questionnaire_reminder_1 = default_questionnaire_reminder_1
        existing.default_questionnaire_reminder_2 = default_questionnaire_reminder_2
        existing.default_blood_report_notification = default_blood_report_notification
        existing.default_bioai_report_notification = default_bioai_report_notification
        existing.updated_by_user_id = updated_by_user_id
        await db.flush()
        return existing

    async def upsert_default_onboarding_assistants(
        self,
        db: AsyncSession,
        *,
        default_onboarding_assistant_employee_ids: str | None,
        updated_by_user_id: int | None,
        assessment_package_id: int,
        diagnostic_package_id: int,
    ) -> PlatformSettings:
        existing = await self.get_by_id(db)
        if existing is None:
            row = PlatformSettings(
                settings_id=_PLATFORM_SETTINGS_PK,
                b2c_default_assessment_package_id=assessment_package_id,
                b2c_default_diagnostic_package_id=diagnostic_package_id,
                default_onboarding_assistant_employee_ids=default_onboarding_assistant_employee_ids,
                updated_by_user_id=updated_by_user_id,
            )
            db.add(row)
            await db.flush()
            return row

        existing.default_onboarding_assistant_employee_ids = default_onboarding_assistant_employee_ids
        existing.updated_by_user_id = updated_by_user_id
        await db.flush()
        return existing

    async def resolve_default_onboarding_assistant_employee_ids(self, db: AsyncSession) -> list[int]:
        row = await self.get_by_id(db)
        if row is None:
            return []
        return parse_comma_separated_employee_ids(row.default_onboarding_assistant_employee_ids)
