"""Notifications repository.

Only database queries live here.
"""

from __future__ import annotations

from sqlalchemy import cast, delete, func, select, type_coerce, update as sql_update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.notifications.models import Notification, NotificationService
from modules.users.models import User


class NotificationsRepository:
    """Notification database queries."""

    # ── NotificationService CRUD ────────────────────────────────────────

    async def get_service_by_key(
        self, db: AsyncSession, *, service_key: str
    ) -> NotificationService | None:
        result = await db.execute(
            select(NotificationService).where(NotificationService.service_key == service_key)
        )
        return result.scalar_one_or_none()

    async def get_service_by_id(
        self, db: AsyncSession, *, notification_service_id: int
    ) -> NotificationService | None:
        result = await db.execute(
            select(NotificationService).where(
                NotificationService.notification_service_id == notification_service_id
            )
        )
        return result.scalar_one_or_none()

    async def list_services(self, db: AsyncSession) -> list[NotificationService]:
        result = await db.execute(
            select(NotificationService).order_by(NotificationService.notification_service_id.asc())
        )
        return list(result.scalars().all())

    async def create_service(
        self, db: AsyncSession, service: NotificationService
    ) -> NotificationService:
        db.add(service)
        await db.flush()
        return service

    async def update_service(
        self, db: AsyncSession, service: NotificationService
    ) -> NotificationService:
        db.add(service)
        await db.flush()
        return service

    async def delete_service(self, db: AsyncSession, *, notification_service_id: int) -> int:
        result = await db.execute(
            delete(NotificationService).where(
                NotificationService.notification_service_id == notification_service_id
            )
        )
        await db.flush()
        return int(result.rowcount or 0)

    async def count_notifications_for_service_key(
        self, db: AsyncSession, *, service_key: str
    ) -> int:
        result = await db.execute(
            select(func.count())
            .select_from(Notification)
            .where(Notification.service_key == service_key)
        )
        return int(result.scalar_one())

    # ── Notification CRUD ───────────────────────────────────────────────

    async def create_notification(
        self, db: AsyncSession, notification: Notification
    ) -> Notification:
        db.add(notification)
        await db.flush()
        return notification

    async def get_notification_by_id(
        self, db: AsyncSession, *, notification_id: int
    ) -> Notification | None:
        result = await db.execute(
            select(Notification).where(Notification.notification_id == notification_id)
        )
        return result.scalar_one_or_none()

    async def update_notification(
        self,
        db: AsyncSession,
        *,
        notification_id: int,
        values: dict,
    ) -> None:
        await db.execute(
            sql_update(Notification)
            .where(Notification.notification_id == notification_id)
            .values(**values)
        )
        await db.flush()

    async def delete_notification(self, db: AsyncSession, *, notification_id: int) -> int:
        result = await db.execute(
            delete(Notification).where(Notification.notification_id == notification_id)
        )
        await db.flush()
        return int(result.rowcount or 0)

    async def list_notifications(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        status: str | None = None,
        service_key: str | None = None,
        user_id: int | None = None,
        engagement_id: int | None = None,
    ) -> list[Notification]:
        offset = (page - 1) * limit
        query = (
            select(Notification)
            .order_by(Notification.notification_id.desc())
            .offset(offset)
            .limit(limit)
        )
        if status:
            query = query.where(Notification.status == status)
        if service_key:
            query = query.where(Notification.service_key == service_key)
        if user_id is not None:
            query = query.where(
                cast(Notification.user, JSONB)["user_ids"].contains(type_coerce([user_id], JSONB))
            )
        if engagement_id is not None:
            query = query.where(Notification.engagement_id == engagement_id)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def count_notifications(
        self,
        db: AsyncSession,
        *,
        status: str | None = None,
        service_key: str | None = None,
        user_id: int | None = None,
        engagement_id: int | None = None,
    ) -> int:
        query = select(func.count()).select_from(Notification)
        if status:
            query = query.where(Notification.status == status)
        if service_key:
            query = query.where(Notification.service_key == service_key)
        if user_id is not None:
            query = query.where(
                cast(Notification.user, JSONB)["user_ids"].contains(type_coerce([user_id], JSONB))
            )
        if engagement_id is not None:
            query = query.where(Notification.engagement_id == engagement_id)
        result = await db.execute(query)
        return int(result.scalar_one())

    # ── User & Assessment helpers ───────────────────────────────────────

    async def get_user_by_id(self, db: AsyncSession, *, user_id: int) -> User | None:
        result = await db.execute(select(User).where(User.user_id == user_id))
        return result.scalar_one_or_none()

    async def get_users_by_ids(self, db: AsyncSession, *, user_ids: list[int]) -> list[User]:
        if not user_ids:
            return []
        result = await db.execute(select(User).where(User.user_id.in_(user_ids)))
        return list(result.scalars().all())

    async def get_assessment_instance_by_record_id(
        self, db: AsyncSession, *, metsights_record_id: str
    ) -> AssessmentInstance | None:
        rid = (metsights_record_id or "").strip()
        if not rid:
            return None
        result = await db.execute(
            select(AssessmentInstance)
            .where(AssessmentInstance.metsights_record_id == rid)
            .limit(1)
        )
        return result.scalar_one_or_none()

    def _metsights_instance_base_query(self, *, user_id: int, engagement_id: int | None = None):
        """Shared filter for Metsights Basic/Pro instances with a record id."""
        metsights_type_codes = ("1", "2")
        query = (
            select(AssessmentInstance)
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .where(AssessmentInstance.user_id == user_id)
            .where(AssessmentInstance.metsights_record_id.isnot(None))
            .where(AssessmentInstance.metsights_record_id != "")
        )
        if engagement_id is not None:
            query = query.where(AssessmentInstance.engagement_id == engagement_id)
        return query, metsights_type_codes

    async def get_metsights_instance_for_user_engagement(
        self, db: AsyncSession, *, user_id: int, engagement_id: int
    ) -> AssessmentInstance | None:
        """Pick the latest Metsights Basic/Pro instance with a record id for user + engagement."""
        base, metsights_type_codes = self._metsights_instance_base_query(
            user_id=user_id, engagement_id=engagement_id
        )
        result = await db.execute(
            base.where(AssessmentPackage.assessment_type_code.in_(metsights_type_codes))
            .order_by(AssessmentInstance.assessment_instance_id.desc())
            .limit(1)
        )
        instance = result.scalar_one_or_none()
        if instance is not None:
            return instance
        result = await db.execute(
            base.where(func.coalesce(AssessmentPackage.assessment_type_code, "") != "7")
            .order_by(AssessmentInstance.assessment_instance_id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def get_latest_metsights_instance_for_user(
        self, db: AsyncSession, *, user_id: int
    ) -> AssessmentInstance | None:
        """Pick the latest Metsights Basic/Pro instance with a record id for the user."""
        base, metsights_type_codes = self._metsights_instance_base_query(user_id=user_id)
        result = await db.execute(
            base.where(AssessmentPackage.assessment_type_code.in_(metsights_type_codes))
            .order_by(AssessmentInstance.assessment_instance_id.desc())
            .limit(1)
        )
        instance = result.scalar_one_or_none()
        if instance is not None:
            return instance
        result = await db.execute(
            base.where(func.coalesce(AssessmentPackage.assessment_type_code, "") != "7")
            .order_by(AssessmentInstance.assessment_instance_id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()
