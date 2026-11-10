"""Central notification emit engine."""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagement_notifications.service_config import (
    extract_service_keys,
    get_external_link_for_service,
)
from modules.notifications.binding_resolver import BindingResolver
from modules.notifications.events_catalog import get_event
from modules.notifications.recipient_resolver import RecipientResolver
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import DispatchRequest, SessionDetails
from modules.notifications.service import NotificationsService

logger = logging.getLogger(__name__)


class NotificationEngine:
    def __init__(
        self,
        *,
        notifications_service: NotificationsService | None = None,
        notifications_repository: NotificationsRepository | None = None,
        binding_resolver: BindingResolver | None = None,
        recipient_resolver: RecipientResolver | None = None,
    ) -> None:
        repo = notifications_repository or NotificationsRepository()
        self._notifications_repository = repo
        self._notifications_service = notifications_service or NotificationsService(repository=repo)
        self._binding_resolver = binding_resolver or BindingResolver()
        self._recipient_resolver = recipient_resolver or RecipientResolver()

    async def emit(
        self,
        db: AsyncSession,
        *,
        event_code: str,
        context: dict,
        engagement_id: int | None = None,
        recipient_types: list[str] | None = None,
    ) -> list[int]:
        if get_event(event_code) is None:
            logger.warning("Notification emit skipped: unknown event_code=%s", event_code)
            return []

        bindings = await self._binding_resolver.resolve(
            db,
            event_code=event_code,
            engagement_id=engagement_id,
            context=context,
        )
        if recipient_types:
            allowed = set(recipient_types)
            bindings = [b for b in bindings if b.recipient_type in allowed]

        notification_ids: list[int] = []
        participant_details = context.get("participant_details")
        session_details = context.get("session_details")
        otp = context.get("otp")
        assessment_instance_id = context.get("assessment_instance_id")
        external_link = context.get("external_link")

        scoped_service_key = str(context.get("service_key") or "").strip() or None
        otp_notify_all = context.get("otp_notify_all_channels") is True

        for binding in bindings:
            binding_keys = extract_service_keys(binding.notification_services)
            if (
                scoped_service_key
                and not otp_notify_all
                and binding_keys
                and scoped_service_key not in binding_keys
            ):
                continue
            emit_context = dict(context)
            if binding.recipient_type == "custom" and binding.recipient_config:
                contacts: list[dict] = []
                for phone in binding.recipient_config.get("phones") or []:
                    p = str(phone).strip()
                    if p:
                        contacts.append({"phone": p})
                for email in binding.recipient_config.get("emails") or []:
                    e = str(email).strip()
                    if e:
                        contacts.append({"email": e})
                if contacts:
                    emit_context["custom_contacts"] = contacts
            target = await self._recipient_resolver.resolve(
                db,
                recipient_type=binding.recipient_type,
                context=emit_context,
                engagement_id=engagement_id,
            )
            if not target.user_ids and not target.contacts:
                continue

            for service_key in binding_keys:
                if (
                    scoped_service_key
                    and not otp_notify_all
                    and service_key != scoped_service_key
                ):
                    continue
                try:
                    svc = await self._notifications_repository.get_service_by_key(
                        db, service_key=service_key
                    )
                    if svc is None or not svc.is_active:
                        logger.warning(
                            "Emit skip: service_key=%s inactive (event=%s)",
                            service_key,
                            event_code,
                        )
                        continue

                    ext_link = get_external_link_for_service(
                        binding.notification_services, service_key
                    ) or external_link

                    if target.user_ids:
                        payload = DispatchRequest(
                            service_key=service_key,
                            user_ids=target.user_ids,
                            engagement_id=engagement_id,
                            participant_details=participant_details,
                            otp=str(otp) if otp is not None else None,
                            assessment_instance_id=assessment_instance_id,
                            external_link=ext_link,
                        )
                        if session_details is not None:
                            if isinstance(session_details, SessionDetails):
                                payload = payload.model_copy(
                                    update={"session_details": session_details}
                                )
                            elif isinstance(session_details, dict):
                                payload = payload.model_copy(
                                    update={
                                        "session_details": SessionDetails.model_validate(
                                            session_details
                                        )
                                    }
                                )
                        result = await self._notifications_service.dispatch(db, payload=payload)
                        nid = result.get("notification_id")
                        if nid is not None:
                            notification_ids.append(int(nid))
                    else:
                        result = await self._notifications_service.dispatch_to_contacts(
                            db,
                            service_key=service_key,
                            contacts=target.contacts,
                            otp=str(otp) if otp is not None else None,
                            engagement_id=engagement_id,
                            participant_details=participant_details,
                            session_details=(
                                session_details
                                if isinstance(session_details, SessionDetails)
                                else None
                            ),
                        )
                        nid = result.get("notification_id")
                        if nid is not None:
                            notification_ids.append(int(nid))
                except Exception as exc:
                    logger.warning(
                        "Notification emit failed event=%s service=%s: %s",
                        event_code,
                        service_key,
                        exc,
                    )
        return notification_ids


_engine: NotificationEngine | None = None


def get_notification_engine() -> NotificationEngine:
    global _engine
    if _engine is None:
        from modules.notifications.dependencies import get_notifications_service

        svc = get_notifications_service()
        _engine = NotificationEngine(
            notifications_service=svc,
            notifications_repository=svc._repo,
        )
    return _engine


def reset_notification_engine() -> None:
    """Clear singleton (tests only)."""
    global _engine
    _engine = None


async def emit_system_error(
    db: AsyncSession,
    *,
    exc: BaseException,
    api_path: str,
    context: dict | None = None,
) -> list[int]:
    ctx = dict(context or {})
    ctx.setdefault("error", {"error_message": str(exc), "api_path": api_path})
    return await get_notification_engine().emit(db, event_code="system.error", context=ctx)
