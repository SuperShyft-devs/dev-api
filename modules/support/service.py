"""Support service.

This module owns support business rules.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.models import Employee
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.platform_settings.repository import PlatformSettingsRepository
from modules.support.models import SupportTicket
from modules.support.repository import SupportRepository
from modules.support.schemas import SupportTicketCreate
from modules.support.serialization import ticket_to_dict
from modules.users.repository import UsersRepository


_ALLOWED_STATUS = {"open", "resolved"}


class SupportService:
    """Support service layer."""

    def __init__(
        self,
        repository: SupportRepository,
        audit_service: AuditService | None = None,
        users_repository: UsersRepository | None = None,
        notifications_service: NotificationsService | None = None,
        notifications_repository: NotificationsRepository | None = None,
        platform_settings_repository: PlatformSettingsRepository | None = None,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._users_repository = users_repository or UsersRepository()
        self._notifications_service = notifications_service
        self._notifications_repository = notifications_repository or NotificationsRepository()
        self._platform_settings_repository = platform_settings_repository or PlatformSettingsRepository()

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    def _normalize_status(self, value: str | None) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in _ALLOWED_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        return normalized

    def _normalize_resolution_notes(self, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        return stripped or None

    def _contact_input_from_user(self, user, user_id: int) -> str:
        for value in (user.phone, user.email):
            normalized = str(value or "").strip()
            if normalized:
                return normalized[:255]

        return str(user_id)

    async def _employee_names(self, db: AsyncSession, employee_ids: set[int]) -> dict[int, str]:
        if not employee_ids:
            return {}
        result = await db.execute(select(Employee).where(Employee.employee_id.in_(employee_ids)))
        return {row.employee_id: row.name for row in result.scalars().all()}

    async def tickets_to_responses(
        self,
        db: AsyncSession,
        tickets: list[SupportTicket],
    ) -> list[dict]:
        employee_ids = {
            int(ticket.resolved_by_employee_id)
            for ticket in tickets
            if ticket.resolved_by_employee_id is not None
        }
        names = await self._employee_names(db, employee_ids)
        return [
            ticket_to_dict(
                ticket,
                resolved_by_employee_name=(
                    names.get(int(ticket.resolved_by_employee_id))
                    if ticket.resolved_by_employee_id is not None
                    else None
                ),
            )
            for ticket in tickets
        ]

    async def ticket_to_response(self, db: AsyncSession, ticket: SupportTicket) -> dict:
        responses = await self.tickets_to_responses(db, [ticket])
        return responses[0]

    async def submit_ticket(
        self,
        db: AsyncSession,
        *,
        data: SupportTicketCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> SupportTicket:
        user_id = data.user_id
        user = await self._users_repository.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        contact_input = self._contact_input_from_user(user, user_id)

        ticket = await self._repository.create_ticket(
            db,
            data={
                "query_text": data.query_text,
                "contact_input": contact_input,
            },
            user_id=user_id,
        )

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="SUPPORT_SUBMIT_TICKET",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        if self._notifications_service is not None:
            from modules.notifications.support_notify import (
                notify_default_onboarding_assistants_on_support_query,
            )

            await notify_default_onboarding_assistants_on_support_query(
                db,
                notifications_service=self._notifications_service,
                notifications_repository=self._notifications_repository,
                platform_settings_repository=self._platform_settings_repository,
                ticket=ticket,
                user=user,
            )

        return ticket

    async def list_tickets(
        self,
        db: AsyncSession,
        *,
        status_filter: str | None = None,
        limit: int | None = None,
    ) -> list[SupportTicket]:
        normalized_status = self._normalize_status(status_filter) if status_filter is not None else None
        return await self._repository.get_all_tickets(db, status=normalized_status, limit=limit)

    async def get_ticket(self, db: AsyncSession, ticket_id: int) -> SupportTicket:
        ticket = await self._repository.get_ticket_by_id(db, ticket_id)
        if ticket is None:
            raise AppError(status_code=404, error_code="SUPPORT_TICKET_NOT_FOUND", message="Ticket does not exist")
        return ticket

    async def change_ticket_status(
        self,
        db: AsyncSession,
        *,
        ticket_id: int,
        status: str,
        actor_employee_id: int,
        resolution_notes: str | None = None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> SupportTicket:
        normalized_status = self._normalize_status(status)
        now = datetime.now(timezone.utc)

        if normalized_status == "open":
            ticket = await self._repository.update_ticket_status(
                db,
                ticket_id,
                normalized_status,
                clear_resolution=True,
            )
        else:
            notes = self._normalize_resolution_notes(resolution_notes)
            ticket = await self._repository.update_ticket_status(
                db,
                ticket_id,
                normalized_status,
                resolved_by_employee_id=actor_employee_id,
                resolved_at=now,
                resolution_notes=notes,
                update_resolution_notes=True,
            )

        if ticket is None:
            raise AppError(status_code=404, error_code="SUPPORT_TICKET_NOT_FOUND", message="Ticket does not exist")

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="SUPPORT_UPDATE_TICKET_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return ticket
