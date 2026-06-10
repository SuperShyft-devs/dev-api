"""Support service.

This module owns support business rules.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.support.models import SupportTicket
from modules.support.repository import SupportRepository
from modules.support.schemas import SupportTicketCreate
from modules.users.repository import UsersRepository


_ALLOWED_STATUS = {"open", "resolved", "closed"}


class SupportService:
    """Support service layer."""

    def __init__(
        self,
        repository: SupportRepository,
        audit_service: AuditService | None = None,
        users_repository: UsersRepository | None = None,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._users_repository = users_repository or UsersRepository()

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    def _normalize_status(self, value: str | None) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in _ALLOWED_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        return normalized

    async def _resolve_contact_input(self, db: AsyncSession, user_id: int) -> str:
        user = await self._users_repository.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        for value in (user.phone, user.email):
            normalized = str(value or "").strip()
            if normalized:
                return normalized[:255]

        return str(user_id)

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
        contact_input = await self._resolve_contact_input(db, user_id)

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
        return ticket

    async def list_tickets(self, db: AsyncSession, *, status_filter: str | None = None) -> list[SupportTicket]:
        normalized_status = self._normalize_status(status_filter) if status_filter is not None else None
        return await self._repository.get_all_tickets(db, status=normalized_status)

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
        actor_user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> SupportTicket:
        normalized_status = self._normalize_status(status)
        ticket = await self._repository.update_ticket_status(db, ticket_id, normalized_status)
        if ticket is None:
            raise AppError(status_code=404, error_code="SUPPORT_TICKET_NOT_FOUND", message="Ticket does not exist")

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="SUPPORT_UPDATE_TICKET_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=actor_user_id,
            session_id=None,
        )
        return ticket
