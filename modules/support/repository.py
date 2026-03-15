"""Support repository.

Only database queries live here.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.support.models import SupportTicket


class SupportRepository:
    """Support ticket database queries."""

    async def create_ticket(
        self,
        db: AsyncSession,
        data: dict,
        *,
        user_id: int | None = None,
    ) -> SupportTicket:
        ticket = SupportTicket(user_id=user_id, **data)
        db.add(ticket)
        await db.flush()
        return ticket

    async def get_all_tickets(
        self,
        db: AsyncSession,
        *,
        status: str | None = None,
    ) -> list[SupportTicket]:
        query = select(SupportTicket)
        if status is not None:
            query = query.where(SupportTicket.status == status)
        query = query.order_by(SupportTicket.ticket_id.desc())
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_ticket_by_id(self, db: AsyncSession, ticket_id: int) -> SupportTicket | None:
        result = await db.execute(select(SupportTicket).where(SupportTicket.ticket_id == ticket_id))
        return result.scalar_one_or_none()

    async def update_ticket_status(self, db: AsyncSession, ticket_id: int, status: str) -> SupportTicket | None:
        ticket = await self.get_ticket_by_id(db, ticket_id)
        if ticket is None:
            return None

        ticket.status = status
        db.add(ticket)
        await db.flush()
        return ticket
