"""Support HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from db.session import get_db
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.support.schemas import SupportTicketCreate, SupportTicketStatusUpdate
from modules.support.service import SupportService
from modules.support.repository import SupportRepository


router = APIRouter(prefix="/support", tags=["support"])


def get_support_service() -> SupportService:
    audit_service = AuditService(AuditRepository())
    return SupportService(repository=SupportRepository(), audit_service=audit_service)


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


@router.post("/tickets", status_code=201)
async def submit_ticket(
    payload: SupportTicketCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    support_service: SupportService = Depends(get_support_service),
):
    ticket = await support_service.submit_ticket(
        db,
        data=payload,
        user_id=None,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response(
        {
            "ticket_id": ticket.ticket_id,
            "user_id": ticket.user_id,
            "contact_input": ticket.contact_input,
            "query_text": ticket.query_text,
            "status": ticket.status,
            "created_at": ticket.created_at,
        }
    )


@router.get("/tickets")
async def list_tickets(
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    support_service: SupportService = Depends(get_support_service),
):
    _ = employee
    tickets = await support_service.list_tickets(db, status_filter=status)

    return success_response(
        [
            {
                "ticket_id": ticket.ticket_id,
                "user_id": ticket.user_id,
                "contact_input": ticket.contact_input,
                "query_text": ticket.query_text,
                "status": ticket.status,
                "created_at": ticket.created_at,
            }
            for ticket in tickets
        ]
    )


@router.get("/tickets/{ticket_id}")
async def get_ticket(
    ticket_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    support_service: SupportService = Depends(get_support_service),
):
    _ = employee
    ticket = await support_service.get_ticket(db, ticket_id)

    return success_response(
        {
            "ticket_id": ticket.ticket_id,
            "user_id": ticket.user_id,
            "contact_input": ticket.contact_input,
            "query_text": ticket.query_text,
            "status": ticket.status,
            "created_at": ticket.created_at,
        }
    )


@router.patch("/tickets/{ticket_id}/status")
async def update_ticket_status(
    ticket_id: int,
    payload: SupportTicketStatusUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    support_service: SupportService = Depends(get_support_service),
):
    ticket = await support_service.change_ticket_status(
        db,
        ticket_id=ticket_id,
        status=payload.status,
        actor_user_id=employee.user_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"ticket_id": ticket.ticket_id, "status": ticket.status})
