"""Users module dependencies."""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.engagements.dependencies import get_engagements_service
from modules.assessments.dependencies import get_assessments_service
from modules.users.repository import UsersRepository
from modules.users.service import UsersService


def get_users_service() -> UsersService:
    audit_service = AuditService(AuditRepository())
    engagements_service = get_engagements_service()
    assessments_service = get_assessments_service()
    return UsersService(
        repository=UsersRepository(),
        audit_service=audit_service,
        engagements_service=engagements_service,
        assessments_service=assessments_service,
    )
