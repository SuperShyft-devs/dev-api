"""Partners module dependencies."""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.notifications.dependencies import get_notifications_service
from modules.partners.auth_service import PartnerAuthService
from modules.partners.repository import PartnersRepository
from modules.partners.service import PartnersService


def get_partners_service() -> PartnersService:
    return PartnersService(
        repository=PartnersRepository(),
        audit_service=AuditService(AuditRepository()),
    )


def get_partner_auth_service() -> PartnerAuthService:
    return PartnerAuthService(
        repository=PartnersRepository(),
        audit_service=AuditService(AuditRepository()),
        notifications_service=get_notifications_service(),
    )
