"""Organizations module dependencies."""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.organizations.repository import OrganizationsRepository
from modules.organizations.service import OrganizationsService


def get_organizations_service() -> OrganizationsService:
    audit_service = AuditService(AuditRepository())
    return OrganizationsService(repository=OrganizationsRepository(), audit_service=audit_service)
