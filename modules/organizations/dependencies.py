"""Organizations module dependencies."""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.employee.repository import EmployeeRepository
from modules.organizations.repository import OrganizationsRepository
from modules.organizations.service import OrganizationsService
from modules.users.repository import UsersRepository


def get_organizations_service() -> OrganizationsService:
    audit_service = AuditService(AuditRepository())
    return OrganizationsService(
        repository=OrganizationsRepository(),
        employee_repository=EmployeeRepository(),
        users_repository=UsersRepository(),
        audit_service=audit_service,
    )
