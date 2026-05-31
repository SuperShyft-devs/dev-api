"""Auth module dependencies.

This keeps router construction clean and testable.
"""

from __future__ import annotations

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.auth.repository import AuthRepository
from modules.auth.service import AuthService
from modules.notifications.dependencies import get_notifications_service
from modules.users.repository import UsersRepository
from modules.users.service import UsersService


def get_auth_service() -> AuthService:
    """Build AuthService with concrete dependencies."""
    users_repo = UsersRepository()
    users_service = UsersService(users_repo)

    audit_repo = AuditRepository()
    audit_service = AuditService(audit_repo)

    auth_repo = AuthRepository()

    return AuthService(
        repository=auth_repo,
        users_service=users_service,
        audit_service=audit_service,
        notifications_service=get_notifications_service(),
    )
