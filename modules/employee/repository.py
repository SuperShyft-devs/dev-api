"""Employee repository.

Only database queries live here.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import String, cast, delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from common.listing import apply_sort, ilike_pattern
from modules.employee.models import (
    Employee,
    EmployeeAuthOtpSession,
    EmployeeAuthToken,
    EmployeeCategoryPermission,
    EmployeeTaskPermission,
    PermissionCategory,
)


class EmployeeRepository:
    """Employee database queries."""

    _EMPLOYEE_SORT_COLUMNS = {
        "employee_id": Employee.employee_id,
        "name": Employee.name,
        "role": Employee.role,
        "status": Employee.status,
        "phone": Employee.phone,
        "email": Employee.email,
    }

    def _apply_employee_list_filters(
        self,
        query,
        *,
        status: str | None = None,
        role: str | None = None,
        search: str | None = None,
    ):
        if status is not None:
            query = query.where(Employee.status == status)
        if role is not None:
            query = query.where(Employee.role == role)
        if search is not None and search.strip():
            pattern = ilike_pattern(search)
            query = query.where(
                or_(
                    Employee.name.ilike(pattern),
                    Employee.phone.ilike(pattern),
                    Employee.email.ilike(pattern),
                    # role is a native PG enum; cast to text before ILIKE
                    cast(Employee.role, String).ilike(pattern),
                )
            )
        return query

    async def get_by_id(self, db: AsyncSession, employee_id: int) -> Optional[Employee]:
        result = await db.execute(select(Employee).where(Employee.employee_id == employee_id))
        return result.scalar_one_or_none()

    async def get_by_id_for_update(self, db: AsyncSession, employee_id: int) -> Optional[Employee]:
        result = await db.execute(
            select(Employee).where(Employee.employee_id == employee_id).with_for_update()
        )
        return result.scalar_one_or_none()

    async def get_by_phone(self, db: AsyncSession, phone: str) -> Optional[Employee]:
        result = await db.execute(select(Employee).where(Employee.phone == phone))
        return result.scalar_one_or_none()

    async def get_by_email(self, db: AsyncSession, email: str) -> Optional[Employee]:
        normalized = (email or "").strip().lower()
        if not normalized:
            return None
        result = await db.execute(select(Employee).where(func.lower(Employee.email) == normalized))
        return result.scalar_one_or_none()

    async def list_by_phone(self, db: AsyncSession, phone: str) -> list[Employee]:
        result = await db.execute(select(Employee).where(Employee.phone == phone))
        return list(result.scalars().all())

    async def list_permissions(
        self, db: AsyncSession, employee_id: int
    ) -> list[EmployeeCategoryPermission]:
        result = await db.execute(
            select(EmployeeCategoryPermission)
            .where(EmployeeCategoryPermission.employee_id == employee_id)
            .order_by(EmployeeCategoryPermission.category_key)
        )
        return list(result.scalars().all())

    async def list_active_categories(self, db: AsyncSession) -> list[PermissionCategory]:
        result = await db.execute(
            select(PermissionCategory)
            .where(PermissionCategory.is_active.is_(True))
            .order_by(PermissionCategory.display_order)
        )
        return list(result.scalars().all())

    async def list_task_permissions(
        self, db: AsyncSession, employee_id: int
    ) -> list[EmployeeTaskPermission]:
        result = await db.execute(
            select(EmployeeTaskPermission)
            .where(EmployeeTaskPermission.employee_id == employee_id)
            .order_by(
                EmployeeTaskPermission.category_key,
                EmployeeTaskPermission.task_key,
            )
        )
        return list(result.scalars().all())

    async def replace_permissions(
        self,
        db: AsyncSession,
        *,
        employee_id: int,
        grants: list[EmployeeCategoryPermission],
    ) -> None:
        await db.execute(
            delete(EmployeeCategoryPermission).where(
                EmployeeCategoryPermission.employee_id == employee_id
            )
        )
        db.add_all(grants)
        await db.flush()

    async def replace_task_permissions(
        self,
        db: AsyncSession,
        *,
        employee_id: int,
        grants: list[EmployeeTaskPermission],
    ) -> None:
        await db.execute(
            delete(EmployeeTaskPermission).where(
                EmployeeTaskPermission.employee_id == employee_id
            )
        )
        db.add_all(grants)
        await db.flush()

    async def bump_permissions_version(
        self, db: AsyncSession, *, employee_id: int, expected_version: int
    ) -> bool:
        result = await db.execute(
            update(Employee)
            .where(
                Employee.employee_id == employee_id,
                Employee.permissions_version == expected_version,
            )
            .values(permissions_version=Employee.permissions_version + 1)
        )
        return result.rowcount == 1

    async def count_active_admins(self, db: AsyncSession) -> int:
        result = await db.execute(
            select(func.count())
            .select_from(Employee)
            .where(Employee.role == "admin", func.lower(Employee.status) == "active")
        )
        return int(result.scalar_one())

    async def lock_active_admins(self, db: AsyncSession) -> list[Employee]:
        result = await db.execute(
            select(Employee)
            .where(Employee.role == "admin", func.lower(Employee.status) == "active")
            .order_by(Employee.employee_id)
            .with_for_update()
        )
        return list(result.scalars().all())

    async def count_employees(
        self,
        db: AsyncSession,
        *,
        status: str | None = None,
        role: str | None = None,
        search: str | None = None,
    ) -> int:
        query = select(func.count()).select_from(Employee)
        query = self._apply_employee_list_filters(
            query,
            status=status,
            role=role,
            search=search,
        )

        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_employees(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        status: str | None = None,
        role: str | None = None,
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> list[Employee]:
        offset = (page - 1) * limit

        query = select(Employee)
        query = self._apply_employee_list_filters(
            query,
            status=status,
            role=role,
            search=search,
        )
        query = apply_sort(
            query,
            sort_by=sort_by,
            sort_dir=sort_dir,
            columns=self._EMPLOYEE_SORT_COLUMNS,
            default_column=Employee.employee_id,
        )
        query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def create(self, db: AsyncSession, employee: Employee) -> Employee:
        db.add(employee)
        await db.flush()
        return employee

    async def update(self, db: AsyncSession, employee: Employee) -> Employee:
        db.add(employee)
        await db.flush()
        return employee

    # ── Auth OTP / refresh ──────────────────────────────────────────────

    async def create_otp_session(
        self, db: AsyncSession, session: EmployeeAuthOtpSession
    ) -> EmployeeAuthOtpSession:
        db.add(session)
        await db.flush()
        return session

    async def get_latest_otp_session(
        self, db: AsyncSession, employee_id: int
    ) -> Optional[EmployeeAuthOtpSession]:
        result = await db.execute(
            select(EmployeeAuthOtpSession)
            .where(EmployeeAuthOtpSession.employee_id == employee_id)
            .order_by(EmployeeAuthOtpSession.created_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def delete_otp_session(self, db: AsyncSession, session_id: int) -> None:
        await db.execute(
            delete(EmployeeAuthOtpSession).where(EmployeeAuthOtpSession.session_id == session_id)
        )

    async def delete_expired_otp_sessions(self, db: AsyncSession) -> None:
        now = datetime.now(timezone.utc)
        await db.execute(
            delete(EmployeeAuthOtpSession).where(EmployeeAuthOtpSession.otp_expires_at <= now)
        )

    async def delete_all_otp_sessions_for_employee(self, db: AsyncSession, employee_id: int) -> None:
        await db.execute(
            delete(EmployeeAuthOtpSession).where(EmployeeAuthOtpSession.employee_id == employee_id)
        )

    async def create_refresh_token(
        self, db: AsyncSession, token: EmployeeAuthToken
    ) -> EmployeeAuthToken:
        db.add(token)
        await db.flush()
        return token

    async def get_refresh_token_record(
        self, db: AsyncSession, token_id: int
    ) -> Optional[EmployeeAuthToken]:
        result = await db.execute(
            select(EmployeeAuthToken).where(EmployeeAuthToken.token_id == token_id)
        )
        return result.scalar_one_or_none()

    async def delete_refresh_token_record(self, db: AsyncSession, token_id: int) -> None:
        await db.execute(delete(EmployeeAuthToken).where(EmployeeAuthToken.token_id == token_id))

    async def update_refresh_token_hash(
        self, db: AsyncSession, token_id: int, refresh_token_hash: str
    ) -> None:
        await db.execute(
            update(EmployeeAuthToken)
            .where(EmployeeAuthToken.token_id == token_id)
            .values(refresh_token_hash=refresh_token_hash, issued_at=datetime.now(timezone.utc))
        )
