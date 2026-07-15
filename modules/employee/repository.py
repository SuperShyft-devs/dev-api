"""Employee repository.

Only database queries live here.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import String, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from common.listing import apply_sort, ilike_pattern
from modules.employee.models import Employee
from modules.users.models import User


class EmployeeRepository:
    """Employee database queries."""

    _EMPLOYEE_SORT_COLUMNS = {
        "employee_id": Employee.employee_id,
        "user_id": Employee.user_id,
        "role": Employee.role,
        "status": Employee.status,
        "first_name": User.first_name,
        "last_name": User.last_name,
    }

    def _apply_employee_list_filters(
        self,
        query,
        *,
        status: str | None = None,
        role: str | None = None,
        user_id: int | None = None,
        search: str | None = None,
    ):
        if status is not None:
            query = query.where(Employee.status == status)
        if role is not None:
            query = query.where(Employee.role == role)
        if user_id is not None:
            query = query.where(Employee.user_id == user_id)
        if search is not None and search.strip():
            pattern = ilike_pattern(search)
            full_name = func.trim(func.concat(func.coalesce(User.first_name, ""), " ", func.coalesce(User.last_name, "")))
            query = query.where(
                or_(
                    User.first_name.ilike(pattern),
                    User.last_name.ilike(pattern),
                    full_name.ilike(pattern),
                    # role is a native PG enum; cast to text before ILIKE
                    cast(Employee.role, String).ilike(pattern),
                )
            )
        return query

    async def get_by_user_id(self, db: AsyncSession, user_id: int) -> Optional[Employee]:
        result = await db.execute(select(Employee).where(Employee.user_id == user_id))
        return result.scalar_one_or_none()

    async def get_by_id(self, db: AsyncSession, employee_id: int) -> Optional[Employee]:
        result = await db.execute(select(Employee).where(Employee.employee_id == employee_id))
        return result.scalar_one_or_none()

    async def get_by_id_with_user_names(
        self, db: AsyncSession, employee_id: int
    ) -> tuple[Employee, str | None, str | None] | None:
        result = await db.execute(
            select(Employee, User.first_name, User.last_name)
            .join(User, User.user_id == Employee.user_id)
            .where(Employee.employee_id == employee_id)
        )
        row = result.one_or_none()
        if row is None:
            return None
        emp, first_name, last_name = row
        return emp, first_name, last_name

    async def count_employees(
        self,
        db: AsyncSession,
        *,
        status: str | None = None,
        role: str | None = None,
        user_id: int | None = None,
        search: str | None = None,
    ) -> int:
        query = select(func.count()).select_from(Employee).join(User, User.user_id == Employee.user_id)
        query = self._apply_employee_list_filters(
            query,
            status=status,
            role=role,
            user_id=user_id,
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
        user_id: int | None = None,
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> list[tuple[Employee, str | None, str | None]]:
        offset = (page - 1) * limit

        query = select(Employee, User.first_name, User.last_name).join(
            User, User.user_id == Employee.user_id
        )
        query = self._apply_employee_list_filters(
            query,
            status=status,
            role=role,
            user_id=user_id,
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
        return list(result.all())

    async def create(self, db: AsyncSession, employee: Employee) -> Employee:
        db.add(employee)
        await db.flush()
        return employee

    async def update(self, db: AsyncSession, employee: Employee) -> Employee:
        db.add(employee)
        await db.flush()
        return employee
