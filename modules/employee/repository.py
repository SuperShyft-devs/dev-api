"""Employee repository.

Only database queries live here.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.employee.models import Employee


class EmployeeRepository:
    """Employee database queries."""

    async def get_by_user_id(self, db: AsyncSession, user_id: int) -> Optional[Employee]:
        result = await db.execute(select(Employee).where(Employee.user_id == user_id))
        return result.scalar_one_or_none()

    async def get_by_id(self, db: AsyncSession, employee_id: int) -> Optional[Employee]:
        result = await db.execute(select(Employee).where(Employee.employee_id == employee_id))
        return result.scalar_one_or_none()

    async def count_employees(
        self,
        db: AsyncSession,
        *,
        status: str | None = None,
        role: str | None = None,
        user_id: int | None = None,
    ) -> int:
        from sqlalchemy import func

        query = select(func.count()).select_from(Employee)
        if status is not None:
            query = query.where(Employee.status == status)
        if role is not None:
            query = query.where(Employee.role == role)
        if user_id is not None:
            query = query.where(Employee.user_id == user_id)

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
    ) -> list[Employee]:
        offset = (page - 1) * limit

        query = select(Employee)
        if status is not None:
            query = query.where(Employee.status == status)
        if role is not None:
            query = query.where(Employee.role == role)
        if user_id is not None:
            query = query.where(Employee.user_id == user_id)

        query = query.order_by(Employee.employee_id.desc()).offset(offset).limit(limit)
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
