"""Persistence helpers for discount codes."""

from __future__ import annotations

import secrets
import string
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from modules.discounts.models import (
    DiscountAllowlistUser,
    DiscountCode,
    DiscountCodeAudit,
    DiscountCodeCity,
    DiscountCodeInstance,
    DiscountCodePackage,
    DiscountCodeScope,
    DiscountUsage,
)
from modules.discounts.schemas import DiscountCodeCreate, DiscountCodeUpdate, normalize_code


class DiscountRepository:
    async def get_by_id(self, db: AsyncSession, discount_code_id: int) -> DiscountCode | None:
        result = await db.execute(
            select(DiscountCode)
            .options(
                selectinload(DiscountCode.scopes),
                selectinload(DiscountCode.packages),
                selectinload(DiscountCode.cities),
                selectinload(DiscountCode.allowlist),
            )
            .where(DiscountCode.discount_code_id == discount_code_id)
        )
        return result.scalar_one_or_none()

    async def get_by_code(self, db: AsyncSession, code: str) -> DiscountCode | None:
        result = await db.execute(
            select(DiscountCode)
            .options(
                selectinload(DiscountCode.scopes),
                selectinload(DiscountCode.packages),
                selectinload(DiscountCode.cities),
            )
            .where(DiscountCode.code == normalize_code(code))
        )
        return result.scalar_one_or_none()

    async def list_codes(
        self,
        db: AsyncSession,
        *,
        status: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[DiscountCode], int]:
        filters = []
        if status:
            filters.append(DiscountCode.status == status)
        if search:
            q = f"%{search.strip()}%"
            filters.append(
                (DiscountCode.code.ilike(q)) | (DiscountCode.name.ilike(q))
            )
        count_stmt = select(func.count(DiscountCode.discount_code_id))
        if filters:
            count_stmt = count_stmt.where(*filters)
        total = int((await db.execute(count_stmt)).scalar() or 0)

        stmt = (
            select(DiscountCode)
            .options(
                selectinload(DiscountCode.scopes),
                selectinload(DiscountCode.packages),
                selectinload(DiscountCode.cities),
            )
            .order_by(DiscountCode.discount_code_id.desc())
            .limit(limit)
            .offset(offset)
        )
        if filters:
            stmt = stmt.where(*filters)
        rows = list((await db.execute(stmt)).scalars().all())
        return rows, total

    async def create(self, db: AsyncSession, payload: DiscountCodeCreate, *, actor_id: int) -> DiscountCode:
        row = DiscountCode(
            code=payload.code,
            name=payload.name,
            description=payload.description,
            status=payload.status,
            discount_type=payload.discount_type,
            percent_value=payload.percent_value,
            fixed_amount_paise=payload.fixed_amount_paise,
            max_discount_paise=payload.max_discount_paise,
            hard_ceiling_paise=payload.hard_ceiling_paise,
            min_bill_paise=payload.min_bill_paise,
            combine_with_others=payload.combine_with_others,
            auto_apply=payload.auto_apply,
            audience=payload.audience,
            first_purchase_only=payload.first_purchase_only,
            scope_mode=payload.scope_mode,
            package_apply_mode=payload.package_apply_mode,
            include_addons=payload.include_addons,
            valid_from=payload.valid_from,
            valid_to=payload.valid_to,
            time_of_day_start=payload.time_of_day_start,
            time_of_day_end=payload.time_of_day_end,
            days_of_week=payload.days_of_week,
            camp_valid_days_after=payload.camp_valid_days_after,
            max_total_uses=payload.max_total_uses,
            max_uses_per_user=payload.max_uses_per_user,
            per_user_frequency=payload.per_user_frequency,
            max_uses_per_camp=payload.max_uses_per_camp,
            max_uses_per_order=payload.max_uses_per_order,
            max_total_discount_paise=payload.max_total_discount_paise,
            code_kind=payload.code_kind,
            referral_user_id=payload.referral_user_id,
            min_price_protection=payload.min_price_protection,
            created_by=actor_id,
            updated_by=actor_id,
        )
        db.add(row)
        await db.flush()
        await self._replace_scopes(db, row, payload.scope_mode, payload.scope_keys)
        await self._replace_packages(db, row, payload.package_apply_mode, payload.package_ids)
        await self._replace_cities(db, row, payload.cities)
        await self.add_audit(db, row.discount_code_id, actor_id, "create", {"code": row.code})
        return row

    async def update(
        self, db: AsyncSession, row: DiscountCode, payload: DiscountCodeUpdate, *, actor_id: int
    ) -> DiscountCode:
        data = payload.model_dump(exclude_unset=True)
        scope_keys = data.pop("scope_keys", None)
        package_ids = data.pop("package_ids", None)
        cities = data.pop("cities", None)
        for key, value in data.items():
            setattr(row, key, value)
        row.updated_by = actor_id
        if "scope_mode" in data or scope_keys is not None:
            await self._replace_scopes(
                db, row, row.scope_mode, scope_keys if scope_keys is not None else [s.scope_key for s in row.scopes]
            )
        if "package_apply_mode" in data or package_ids is not None:
            await self._replace_packages(
                db,
                row,
                row.package_apply_mode,
                package_ids
                if package_ids is not None
                else [p.diagnostic_package_id for p in row.packages],
            )
        if cities is not None:
            await self._replace_cities(db, row, cities)
        await self.add_audit(db, row.discount_code_id, actor_id, "update", data)
        await db.flush()
        return row

    async def _replace_scopes(
        self, db: AsyncSession, row: DiscountCode, scope_mode: str, scope_keys: list[str]
    ) -> None:
        await db.execute(delete(DiscountCodeScope).where(DiscountCodeScope.discount_code_id == row.discount_code_id))
        if scope_mode == "general":
            return
        for key in scope_keys:
            db.add(
                DiscountCodeScope(
                    discount_code_id=row.discount_code_id,
                    scope_type=scope_mode,
                    scope_key=str(key).strip(),
                )
            )

    async def _replace_packages(
        self, db: AsyncSession, row: DiscountCode, mode: str, package_ids: list[int]
    ) -> None:
        await db.execute(
            delete(DiscountCodePackage).where(DiscountCodePackage.discount_code_id == row.discount_code_id)
        )
        if mode == "all":
            return
        pkg_mode = "include" if mode == "include" else "exclude"
        for pid in package_ids:
            db.add(
                DiscountCodePackage(
                    discount_code_id=row.discount_code_id,
                    diagnostic_package_id=pid,
                    mode=pkg_mode,
                )
            )

    async def _replace_cities(self, db: AsyncSession, row: DiscountCode, cities: list[str]) -> None:
        await db.execute(delete(DiscountCodeCity).where(DiscountCodeCity.discount_code_id == row.discount_code_id))
        for city in cities:
            cleaned = (city or "").strip()
            if cleaned:
                db.add(DiscountCodeCity(discount_code_id=row.discount_code_id, city=cleaned))

    async def add_audit(
        self,
        db: AsyncSession,
        discount_code_id: int,
        actor_id: int | None,
        action: str,
        diff: dict[str, Any] | None = None,
    ) -> None:
        db.add(
            DiscountCodeAudit(
                discount_code_id=discount_code_id,
                actor_user_id=actor_id,
                action=action,
                diff=diff,
            )
        )

    async def list_audit(self, db: AsyncSession, discount_code_id: int, limit: int = 50) -> list[DiscountCodeAudit]:
        result = await db.execute(
            select(DiscountCodeAudit)
            .where(DiscountCodeAudit.discount_code_id == discount_code_id)
            .order_by(DiscountCodeAudit.audit_id.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def bulk_instances(
        self, db: AsyncSession, discount_code_id: int, *, count: int, prefix: str | None
    ) -> list[DiscountCodeInstance]:
        alphabet = string.ascii_uppercase + string.digits
        created: list[DiscountCodeInstance] = []
        prefix = (prefix or "").strip().upper()
        for _ in range(count):
            for _attempt in range(20):
                suffix = "".join(secrets.choice(alphabet) for _ in range(8))
                code = f"{prefix}{suffix}" if prefix else suffix
                exists = await db.execute(
                    select(DiscountCodeInstance.instance_id).where(DiscountCodeInstance.code == code).limit(1)
                )
                parent_clash = await db.execute(
                    select(DiscountCode.discount_code_id).where(DiscountCode.code == code).limit(1)
                )
                if exists.scalar_one_or_none() is None and parent_clash.scalar_one_or_none() is None:
                    inst = DiscountCodeInstance(discount_code_id=discount_code_id, code=code, status="available")
                    db.add(inst)
                    created.append(inst)
                    break
            else:
                raise ValueError("Could not generate unique instance codes")
        await db.flush()
        return created

    async def list_instances(
        self, db: AsyncSession, discount_code_id: int, *, limit: int = 5000
    ) -> list[DiscountCodeInstance]:
        result = await db.execute(
            select(DiscountCodeInstance)
            .where(DiscountCodeInstance.discount_code_id == discount_code_id)
            .order_by(DiscountCodeInstance.instance_id.asc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def add_allowlist(
        self, db: AsyncSession, discount_code_id: int, entries: list[dict[str, Optional[str]]]
    ) -> int:
        added = 0
        for entry in entries:
            phone = (entry.get("phone") or "").strip() or None
            email = (entry.get("email") or "").strip().lower() or None
            if not phone and not email:
                continue
            db.add(
                DiscountAllowlistUser(
                    discount_code_id=discount_code_id,
                    phone=phone,
                    email=email,
                )
            )
            added += 1
        await db.flush()
        return added

    async def list_auto_apply(self, db: AsyncSession) -> list[DiscountCode]:
        result = await db.execute(
            select(DiscountCode)
            .options(
                selectinload(DiscountCode.scopes),
                selectinload(DiscountCode.packages),
                selectinload(DiscountCode.cities),
            )
            .where(DiscountCode.auto_apply.is_(True), DiscountCode.status == "active")
            .order_by(DiscountCode.discount_code_id.desc())
            .limit(50)
        )
        return list(result.scalars().all())

    async def release_stale_reservations(self, db: AsyncSession, *, older_than_minutes: int = 30) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=older_than_minutes)
        result = await db.execute(
            select(DiscountUsage).where(
                DiscountUsage.status == "reserved",
                DiscountUsage.created_at < cutoff,
            )
        )
        rows = list(result.scalars().all())
        for row in rows:
            row.status = "released"
        await db.flush()
        return len(rows)

    async def find_open_reservation(
        self, db: AsyncSession, *, user_id: int, discount_code_id: int
    ) -> DiscountUsage | None:
        result = await db.execute(
            select(DiscountUsage)
            .where(
                DiscountUsage.user_id == user_id,
                DiscountUsage.discount_code_id == discount_code_id,
                DiscountUsage.status == "reserved",
            )
            .order_by(DiscountUsage.usage_id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def usage_stats(self, db: AsyncSession, discount_code_id: int) -> dict[str, Any]:
        committed = await db.execute(
            select(
                func.count(DiscountUsage.usage_id),
                func.coalesce(func.sum(DiscountUsage.discount_paise), 0),
                func.coalesce(func.sum(DiscountUsage.final_paise), 0),
            ).where(
                DiscountUsage.discount_code_id == discount_code_id,
                DiscountUsage.status == "committed",
            )
        )
        uses, discount_sum, revenue = committed.one()
        reserved = await db.execute(
            select(func.count(DiscountUsage.usage_id)).where(
                DiscountUsage.discount_code_id == discount_code_id,
                DiscountUsage.status == "reserved",
            )
        )
        return {
            "committed_uses": int(uses or 0),
            "reserved_uses": int(reserved.scalar() or 0),
            "total_discount_paise": int(discount_sum or 0),
            "revenue_after_discount_paise": int(revenue or 0),
        }
