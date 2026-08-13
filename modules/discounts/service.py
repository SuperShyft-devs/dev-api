"""Discount module application service."""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.diagnostics.models import DiagnosticPackage
from modules.discounts import abuse
from modules.discounts.engine import DiscountEngine, PUBLIC_INVALID
from modules.discounts.models import DiscountCode, DiscountCodeInstance, DiscountUsage
from modules.discounts.repository import DiscountRepository
from modules.discounts.schemas import (
    CartLine,
    CheckoutContext,
    DiscountCodeCreate,
    DiscountCodeOut,
    DiscountCodeUpdate,
    EvaluateResult,
    normalize_code,
)
from modules.engagements.models import Engagement
from modules.organizations.models import Organization
from modules.users.models import User

logger = logging.getLogger(__name__)


def serialize_code(row: DiscountCode) -> dict[str, Any]:
    return DiscountCodeOut(
        discount_code_id=row.discount_code_id,
        code=row.code,
        name=row.name,
        description=row.description,
        status=row.status,
        discount_type=row.discount_type,
        percent_value=row.percent_value,
        fixed_amount_paise=row.fixed_amount_paise,
        max_discount_paise=row.max_discount_paise,
        hard_ceiling_paise=row.hard_ceiling_paise,
        min_bill_paise=row.min_bill_paise,
        combine_with_others=bool(row.combine_with_others),
        auto_apply=bool(row.auto_apply),
        audience=row.audience,
        first_purchase_only=bool(row.first_purchase_only),
        scope_mode=row.scope_mode,
        scope_keys=[s.scope_key for s in (row.scopes or [])],
        package_apply_mode=row.package_apply_mode,
        package_ids=[p.diagnostic_package_id for p in (row.packages or [])],
        include_addons=bool(row.include_addons),
        cities=[c.city for c in (row.cities or [])],
        valid_from=row.valid_from,
        valid_to=row.valid_to,
        time_of_day_start=row.time_of_day_start,
        time_of_day_end=row.time_of_day_end,
        days_of_week=row.days_of_week,
        camp_valid_days_after=row.camp_valid_days_after,
        max_total_uses=row.max_total_uses,
        max_uses_per_user=row.max_uses_per_user,
        per_user_frequency=row.per_user_frequency or "none",
        max_uses_per_camp=row.max_uses_per_camp,
        max_uses_per_order=row.max_uses_per_order,
        max_total_discount_paise=row.max_total_discount_paise,
        total_discount_given_paise=int(row.total_discount_given_paise or 0),
        code_kind=row.code_kind,
        referral_user_id=row.referral_user_id,
        min_price_protection=bool(row.min_price_protection),
        created_at=row.created_at,
        updated_at=row.updated_at,
    ).model_dump()


class DiscountService:
    def __init__(
        self,
        repository: DiscountRepository | None = None,
        engine: DiscountEngine | None = None,
    ):
        self.repository = repository or DiscountRepository()
        self.engine = engine or DiscountEngine()

    async def create_code(
        self, db: AsyncSession, payload: DiscountCodeCreate, *, actor_id: int
    ) -> dict[str, Any]:
        existing = await self.repository.get_by_code(db, payload.code)
        if existing:
            return {"_error": (409, "Discount code already exists")}
        row = await self.repository.create(db, payload, actor_id=actor_id)
        await db.refresh(row, attribute_names=["scopes", "packages", "cities"])
        return serialize_code(row)

    async def update_code(
        self,
        db: AsyncSession,
        discount_code_id: int,
        payload: DiscountCodeUpdate,
        *,
        actor_id: int,
    ) -> dict[str, Any]:
        row = await self.repository.get_by_id(db, discount_code_id)
        if row is None:
            return {"_error": (404, "Discount code not found")}
        row = await self.repository.update(db, row, payload, actor_id=actor_id)
        row = await self.repository.get_by_id(db, discount_code_id)
        assert row is not None
        return serialize_code(row)

    async def set_status(
        self, db: AsyncSession, discount_code_id: int, action: str, *, actor_id: int
    ) -> dict[str, Any]:
        row = await self.repository.get_by_id(db, discount_code_id)
        if row is None:
            return {"_error": (404, "Discount code not found")}
        mapping = {
            "activate": "active",
            "pause": "paused",
            "disable": "disabled",
            "draft": "draft",
        }
        new_status = mapping.get(action)
        if not new_status:
            return {"_error": (400, "Invalid status action")}
        old = row.status
        row.status = new_status
        row.updated_by = actor_id
        await self.repository.add_audit(
            db, discount_code_id, actor_id, "status", {"from": old, "to": new_status, "action": action}
        )
        await db.flush()
        return serialize_code(row)

    async def list_codes(
        self,
        db: AsyncSession,
        *,
        status: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        rows, total = await self.repository.list_codes(
            db, status=status, search=search, limit=limit, offset=offset
        )
        return {
            "items": [serialize_code(r) for r in rows],
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    async def get_code(self, db: AsyncSession, discount_code_id: int) -> dict[str, Any]:
        row = await self.repository.get_by_id(db, discount_code_id)
        if row is None:
            return {"_error": (404, "Discount code not found")}
        return serialize_code(row)

    async def validate_for_user(
        self,
        db: AsyncSession,
        *,
        code: str,
        user_id: int,
        items: list[CartLine],
        context: CheckoutContext,
        client_ip: str | None,
        endpoint: str,
        for_support: bool = False,
    ) -> dict[str, Any]:
        sanitized = abuse.sanitize_code(code)
        if sanitized is None:
            await abuse.record_attempt(
                db,
                user_id=user_id,
                client_ip=client_ip,
                code_submitted=normalize_code(code)[:64],
                outcome="invalid",
                endpoint=endpoint,
                detail="invalid_format",
            )
            return {
                "ok": False,
                "message": PUBLIC_INVALID if not for_support else "invalid_format",
                "reason": "invalid_format" if for_support else None,
            }

        try:
            await abuse.assert_not_locked(db, user_id=user_id, client_ip=client_ip)
            if not for_support:
                await abuse.assert_validate_hourly_cap(db, user_id=user_id)
        except abuse.AbuseError as exc:
            await abuse.record_attempt(
                db,
                user_id=user_id,
                client_ip=client_ip,
                code_submitted=sanitized,
                outcome=exc.outcome,
                endpoint=endpoint,
                detail=exc.message,
            )
            return {"_error": (exc.status_code, exc.message)}

        result = await self.engine.evaluate(
            db,
            code=sanitized,
            user_id=user_id,
            items=items,
            context=context,
            for_support=for_support,
        )
        outcome = abuse.map_engine_outcome(result.ok, result.reason)
        await abuse.record_attempt(
            db,
            user_id=user_id,
            client_ip=client_ip,
            code_submitted=sanitized,
            outcome=outcome,
            endpoint=endpoint,
            detail=result.reason,
        )
        if not result.ok:
            return {
                "ok": False,
                "message": result.public_message or PUBLIC_INVALID,
                "reason": result.reason if for_support else None,
            }
        return {
            "ok": True,
            "code": result.code,
            "discount_code_id": result.discount_code_id,
            "original_paise": result.original_paise,
            "discount_paise": result.discount_paise,
            "final_paise": result.final_paise,
            "eligible_line_indexes": result.eligible_line_indexes,
            "line_discounts_paise": result.line_discounts_paise,
        }

    async def auto_apply(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        items: list[CartLine],
        context: CheckoutContext,
        client_ip: str | None,
        endpoint: str,
    ) -> dict[str, Any]:
        try:
            await abuse.assert_not_locked(db, user_id=user_id, client_ip=client_ip)
        except abuse.AbuseError as exc:
            return {"_error": (exc.status_code, exc.message)}

        best: EvaluateResult | None = None
        codes = await self.repository.list_auto_apply(db)
        for row in codes:
            result = await self.engine.evaluate(
                db, code=row.code, user_id=user_id, items=items, context=context
            )
            if not result.ok:
                continue
            if best is None or result.discount_paise > best.discount_paise:
                best = result
        if best is None:
            await abuse.record_attempt(
                db,
                user_id=user_id,
                client_ip=client_ip,
                code_submitted=None,
                outcome="ineligible",
                endpoint=endpoint,
                detail="no_auto_apply",
            )
            return {"ok": False, "message": "No auto-apply discount available"}
        await abuse.record_attempt(
            db,
            user_id=user_id,
            client_ip=client_ip,
            code_submitted=best.code,
            outcome="ok",
            endpoint=endpoint,
        )
        return {
            "ok": True,
            "code": best.code,
            "discount_code_id": best.discount_code_id,
            "original_paise": best.original_paise,
            "discount_paise": best.discount_paise,
            "final_paise": best.final_paise,
            "eligible_line_indexes": best.eligible_line_indexes,
            "line_discounts_paise": best.line_discounts_paise,
        }

    async def reserve_for_order(
        self,
        db: AsyncSession,
        *,
        code: str,
        user_id: int,
        items: list[CartLine],
        context: CheckoutContext,
        order_id: int,
        booking_ids: list[int],
        client_ip: str | None,
    ) -> dict[str, Any]:
        try:
            await abuse.assert_not_locked(db, user_id=user_id, client_ip=client_ip)
            await abuse.assert_discounted_create_cap(db, user_id=user_id, client_ip=client_ip)
        except abuse.AbuseError as exc:
            await abuse.record_attempt(
                db,
                user_id=user_id,
                client_ip=client_ip,
                code_submitted=normalize_code(code),
                outcome=exc.outcome,
                endpoint="/payments/create-order",
                detail=exc.message,
            )
            return {"_error": (exc.status_code, exc.message)}

        result = await self.engine.evaluate(
            db,
            code=code,
            user_id=user_id,
            items=items,
            context=context,
            lock_code=True,
        )
        if not result.ok or not result.discount_code_id:
            await abuse.record_attempt(
                db,
                user_id=user_id,
                client_ip=client_ip,
                code_submitted=normalize_code(code),
                outcome=abuse.map_engine_outcome(False, result.reason),
                endpoint="/payments/create-order",
                detail=result.reason,
            )
            return {"_error": (400, PUBLIC_INVALID)}

        existing = await self.repository.find_open_reservation(
            db, user_id=user_id, discount_code_id=result.discount_code_id
        )
        if existing is not None:
            existing.status = "released"
            await db.flush()

        usage = DiscountUsage(
            discount_code_id=result.discount_code_id,
            instance_id=result.instance_id,
            user_id=user_id,
            order_id=order_id,
            booking_ids=booking_ids,
            original_paise=result.original_paise,
            discount_paise=result.discount_paise,
            final_paise=result.final_paise,
            organization_id=context.organization_id,
            camp_no=context.camp_no,
            engagement_id=context.engagement_id,
            status="reserved",
        )
        db.add(usage)
        await db.flush()
        await abuse.record_attempt(
            db,
            user_id=user_id,
            client_ip=client_ip,
            code_submitted=result.code,
            outcome="ok",
            endpoint="/payments/create-order",
        )
        return {
            "usage_id": usage.usage_id,
            "discount_paise": result.discount_paise,
            "final_paise": result.final_paise,
            "original_paise": result.original_paise,
            "line_discounts_paise": result.line_discounts_paise,
            "eligible_line_indexes": result.eligible_line_indexes,
            "discount_code_id": result.discount_code_id,
        }

    async def commit_for_order(self, db: AsyncSession, order_id: int) -> None:
        result = await db.execute(
            select(DiscountUsage).where(
                DiscountUsage.order_id == order_id,
                DiscountUsage.status == "reserved",
            )
        )
        for usage in result.scalars().all():
            usage.status = "committed"
            code = await self.repository.get_by_id(db, usage.discount_code_id)
            if code is not None:
                code.total_discount_given_paise = int(code.total_discount_given_paise or 0) + int(
                    usage.discount_paise
                )
                if code.max_total_uses is not None:
                    stats = await self.repository.usage_stats(db, code.discount_code_id)
                    if stats["committed_uses"] + 1 >= code.max_total_uses:
                        code.status = "finished"
                if (
                    code.max_total_discount_paise is not None
                    and code.total_discount_given_paise >= code.max_total_discount_paise
                ):
                    code.status = "finished"
            if usage.instance_id:
                inst = await db.get(DiscountCodeInstance, usage.instance_id)
                if inst is not None:
                    inst.status = "used"
                    inst.used_at = datetime.now(timezone.utc)
        await db.flush()

    async def release_for_order(self, db: AsyncSession, order_id: int) -> None:
        result = await db.execute(
            select(DiscountUsage).where(
                DiscountUsage.order_id == order_id,
                DiscountUsage.status == "reserved",
            )
        )
        for usage in result.scalars().all():
            usage.status = "released"
        await db.flush()

    async def refund_for_order(self, db: AsyncSession, order_id: int, *, partial: bool = False) -> None:
        result = await db.execute(
            select(DiscountUsage).where(
                DiscountUsage.order_id == order_id,
                DiscountUsage.status == "committed",
            )
        )
        for usage in result.scalars().all():
            if partial:
                # Fair partial: leave committed but reduce counters proportionally later; for now mark refunded fully only on full refund
                continue
            usage.status = "refunded"
            code = await self.repository.get_by_id(db, usage.discount_code_id)
            if code is not None:
                code.total_discount_given_paise = max(
                    0, int(code.total_discount_given_paise or 0) - int(usage.discount_paise)
                )
                if code.status == "finished":
                    code.status = "active"
            if usage.instance_id:
                inst = await db.get(DiscountCodeInstance, usage.instance_id)
                if inst is not None:
                    inst.status = "available"
                    inst.used_at = None
        await db.flush()

    async def bulk_instances(
        self, db: AsyncSession, discount_code_id: int, *, count: int, prefix: str | None, actor_id: int
    ) -> dict[str, Any]:
        row = await self.repository.get_by_id(db, discount_code_id)
        if row is None:
            return {"_error": (404, "Discount code not found")}
        if row.code_kind != "unique_pool":
            return {"_error": (400, "Code must be unique_pool to generate instances")}
        instances = await self.repository.bulk_instances(db, discount_code_id, count=count, prefix=prefix)
        await self.repository.add_audit(
            db, discount_code_id, actor_id, "bulk_instances", {"count": count}
        )
        return {
            "count": len(instances),
            "codes": [i.code for i in instances],
        }

    async def instances_csv(self, db: AsyncSession, discount_code_id: int) -> str:
        instances = await self.repository.list_instances(db, discount_code_id)
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["code", "status", "assigned_user_id", "used_at"])
        for inst in instances:
            writer.writerow([inst.code, inst.status, inst.assigned_user_id or "", inst.used_at or ""])
        return buf.getvalue()

    async def upload_allowlist(
        self, db: AsyncSession, discount_code_id: int, entries: list[dict], *, actor_id: int
    ) -> dict[str, Any]:
        row = await self.repository.get_by_id(db, discount_code_id)
        if row is None:
            return {"_error": (404, "Discount code not found")}
        added = await self.repository.add_allowlist(db, discount_code_id, entries)
        await self.repository.add_audit(
            db, discount_code_id, actor_id, "allowlist_upload", {"added": added}
        )
        return {"added": added}

    async def report(self, db: AsyncSession, discount_code_id: int) -> dict[str, Any]:
        row = await self.repository.get_by_id(db, discount_code_id)
        if row is None:
            return {"_error": (404, "Discount code not found")}
        stats = await self.repository.usage_stats(db, discount_code_id)
        remaining_uses = None
        if row.max_total_uses is not None:
            remaining_uses = max(0, row.max_total_uses - stats["committed_uses"] - stats["reserved_uses"])
        remaining_budget = None
        if row.max_total_discount_paise is not None:
            remaining_budget = max(
                0, row.max_total_discount_paise - int(row.total_discount_given_paise or 0)
            )
        return {
            "code": serialize_code(row),
            **stats,
            "remaining_uses": remaining_uses,
            "remaining_budget_paise": remaining_budget,
        }

    async def reports_summary(self, db: AsyncSession) -> dict[str, Any]:
        rows, total = await self.repository.list_codes(db, limit=200, offset=0)
        abuse_events = await abuse.count_abuse_events_24h(db)
        summaries = []
        for row in rows:
            stats = await self.repository.usage_stats(db, row.discount_code_id)
            summaries.append(
                {
                    "discount_code_id": row.discount_code_id,
                    "code": row.code,
                    "name": row.name,
                    "status": row.status,
                    **stats,
                }
            )
        return {"total_codes": total, "abuse_events_24h": abuse_events, "items": summaries}

    async def support_lookup(
        self,
        db: AsyncSession,
        *,
        code: str,
        phone: str | None,
        items: list[CartLine] | None,
        context: CheckoutContext,
    ) -> dict[str, Any]:
        user_id = None
        if phone:
            result = await db.execute(select(User).where(User.phone == phone).limit(1))
            user = result.scalar_one_or_none()
            if user:
                user_id = user.user_id

        row = await self.repository.get_by_code(db, code)
        instance = None
        if row is None:
            inst_result = await db.execute(
                select(DiscountCodeInstance).where(
                    DiscountCodeInstance.code == normalize_code(code)
                )
            )
            instance = inst_result.scalar_one_or_none()
            if instance:
                row = await self.repository.get_by_id(db, instance.discount_code_id)

        base = {
            "found": row is not None,
            "code": serialize_code(row) if row else None,
            "instance": (
                {
                    "instance_id": instance.instance_id,
                    "code": instance.code,
                    "status": instance.status,
                    "assigned_user_id": instance.assigned_user_id,
                }
                if instance
                else None
            ),
            "user_id": user_id,
        }
        if row is None or user_id is None or not items:
            return base

        result = await self.engine.evaluate(
            db,
            code=normalize_code(code),
            user_id=user_id,
            items=items,
            context=context,
            for_support=True,
        )
        return {
            **base,
            "evaluation": result.model_dump(),
        }

    async def options_packages(
        self, db: AsyncSession, *, package_for: str | None = None, search: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        stmt = select(DiagnosticPackage).where(DiagnosticPackage.status == "active").limit(limit)
        if package_for:
            stmt = stmt.where(DiagnosticPackage.package_for == package_for)
        if search:
            stmt = stmt.where(DiagnosticPackage.package_name.ilike(f"%{search.strip()}%"))
        rows = list((await db.execute(stmt)).scalars().all())
        return [
            {
                "id": r.diagnostic_package_id,
                "label": r.package_name,
                "package_for": r.package_for,
                "price": float(r.price) if r.price is not None else None,
            }
            for r in rows
        ]

    async def options_organizations(
        self, db: AsyncSession, *, search: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        stmt = select(Organization).order_by(Organization.name.asc()).limit(limit)
        if search:
            stmt = stmt.where(Organization.name.ilike(f"%{search.strip()}%"))
        rows = list((await db.execute(stmt)).scalars().all())
        return [{"id": str(r.organization_id), "label": r.name} for r in rows]

    async def options_engagements(
        self, db: AsyncSession, *, search: str | None = None, organization_id: int | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        stmt = select(Engagement).order_by(Engagement.engagement_id.desc()).limit(limit)
        if organization_id:
            stmt = stmt.where(Engagement.organization_id == organization_id)
        if search:
            stmt = stmt.where(Engagement.engagement_name.ilike(f"%{search.strip()}%"))
        rows = list((await db.execute(stmt)).scalars().all())
        return [
            {
                "id": str(r.engagement_id),
                "label": r.engagement_name or f"Engagement {r.engagement_id}",
                "camp_no": str(r.camp_no) if r.camp_no is not None else None,
                "organization_id": r.organization_id,
            }
            for r in rows
        ]

    async def options_camps(
        self,
        db: AsyncSession,
        *,
        search: str | None = None,
        organization_id: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses = [Engagement.camp_no.is_not(None)]
        if organization_id:
            clauses.append(Engagement.organization_id == organization_id)
        stmt = (
            select(
                Engagement.camp_no,
                Engagement.organization_id,
                func.min(Engagement.engagement_name).label("camp_name"),
            )
            .where(*clauses)
            .group_by(Engagement.camp_no, Engagement.organization_id)
            .order_by(Engagement.camp_no.desc())
            .limit(limit)
        )
        rows = (await db.execute(stmt)).all()
        out = []
        for camp_no, org_id, camp_name in rows:
            key = str(camp_no)
            if search and search.strip().lower() not in key.lower() and search.strip().lower() not in (camp_name or "").lower():
                continue
            out.append(
                {
                    "id": key,
                    "label": camp_name or f"Camp {key}",
                    "organization_id": org_id,
                }
            )
        return out
