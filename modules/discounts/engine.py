"""Discount evaluation engine (pure eligibility + amount math)."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from modules.diagnostics.models import DiagnosticPackage
from modules.discounts.models import (
    DiscountAllowlistUser,
    DiscountCode,
    DiscountCodeInstance,
    DiscountUsage,
)
from modules.discounts.schemas import CartLine, CheckoutContext, EvaluateResult, normalize_code
from modules.payments.models import Payment
from modules.users.models import User

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")
PUBLIC_INVALID = "This code is not valid for this order"
CODE_PATTERN = re.compile(r"^[A-Z0-9_-]{3,32}$")
ACTIVE_USAGE_STATUSES = ("reserved", "committed")


def round_to_nearest_rupee_paise(paise: int) -> int:
    """Round paise to nearest whole rupee (100 paise)."""
    if paise <= 0:
        return 0
    return int(round(paise / 100.0) * 100)


def _price_to_paise(price: Decimal | float | int | None) -> int:
    if price is None:
        return 0
    return int(Decimal(str(price)) * 100)


def _parse_hhmm(value: str | None) -> time | None:
    if not value:
        return None
    try:
        parts = value.strip().split(":")
        return time(int(parts[0]), int(parts[1]) if len(parts) > 1 else 0)
    except (ValueError, IndexError):
        return None


@dataclass
class ResolvedLine:
    index: int
    user_id: int
    entity_type: str
    entity_id: int
    amount_paise: int
    min_price_paise: int
    is_addon: bool
    package_name: str = ""


class DiscountEngine:
    async def evaluate(
        self,
        db: AsyncSession,
        *,
        code: str,
        user_id: int,
        items: list[CartLine],
        context: CheckoutContext | None = None,
        for_support: bool = False,
        lock_code: bool = False,
    ) -> EvaluateResult:
        context = context or CheckoutContext()
        normalized = normalize_code(code)
        if not CODE_PATTERN.fullmatch(normalized):
            return self._fail("invalid_format", for_support=for_support)

        code_row, instance = await self._load_code(db, normalized, lock=lock_code)
        if code_row is None:
            return self._fail("not_found", for_support=for_support)

        effective_status = self._effective_status(code_row)
        if effective_status != "active":
            return self._fail(f"status_{effective_status}", for_support=for_support)

        now_ist = datetime.now(IST)
        schedule_fail = self._check_schedule(code_row, now_ist, context)
        if schedule_fail:
            return self._fail(schedule_fail, for_support=for_support)

        scope_fail = self._check_scope(code_row, context)
        if scope_fail:
            return self._fail(scope_fail, for_support=for_support)

        city_fail = self._check_cities(code_row, context)
        if city_fail:
            return self._fail(city_fail, for_support=for_support)

        user = await self._get_user(db, user_id)
        if user is None:
            return self._fail("user_not_found", for_support=for_support)

        audience_fail = await self._check_audience(db, code_row, user)
        if audience_fail:
            return self._fail(audience_fail, for_support=for_support)

        allow_fail = await self._check_allowlist(db, code_row, user)
        if allow_fail:
            return self._fail(allow_fail, for_support=for_support)

        if instance is not None:
            if instance.status not in ("available", "assigned"):
                return self._fail("instance_used", for_support=for_support)
            if instance.assigned_user_id and instance.assigned_user_id != user_id:
                return self._fail("instance_wrong_user", for_support=for_support)

        resolved = await self._resolve_lines(db, items)
        if not resolved:
            return self._fail("no_eligible_lines", for_support=for_support)

        eligible = self._filter_packages(code_row, resolved)
        if not eligible:
            return self._fail("packages_not_eligible", for_support=for_support)

        eligible_subtotal = sum(line.amount_paise for line in eligible)
        if code_row.min_bill_paise and eligible_subtotal < code_row.min_bill_paise:
            return self._fail("min_bill", for_support=for_support)

        usage_fail = await self._check_usage_limits(db, code_row, user_id, context, camp_no=context.camp_no)
        if usage_fail:
            return self._fail(usage_fail, for_support=for_support)

        discount_paise, line_discounts = self._compute_discount(code_row, eligible, eligible_subtotal)
        if discount_paise <= 0:
            return self._fail("zero_discount", for_support=for_support)

        original = sum(line.amount_paise for line in resolved)
        final = max(0, original - discount_paise)

        money_fail = await self._check_money_limit(db, code_row, discount_paise)
        if money_fail:
            return self._fail(money_fail, for_support=for_support)

        return EvaluateResult(
            ok=True,
            discount_code_id=code_row.discount_code_id,
            instance_id=instance.instance_id if instance else None,
            code=code_row.code if code_row.code_kind == "shared" else normalized,
            original_paise=original,
            discount_paise=discount_paise,
            final_paise=final,
            eligible_line_indexes=[line.index for line in eligible],
            line_discounts_paise=line_discounts,
            details={"effective_status": effective_status},
        )

    def _fail(self, reason: str, *, for_support: bool) -> EvaluateResult:
        return EvaluateResult(
            ok=False,
            reason=reason,
            public_message=PUBLIC_INVALID if not for_support else reason,
        )

    async def _load_code(
        self, db: AsyncSession, normalized: str, *, lock: bool
    ) -> tuple[DiscountCode | None, DiscountCodeInstance | None]:
        stmt = (
            select(DiscountCode)
            .options(
                selectinload(DiscountCode.scopes),
                selectinload(DiscountCode.packages),
                selectinload(DiscountCode.cities),
            )
            .where(DiscountCode.code == normalized)
        )
        if lock:
            stmt = stmt.with_for_update()
        result = await db.execute(stmt)
        code_row = result.scalar_one_or_none()
        if code_row is not None:
            return code_row, None

        inst_stmt = select(DiscountCodeInstance).where(DiscountCodeInstance.code == normalized)
        if lock:
            inst_stmt = inst_stmt.with_for_update()
        inst_result = await db.execute(inst_stmt)
        instance = inst_result.scalar_one_or_none()
        if instance is None:
            return None, None

        parent_stmt = (
            select(DiscountCode)
            .options(
                selectinload(DiscountCode.scopes),
                selectinload(DiscountCode.packages),
                selectinload(DiscountCode.cities),
            )
            .where(DiscountCode.discount_code_id == instance.discount_code_id)
        )
        if lock:
            parent_stmt = parent_stmt.with_for_update()
        parent_result = await db.execute(parent_stmt)
        return parent_result.scalar_one_or_none(), instance

    def _effective_status(self, code: DiscountCode) -> str:
        if code.status in ("draft", "paused", "disabled", "finished"):
            return code.status
        now = datetime.now(timezone.utc)
        if code.valid_from and code.valid_from > now:
            return "draft"
        if code.valid_to and code.valid_to < now:
            return "expired"
        if code.max_total_uses is not None or code.max_total_discount_paise is not None:
            # finished is primarily set on commit; treat over money as finished if counter already past
            if (
                code.max_total_discount_paise is not None
                and (code.total_discount_given_paise or 0) >= code.max_total_discount_paise
            ):
                return "finished"
        return code.status if code.status == "active" else code.status

    def _check_schedule(
        self, code: DiscountCode, now_ist: datetime, context: CheckoutContext
    ) -> str | None:
        if code.days_of_week:
            # Monday=0 .. Sunday=6
            if now_ist.weekday() not in set(code.days_of_week):
                return "day_of_week"
        start_t = _parse_hhmm(code.time_of_day_start)
        end_t = _parse_hhmm(code.time_of_day_end)
        if start_t and end_t:
            current = now_ist.time().replace(second=0, microsecond=0)
            if start_t <= end_t:
                if not (start_t <= current <= end_t):
                    return "time_of_day"
            else:
                # overnight window
                if not (current >= start_t or current <= end_t):
                    return "time_of_day"
        if code.camp_valid_days_after is not None and context.camp_no:
            # Without camp start in context, skip strict check; service may enrich later
            pass
        return None

    def _check_scope(self, code: DiscountCode, context: CheckoutContext) -> str | None:
        if code.scope_mode == "general":
            return None
        keys = {s.scope_key for s in (code.scopes or []) if s.scope_type == code.scope_mode}
        if not keys:
            return "scope_empty"
        if code.scope_mode == "organization":
            if not context.organization_id or str(context.organization_id) not in keys:
                return "scope_organization"
        elif code.scope_mode == "camp":
            if not context.camp_no or context.camp_no not in keys:
                return "scope_camp"
        elif code.scope_mode == "engagement":
            if not context.engagement_id or str(context.engagement_id) not in keys:
                return "scope_engagement"
        return None

    def _check_cities(self, code: DiscountCode, context: CheckoutContext) -> str | None:
        cities = [c.city.lower() for c in (code.cities or [])]
        if not cities:
            return None
        if not context.city or context.city.strip().lower() not in cities:
            return "city"
        return None

    async def _get_user(self, db: AsyncSession, user_id: int) -> User | None:
        result = await db.execute(select(User).where(User.user_id == user_id))
        return result.scalar_one_or_none()

    async def _check_audience(self, db: AsyncSession, code: DiscountCode, user: User) -> str | None:
        if code.audience == "everyone" and not code.first_purchase_only:
            return None

        paid_count = await self._count_paid_orders(db, user.user_id)
        is_new = paid_count == 0
        if code.audience == "new_users" and not is_new:
            return "audience_new"
        if code.audience == "existing_users" and is_new:
            return "audience_existing"
        if code.first_purchase_only and paid_count > 0:
            return "first_purchase_only"
        return None

    async def _count_paid_orders(self, db: AsyncSession, user_id: int) -> int:
        result = await db.execute(
            select(func.count(Payment.payment_id)).where(
                Payment.user_id == user_id,
                Payment.status.in_(("captured", "paid", "success")),
            )
        )
        return int(result.scalar() or 0)

    async def _check_allowlist(self, db: AsyncSession, code: DiscountCode, user: User) -> str | None:
        count_result = await db.execute(
            select(func.count(DiscountAllowlistUser.id)).where(
                DiscountAllowlistUser.discount_code_id == code.discount_code_id
            )
        )
        if int(count_result.scalar() or 0) == 0:
            return None
        phone = (getattr(user, "phone", None) or "").strip()
        email = (getattr(user, "email", None) or "").strip().lower()
        clauses = []
        if phone:
            clauses.append(DiscountAllowlistUser.phone == phone)
        if email:
            clauses.append(func.lower(DiscountAllowlistUser.email) == email)
        if not clauses:
            return "allowlist"
        result = await db.execute(
            select(DiscountAllowlistUser.id).where(
                DiscountAllowlistUser.discount_code_id == code.discount_code_id,
                or_(*clauses),
            ).limit(1)
        )
        if result.scalar_one_or_none() is None:
            return "allowlist"
        return None

    async def _resolve_lines(self, db: AsyncSession, items: list[CartLine]) -> list[ResolvedLine]:
        out: list[ResolvedLine] = []
        for idx, item in enumerate(items):
            if item.entity_type.strip() != "diagnostic_package":
                continue
            result = await db.execute(
                select(DiagnosticPackage).where(
                    DiagnosticPackage.diagnostic_package_id == item.entity_id,
                    DiagnosticPackage.status == "active",
                )
            )
            package = result.scalar_one_or_none()
            if package is None:
                continue
            amount = item.amount_paise
            if amount is None:
                rupee = package.price if package.price is not None else package.original_price
                amount = _price_to_paise(rupee)
            min_paise = _price_to_paise(getattr(package, "min_price", None))
            out.append(
                ResolvedLine(
                    index=idx,
                    user_id=item.user_id,
                    entity_type="diagnostic_package",
                    entity_id=item.entity_id,
                    amount_paise=amount,
                    min_price_paise=min_paise,
                    is_addon=bool(item.is_addon),
                    package_name=package.package_name or "",
                )
            )
        return out

    def _filter_packages(self, code: DiscountCode, lines: list[ResolvedLine]) -> list[ResolvedLine]:
        eligible = []
        include_ids = {
            p.diagnostic_package_id for p in (code.packages or []) if p.mode == "include"
        }
        exclude_ids = {
            p.diagnostic_package_id for p in (code.packages or []) if p.mode == "exclude"
        }
        for line in lines:
            if not code.include_addons and line.is_addon:
                continue
            if code.package_apply_mode == "include":
                if line.entity_id not in include_ids:
                    continue
            elif code.package_apply_mode == "exclude":
                if line.entity_id in exclude_ids:
                    continue
            eligible.append(line)
        return eligible

    def _compute_discount(
        self, code: DiscountCode, eligible: list[ResolvedLine], eligible_subtotal: int
    ) -> tuple[int, list[int]]:
        if code.discount_type == "fixed":
            raw = int(code.fixed_amount_paise or 0)
        else:
            percent = Decimal(str(code.percent_value or 0))
            raw = int((Decimal(eligible_subtotal) * percent / Decimal(100)).quantize(Decimal("1")))
            if code.discount_type == "percentage_capped" or code.max_discount_paise:
                cap = int(code.max_discount_paise or raw)
                raw = min(raw, cap)

        if code.hard_ceiling_paise is not None:
            raw = min(raw, int(code.hard_ceiling_paise))

        raw = min(raw, eligible_subtotal)
        raw = round_to_nearest_rupee_paise(raw)

        # Distribute proportionally then clamp with min_price
        line_discounts = [0] * len(eligible)
        if raw <= 0 or eligible_subtotal <= 0:
            return 0, line_discounts

        remaining = raw
        for i, line in enumerate(eligible):
            if i == len(eligible) - 1:
                share = remaining
            else:
                share = int(raw * (line.amount_paise / eligible_subtotal))
                remaining -= share
            max_off = line.amount_paise
            if code.min_price_protection and line.min_price_paise > 0:
                max_off = max(0, line.amount_paise - line.min_price_paise)
            line_discounts[i] = min(share, max_off)

        total = sum(line_discounts)
        # If clamping reduced total, leave it; do not redistribute above floors
        return total, line_discounts

    async def _check_usage_limits(
        self,
        db: AsyncSession,
        code: DiscountCode,
        user_id: int,
        context: CheckoutContext,
        *,
        camp_no: str | None,
    ) -> str | None:
        if code.max_total_uses is not None:
            total = await self._count_usages(db, code.discount_code_id)
            if total >= code.max_total_uses:
                return "max_total_uses"

        if code.max_uses_per_user is not None:
            user_total = await self._count_usages(db, code.discount_code_id, user_id=user_id)
            if user_total >= code.max_uses_per_user:
                return "max_uses_per_user"

        if code.per_user_frequency and code.per_user_frequency != "none":
            since = self._frequency_since(code.per_user_frequency)
            freq_count = await self._count_usages(
                db, code.discount_code_id, user_id=user_id, since=since
            )
            if freq_count >= 1:
                return f"frequency_{code.per_user_frequency}"

        if code.max_uses_per_camp is not None and camp_no:
            camp_count = await self._count_usages(db, code.discount_code_id, camp_no=camp_no)
            if camp_count >= code.max_uses_per_camp:
                return "max_uses_per_camp"

        return None

    def _frequency_since(self, freq: str) -> datetime:
        now = datetime.now(IST)
        if freq == "day":
            start = datetime(now.year, now.month, now.day, tzinfo=IST)
        elif freq == "week":
            start = datetime(now.year, now.month, now.day, tzinfo=IST) - timedelta(days=now.weekday())
        else:  # month
            start = datetime(now.year, now.month, 1, tzinfo=IST)
        return start.astimezone(timezone.utc)

    async def _count_usages(
        self,
        db: AsyncSession,
        discount_code_id: int,
        *,
        user_id: int | None = None,
        camp_no: str | None = None,
        since: datetime | None = None,
    ) -> int:
        clauses = [
            DiscountUsage.discount_code_id == discount_code_id,
            DiscountUsage.status.in_(ACTIVE_USAGE_STATUSES),
        ]
        if user_id is not None:
            clauses.append(DiscountUsage.user_id == user_id)
        if camp_no is not None:
            clauses.append(DiscountUsage.camp_no == camp_no)
        if since is not None:
            clauses.append(DiscountUsage.created_at >= since)
        result = await db.execute(select(func.count(DiscountUsage.usage_id)).where(and_(*clauses)))
        return int(result.scalar() or 0)

    async def _check_money_limit(
        self, db: AsyncSession, code: DiscountCode, upcoming_discount: int
    ) -> str | None:
        if code.max_total_discount_paise is None:
            return None
        reserved = await db.execute(
            select(func.coalesce(func.sum(DiscountUsage.discount_paise), 0)).where(
                DiscountUsage.discount_code_id == code.discount_code_id,
                DiscountUsage.status == "reserved",
            )
        )
        reserved_sum = int(reserved.scalar() or 0)
        given = int(code.total_discount_given_paise or 0) + reserved_sum
        if given + upcoming_discount > code.max_total_discount_paise:
            return "max_total_discount"
        return None
