"""Pydantic schemas for the discount module."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


ScopeMode = Literal["general", "organization", "camp", "engagement"]
DiscountType = Literal["percentage", "fixed", "percentage_capped"]
PackageApplyMode = Literal["all", "include", "exclude"]
Audience = Literal["everyone", "new_users", "existing_users"]
CodeKind = Literal["shared", "unique_pool"]
PerUserFrequency = Literal["none", "day", "week", "month"]
DiscountStatus = Literal["draft", "active", "paused", "expired", "finished", "disabled"]
StatusAction = Literal["activate", "pause", "disable", "draft"]


def normalize_code(value: str) -> str:
    return (value or "").strip().upper()


class CartLine(BaseModel):
    user_id: int = Field(..., ge=1)
    entity_type: str = Field(..., min_length=1)
    entity_id: int = Field(..., ge=1)
    amount_paise: Optional[int] = Field(default=None, ge=0)
    is_addon: bool = False


class CheckoutContext(BaseModel):
    organization_id: Optional[int] = None
    camp_no: Optional[str] = None
    engagement_id: Optional[int] = None
    city: Optional[str] = None


class DiscountValidateRequest(BaseModel):
    code: str = Field(..., min_length=1, max_length=64)
    items: list[CartLine] = Field(..., min_length=1, max_length=20)
    context: CheckoutContext = Field(default_factory=CheckoutContext)

    @field_validator("code")
    @classmethod
    def _norm_code(cls, v: str) -> str:
        return normalize_code(v)


class DiscountAutoApplyRequest(BaseModel):
    items: list[CartLine] = Field(..., min_length=1, max_length=20)
    context: CheckoutContext = Field(default_factory=CheckoutContext)


class DiscountCodeCreate(BaseModel):
    code: str = Field(..., min_length=3, max_length=32)
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    status: DiscountStatus = "draft"
    discount_type: DiscountType
    percent_value: Optional[Decimal] = None
    fixed_amount_paise: Optional[int] = Field(default=None, ge=0)
    max_discount_paise: Optional[int] = Field(default=None, ge=0)
    hard_ceiling_paise: Optional[int] = Field(default=None, ge=0)
    min_bill_paise: Optional[int] = Field(default=None, ge=0)
    combine_with_others: bool = False
    auto_apply: bool = False
    audience: Audience = "everyone"
    first_purchase_only: bool = False
    scope_mode: ScopeMode = "general"
    scope_keys: list[str] = Field(default_factory=list)
    package_apply_mode: PackageApplyMode = "all"
    package_ids: list[int] = Field(default_factory=list)
    include_addons: bool = True
    cities: list[str] = Field(default_factory=list)
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    time_of_day_start: Optional[str] = None
    time_of_day_end: Optional[str] = None
    days_of_week: Optional[list[int]] = None
    camp_valid_days_after: Optional[int] = Field(default=None, ge=0)
    max_total_uses: Optional[int] = Field(default=None, ge=1)
    max_uses_per_user: Optional[int] = Field(default=None, ge=1)
    per_user_frequency: PerUserFrequency = "none"
    max_uses_per_camp: Optional[int] = Field(default=None, ge=1)
    max_uses_per_order: Optional[int] = Field(default=None, ge=1)
    max_total_discount_paise: Optional[int] = Field(default=None, ge=1)
    code_kind: CodeKind = "shared"
    referral_user_id: Optional[int] = None
    min_price_protection: bool = True

    @field_validator("code")
    @classmethod
    def _norm_code(cls, v: str) -> str:
        code = normalize_code(v)
        import re

        if not re.fullmatch(r"[A-Z0-9_-]{3,32}", code):
            raise ValueError("Code must be 3-32 chars of A-Z, 0-9, _ or -")
        return code

    @model_validator(mode="after")
    def _validate_type_and_scope(self) -> "DiscountCodeCreate":
        if self.discount_type in ("percentage", "percentage_capped"):
            if self.percent_value is None or self.percent_value <= 0 or self.percent_value > 100:
                raise ValueError("percent_value must be between 0 and 100")
        if self.discount_type == "fixed":
            if not self.fixed_amount_paise or self.fixed_amount_paise <= 0:
                raise ValueError("fixed_amount_paise is required for fixed discounts")
        if self.discount_type == "percentage_capped" and not self.max_discount_paise:
            raise ValueError("max_discount_paise is required for percentage_capped")
        if self.scope_mode != "general" and not self.scope_keys:
            raise ValueError("scope_keys required when scope_mode is not general")
        if self.package_apply_mode in ("include", "exclude") and not self.package_ids:
            raise ValueError("package_ids required for include/exclude package mode")
        return self


class DiscountCodeUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    description: Optional[str] = None
    discount_type: Optional[DiscountType] = None
    percent_value: Optional[Decimal] = None
    fixed_amount_paise: Optional[int] = Field(default=None, ge=0)
    max_discount_paise: Optional[int] = Field(default=None, ge=0)
    hard_ceiling_paise: Optional[int] = Field(default=None, ge=0)
    min_bill_paise: Optional[int] = Field(default=None, ge=0)
    combine_with_others: Optional[bool] = None
    auto_apply: Optional[bool] = None
    audience: Optional[Audience] = None
    first_purchase_only: Optional[bool] = None
    scope_mode: Optional[ScopeMode] = None
    scope_keys: Optional[list[str]] = None
    package_apply_mode: Optional[PackageApplyMode] = None
    package_ids: Optional[list[int]] = None
    include_addons: Optional[bool] = None
    cities: Optional[list[str]] = None
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    time_of_day_start: Optional[str] = None
    time_of_day_end: Optional[str] = None
    days_of_week: Optional[list[int]] = None
    camp_valid_days_after: Optional[int] = Field(default=None, ge=0)
    max_total_uses: Optional[int] = Field(default=None, ge=1)
    max_uses_per_user: Optional[int] = Field(default=None, ge=1)
    per_user_frequency: Optional[PerUserFrequency] = None
    max_uses_per_camp: Optional[int] = Field(default=None, ge=1)
    max_uses_per_order: Optional[int] = Field(default=None, ge=1)
    max_total_discount_paise: Optional[int] = Field(default=None, ge=1)
    referral_user_id: Optional[int] = None
    min_price_protection: Optional[bool] = None


class DiscountStatusUpdate(BaseModel):
    action: StatusAction


class BulkInstancesRequest(BaseModel):
    count: int = Field(..., ge=1, le=5000)
    prefix: Optional[str] = Field(default=None, max_length=12)


class AllowlistUploadRequest(BaseModel):
    entries: list[dict[str, Optional[str]]] = Field(..., min_length=1, max_length=10000)


class DiscountCodeOut(BaseModel):
    discount_code_id: int
    code: str
    name: str
    description: Optional[str] = None
    status: str
    discount_type: str
    percent_value: Optional[Decimal] = None
    fixed_amount_paise: Optional[int] = None
    max_discount_paise: Optional[int] = None
    hard_ceiling_paise: Optional[int] = None
    min_bill_paise: Optional[int] = None
    combine_with_others: bool
    auto_apply: bool
    audience: str
    first_purchase_only: bool
    scope_mode: str
    scope_keys: list[str] = Field(default_factory=list)
    package_apply_mode: str
    package_ids: list[int] = Field(default_factory=list)
    include_addons: bool
    cities: list[str] = Field(default_factory=list)
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    time_of_day_start: Optional[str] = None
    time_of_day_end: Optional[str] = None
    days_of_week: Optional[list[int]] = None
    camp_valid_days_after: Optional[int] = None
    max_total_uses: Optional[int] = None
    max_uses_per_user: Optional[int] = None
    per_user_frequency: str
    max_uses_per_camp: Optional[int] = None
    max_uses_per_order: Optional[int] = None
    max_total_discount_paise: Optional[int] = None
    total_discount_given_paise: int = 0
    code_kind: str
    referral_user_id: Optional[int] = None
    min_price_protection: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class EvaluateResult(BaseModel):
    ok: bool
    reason: Optional[str] = None
    public_message: Optional[str] = None
    discount_code_id: Optional[int] = None
    instance_id: Optional[int] = None
    code: Optional[str] = None
    original_paise: int = 0
    discount_paise: int = 0
    final_paise: int = 0
    eligible_line_indexes: list[int] = Field(default_factory=list)
    line_discounts_paise: list[int] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)
