"""Shared frozen dataclasses for database seed rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class SeedUser:
    user_id: int
    first_name: str
    last_name: str
    age: int
    phone: str
    email: str
    date_of_birth: date
    gender: str
    address: str
    pin_code: str
    city: str
    state: str
    country: str
    referred_by: str | None
    is_participant: bool
    status: str


@dataclass(frozen=True)
class SeedEmployee:
    employee_id: int
    user_id: int
    role: str
    status: str


@dataclass(frozen=True)
class SeedAssessmentPackage:
    package_id: int
    package_code: str
    display_name: str
    assessment_type_code: str
    status: str


@dataclass(frozen=True)
class SeedCategory:
    category_id: int
    category_key: str
    display_name: str
    status: str


@dataclass(frozen=True)
class SeedQuestion:
    question_id: int
    question_key: str
    question_text: str
    question_type: str
    is_required: bool
    is_read_only: bool
    help_text: str | None
    status: str


@dataclass(frozen=True)
class SeedCategoryQuestion:
    id: int
    category_id: int
    question_id: int


@dataclass(frozen=True)
class SeedOption:
    option_id: int
    question_id: int
    option_value: str
    display_name: str
    tooltip_text: str | None


@dataclass(frozen=True)
class SeedPackageCategory:
    id: int
    package_id: int
    category_id: int
