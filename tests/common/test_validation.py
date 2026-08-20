"""Unit tests for common.validation sanitization types."""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError as PydanticValidationError

from common.validation import (
    AddressText,
    ChecklistText,
    ExpertAboutText,
    OptionalPersonName,
    OptionalPinCode,
    PersonName,
    PhoneStr,
    PinCode,
    PositiveIntId,
    SafeDisplayName,
    SafeText,
    SlugKey,
    ServiceKey,
    SupportQueryText,
    ValidationError,
    validate_nested_strings,
)


class _PersonNameModel(BaseModel):
    name: PersonName


class _OptionalPersonNameModel(BaseModel):
    name: OptionalPersonName = None


class _SafeDisplayNameModel(BaseModel):
    name: SafeDisplayName


class _SafeTextModel(BaseModel):
    text: SafeText


class _PhoneModel(BaseModel):
    phone: PhoneStr


class _PinModel(BaseModel):
    pin: PinCode


class _OptionalPinModel(BaseModel):
    pin: OptionalPinCode = None


class _PositiveIdModel(BaseModel):
    id: PositiveIntId


class _SlugModel(BaseModel):
    key: SlugKey


class _ServiceKeyModel(BaseModel):
    key: ServiceKey


class _SupportQueryModel(BaseModel):
    query: SupportQueryText


class _ChecklistTextModel(BaseModel):
    notes: ChecklistText


class _ExpertAboutModel(BaseModel):
    about: ExpertAboutText


class _AddressModel(BaseModel):
    address: AddressText


# ── PersonName ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "value",
    ["Rahul", "Priya Sharma", "राज", "José García"],
)
def test_person_name_accepts_valid(value: str):
    assert _PersonNameModel(name=value).name == value


@pytest.mark.parametrize(
    "value",
    ["John3", "Mary-Jane", "O'Brien", "<script>", "", "   "],
)
def test_person_name_rejects_invalid(value: str):
    with pytest.raises(PydanticValidationError):
        _PersonNameModel(name=value)


def test_optional_person_name_none():
    assert _OptionalPersonNameModel().name is None
    assert _OptionalPersonNameModel(name=None).name is None


def test_optional_person_name_empty_becomes_none():
    assert _OptionalPersonNameModel(name="").name is None
    assert _OptionalPersonNameModel(name="   ").name is None


# ── SafeDisplayName ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "value",
    ["Acme Corp.", "Org & Co", "Test (India)", "Building 42"],
)
def test_safe_display_name_accepts_valid(value: str):
    assert _SafeDisplayNameModel(name=value).name == value


@pytest.mark.parametrize(
    "value",
    ["<script>alert(1)</script>", "bad;drop", "name@corp"],
)
def test_safe_display_name_rejects_invalid(value: str):
    with pytest.raises(PydanticValidationError):
        _SafeDisplayNameModel(name=value)


# ── SafeText ────────────────────────────────────────────────────────────────────


def test_safe_text_accepts_plain():
    assert _SafeTextModel(text="Hello world, line 2.").text == "Hello world, line 2."


@pytest.mark.parametrize(
    "value",
    ["<script>alert(1)</script>", "javascript:alert(1)", "<img onerror=x>"],
)
def test_safe_text_rejects_html_script(value: str):
    with pytest.raises(PydanticValidationError):
        _SafeTextModel(text=value)


def test_support_query_max_length():
    _SupportQueryModel(query="a" * 1000)
    with pytest.raises(PydanticValidationError):
        _SupportQueryModel(query="a" * 1001)


def test_checklist_text_max_length():
    _ChecklistTextModel(notes="a" * 500)
    with pytest.raises(PydanticValidationError):
        _ChecklistTextModel(notes="a" * 501)


def test_expert_about_max_length():
    _ExpertAboutModel(about="a" * 1200)
    with pytest.raises(PydanticValidationError):
        _ExpertAboutModel(about="a" * 1201)


# ── Phone / Pin ─────────────────────────────────────────────────────────────────


def test_phone_accepts_valid():
    assert _PhoneModel(phone="8103946120").phone == "8103946120"
    assert _PhoneModel(phone="+918103946120").phone == "+918103946120"


@pytest.mark.parametrize("value", ["12345", "a" * 16, "abc"])
def test_phone_rejects_invalid(value: str):
    with pytest.raises(PydanticValidationError):
        _PhoneModel(phone=value)


def test_pin_code_exactly_six_digits():
    assert _PinModel(pin="560001").pin == "560001"
    with pytest.raises(PydanticValidationError):
        _PinModel(pin="56001")
    with pytest.raises(PydanticValidationError):
        _PinModel(pin="5600011")
    with pytest.raises(PydanticValidationError):
        _PinModel(pin="56000a")


def test_optional_pin_none():
    assert _OptionalPinModel().pin is None


# ── IDs / Slug ──────────────────────────────────────────────────────────────────


def test_positive_int_id():
    assert _PositiveIdModel(id=1).id == 1
    with pytest.raises(PydanticValidationError):
        _PositiveIdModel(id=0)
    with pytest.raises(PydanticValidationError):
        _PositiveIdModel(id=-1)


def test_slug_key():
    assert _SlugModel(key="type_key_1").key == "type_key_1"
    with pytest.raises(PydanticValidationError):
        _SlugModel(key="Bad Key")


def test_service_key_allows_hyphens():
    assert _ServiceKeyModel(key="whatapi-otp").key == "whatapi-otp"
    assert _ServiceKeyModel(key="email-otp").key == "email-otp"
    with pytest.raises(PydanticValidationError):
        _ServiceKeyModel(key="Bad Key")


# ── Address ─────────────────────────────────────────────────────────────────────


def test_address_accepts_numbers_and_punctuation():
    assert _AddressModel(address="42, MG Road, Block A").address == "42, MG Road, Block A"


# ── Nested validation ───────────────────────────────────────────────────────────


def test_validate_nested_strings_accepts_clean():
    data = {"responses": [{"answer": "yes"}, {"answer": "no"}]}
    assert validate_nested_strings(data) == data


def test_validate_nested_strings_rejects_script():
    with pytest.raises(ValidationError):
        validate_nested_strings({"answer": "<script>alert(1)</script>"})


def test_validate_nested_strings_rejects_deep_nesting():
    obj: dict = {"a": "ok"}
    current = obj
    for _ in range(12):
        current["child"] = {"a": "ok"}
        current = current["child"]
    with pytest.raises(ValidationError):
        validate_nested_strings(obj)
