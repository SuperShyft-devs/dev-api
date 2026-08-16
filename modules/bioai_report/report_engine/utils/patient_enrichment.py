"""Extract and merge patient demographics into assessment payloads."""

from __future__ import annotations

from typing import Any

# Fields the BioReport patient block expects from assessment + profile sources.
PATIENT_DEMOGRAPHIC_FIELDS: tuple[str, ...] = (
    "name",
    "gender",
    "sex",
    "age",
    "date_of_birth",
    "height",
    "weight",
    "bmi",
    "profile_id",
)


def _unwrap_data(raw: dict[str, Any]) -> dict[str, Any]:
    if "diseases" in raw or "metabolic_score" in raw or "metabolic_age" in raw:
        return raw
    nested = raw.get("data")
    if isinstance(nested, dict):
        return nested
    return raw


def _as_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_optional_number(value: Any) -> float | int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            number = float(text)
        except ValueError:
            return None
        if number.is_integer():
            return int(number)
        return number
    return None


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def missing_demographic_fields(assessment: dict[str, Any]) -> list[str]:
    """Return demographic keys that are absent or blank on the assessment body.

    ``gender``/``sex`` and ``age``/``date_of_birth`` are treated as alternatives.
    ``profile_id`` is merged when available but does not gate enrichment — it is
    only needed to look up further demographics.
    """
    body = _unwrap_data(assessment) if isinstance(assessment, dict) else {}
    missing: list[str] = []
    for field in PATIENT_DEMOGRAPHIC_FIELDS:
        if field == "profile_id":
            continue
        if _is_blank(body.get(field)):
            if field == "sex" and not _is_blank(body.get("gender")):
                continue
            if field == "gender" and not _is_blank(body.get("sex")):
                continue
            if field == "date_of_birth" and not _is_blank(body.get("age")):
                continue
            if field == "age" and not _is_blank(body.get("date_of_birth")):
                continue
            missing.append(field)
    return missing


def needs_patient_enrichment(assessment: dict[str, Any]) -> bool:
    return bool(missing_demographic_fields(assessment))


def normalize_gender_label(raw: Any) -> str | None:
    """Map MetSights gender codes / labels to a stable lowercase string."""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        code = int(raw)
        if code == 1:
            return "male"
        if code == 2:
            return "female"
        return str(code)
    text = str(raw).strip().lower()
    if not text:
        return None
    if text in {"1", "male", "m", "man"}:
        return "male"
    if text in {"2", "female", "f", "woman"}:
        return "female"
    return text


def format_person_name(
    *,
    first_name: Any = None,
    last_name: Any = None,
    full_name: Any = None,
) -> str | None:
    """Build a display name from profile name parts."""
    explicit = _as_optional_str(full_name)
    if explicit:
        return explicit
    first = _as_optional_str(first_name) or ""
    last = _as_optional_str(last_name) or ""
    combined = f"{first} {last}".strip()
    return combined or None


def _height_to_meters(height: float, unit: str | None) -> float | None:
    if height <= 0:
        return None
    normalized = (unit or "cm").strip().lower()
    if normalized in {"", "0", "cm"}:
        return height / 100.0
    if normalized in {"1", "m", "meter", "metres", "meters"}:
        return height
    if normalized in {"2", "in", "inch", "inches"}:
        return height * 0.0254
    if normalized in {"ft/in", "ft", "feet"}:
        return height * 0.0254 if height > 10 else height * 0.3048
    return height / 100.0


def _weight_to_kg(weight: float, unit: str | None) -> float | None:
    if weight <= 0:
        return None
    normalized = (unit or "kg").strip().lower()
    if normalized in {"", "0", "kg", "kgs", "kilogram", "kilograms"}:
        return weight
    if normalized in {"1", "lb", "lbs", "pound", "pounds"}:
        return weight * 0.45359237
    return weight


def compute_bmi(
    *,
    height: float | int | None,
    weight: float | int | None,
    height_unit: str | None = None,
    weight_unit: str | None = None,
) -> float | None:
    """Compute BMI when height + weight are available. Returns one decimal place."""
    if height is None or weight is None:
        return None
    try:
        h = float(height)
        w = float(weight)
    except (TypeError, ValueError):
        return None
    meters = _height_to_meters(h, height_unit)
    kg = _weight_to_kg(w, weight_unit)
    if meters is None or kg is None or meters <= 0:
        return None
    return round(kg / (meters * meters), 1)


def extract_demographics_from_profile(profile: dict[str, Any] | None) -> dict[str, Any]:
    """Pull name / gender / sex / age / DOB from a MetSights profile payload."""
    if not isinstance(profile, dict):
        return {}
    out: dict[str, Any] = {}
    name = format_person_name(
        first_name=profile.get("first_name"),
        last_name=profile.get("last_name"),
        full_name=profile.get("name"),
    )
    if name:
        out["name"] = name
    gender = normalize_gender_label(
        profile.get("gender") if profile.get("gender") is not None else profile.get("sex")
    )
    if gender:
        out["gender"] = gender
        out["sex"] = gender
    age = _as_optional_number(profile.get("age"))
    if age is not None:
        out["age"] = age
    dob = _as_optional_str(profile.get("date_of_birth"))
    if dob:
        out["date_of_birth"] = dob
    profile_id = _as_optional_str(profile.get("id")) or _as_optional_str(profile.get("profile_id"))
    if profile_id:
        out["profile_id"] = profile_id
    return out


def extract_demographics_from_physical(physical: dict[str, Any] | None) -> dict[str, Any]:
    """Pull height / weight / bmi from a physical-measurement payload."""
    if not isinstance(physical, dict):
        return {}
    out: dict[str, Any] = {}
    height = _as_optional_number(physical.get("height"))
    weight = _as_optional_number(physical.get("weight"))
    bmi = _as_optional_number(physical.get("bmi"))
    height_unit = _as_optional_str(physical.get("height_unit"))
    weight_unit = _as_optional_str(physical.get("weight_unit"))
    if height is not None:
        out["height"] = height
        if height_unit:
            out["height_unit"] = height_unit
    if weight is not None:
        out["weight"] = weight
        if weight_unit:
            out["weight_unit"] = weight_unit
    if bmi is not None:
        out["bmi"] = bmi
    elif height is not None and weight is not None:
        computed = compute_bmi(
            height=height,
            weight=weight,
            height_unit=height_unit,
            weight_unit=weight_unit,
        )
        if computed is not None:
            out["bmi"] = computed
    return out


def extract_demographics_from_record(record: dict[str, Any] | None) -> dict[str, Any]:
    """Extract demographics nested on a MetSights record-detail payload."""
    if not isinstance(record, dict):
        return {}
    out: dict[str, Any] = {}

    name = format_person_name(
        first_name=record.get("first_name"),
        last_name=record.get("last_name"),
        full_name=record.get("name"),
    )
    if name:
        out["name"] = name
    gender = normalize_gender_label(
        record.get("gender") if record.get("gender") is not None else record.get("sex")
    )
    if gender:
        out["gender"] = gender
        out["sex"] = gender
    age = _as_optional_number(record.get("age"))
    if age is not None:
        out["age"] = age

    # Nested profile / demography blocks (MetSights record detail shape).
    for key in ("profile", "demography"):
        block = record.get(key)
        if isinstance(block, dict):
            for field, value in extract_demographics_from_profile(block).items():
                out.setdefault(field, value)

    profile_id = (
        _as_optional_str(record.get("profile_id"))
        or _as_optional_str(out.get("profile_id"))
    )
    if profile_id:
        out["profile_id"] = profile_id

    physical = record.get("physical_measurement") or record.get("physical-measurement")
    if isinstance(physical, dict):
        for field, value in extract_demographics_from_physical(physical).items():
            out.setdefault(field, value)

    return out


def resolve_profile_id(
    record: dict[str, Any] | None,
    assessment: dict[str, Any] | None = None,
) -> str | None:
    """Find a MetSights profile id from record detail and/or assessment JSON."""
    for source in (record, assessment):
        if not isinstance(source, dict):
            continue
        body = _unwrap_data(source)
        for key in ("profile_id", "metsights_profile_id"):
            value = _as_optional_str(body.get(key)) or _as_optional_str(source.get(key))
            if value:
                return value
        profile = body.get("profile") or source.get("profile")
        if isinstance(profile, dict):
            value = _as_optional_str(profile.get("id")) or _as_optional_str(profile.get("profile_id"))
            if value:
                return value
    return None


def extract_patient_identifiers(
    assessment: dict[str, Any] | None,
    *,
    record_id: str | None = None,
) -> dict[str, str]:
    """Pull user_id / profile_id / patient_id / record_id hints from assessment JSON."""
    ids: dict[str, str] = {}
    if record_id and str(record_id).strip():
        ids["record_id"] = str(record_id).strip()
    if not isinstance(assessment, dict):
        return ids
    body = _unwrap_data(assessment)
    sources = (body, assessment)
    mapping = {
        "user_id": ("user_id", "patient_user_id"),
        "patient_id": ("patient_id",),
        "profile_id": ("profile_id", "metsights_profile_id"),
        "record_id": ("record_id", "record", "id"),
    }
    for out_key, candidates in mapping.items():
        if out_key in ids:
            continue
        for source in sources:
            for key in candidates:
                value = _as_optional_str(source.get(key))
                if value:
                    ids[out_key] = value
                    break
            if out_key in ids:
                break
        if out_key == "profile_id" and out_key not in ids:
            profile = body.get("profile")
            if isinstance(profile, dict):
                value = _as_optional_str(profile.get("id")) or _as_optional_str(profile.get("profile_id"))
                if value:
                    ids["profile_id"] = value
    return ids


def extract_scale_answer(answer: Any) -> tuple[float | int | None, str | None]:
    """Parse questionnaire scale answers ``{value, unit}`` or plain numerics."""
    if isinstance(answer, dict):
        value = _as_optional_number(answer.get("value"))
        unit = _as_optional_str(answer.get("unit"))
        return value, unit
    return _as_optional_number(answer), None


def extract_demographics_from_user(user: Any) -> dict[str, Any]:
    """Map a local ``User`` ORM/row object into demographic fields."""
    if user is None:
        return {}
    out: dict[str, Any] = {}
    name = format_person_name(
        first_name=getattr(user, "first_name", None),
        last_name=getattr(user, "last_name", None),
        full_name=getattr(user, "name", None),
    )
    if name:
        out["name"] = name
    gender = normalize_gender_label(getattr(user, "gender", None) or getattr(user, "sex", None))
    if gender:
        out["gender"] = gender
        out["sex"] = gender
    age = _as_optional_number(getattr(user, "age", None))
    if age is not None:
        out["age"] = age
    user_id = getattr(user, "user_id", None)
    if user_id is not None:
        out["user_id"] = user_id
    return out


def extract_demographics_from_questionnaire(lookup: dict[str, Any] | None) -> dict[str, Any]:
    """Map questionnaire answer lookup (height/weight/bmi keys) into demographics."""
    if not isinstance(lookup, dict):
        return {}
    out: dict[str, Any] = {}

    height, height_unit = extract_scale_answer(lookup.get("height"))
    weight, weight_unit = extract_scale_answer(lookup.get("weight"))
    bmi, _ = extract_scale_answer(lookup.get("bmi"))

    if height is not None:
        out["height"] = height
        if height_unit:
            out["height_unit"] = height_unit
    if weight is not None:
        out["weight"] = weight
        if weight_unit:
            out["weight_unit"] = weight_unit
    if bmi is not None:
        out["bmi"] = bmi
    elif height is not None and weight is not None:
        computed = compute_bmi(
            height=height,
            weight=weight,
            height_unit=height_unit,
            weight_unit=weight_unit,
        )
        if computed is not None:
            out["bmi"] = computed

    gender = normalize_gender_label(lookup.get("gender") or lookup.get("sex"))
    if gender:
        out.setdefault("gender", gender)
        out.setdefault("sex", gender)
    return out


def merge_patient_into_assessment(
    assessment: dict[str, Any],
    patient: dict[str, Any],
) -> dict[str, Any]:
    """Fill blank demographic fields on assessment from patient data (assessment wins)."""
    if not isinstance(assessment, dict):
        raise TypeError("assessment must be a dict")
    if not isinstance(patient, dict) or not patient:
        return dict(assessment)

    merged = dict(assessment)
    if "diseases" in merged or "metabolic_score" in merged or "metabolic_age" in merged:
        target = merged
    else:
        nested = merged.get("data")
        if isinstance(nested, dict):
            target = dict(nested)
            merged["data"] = target
        else:
            target = merged

    for field in PATIENT_DEMOGRAPHIC_FIELDS:
        if not _is_blank(target.get(field)):
            continue
        incoming = patient.get(field)
        if _is_blank(incoming):
            continue
        target[field] = incoming

    if _is_blank(target.get("sex")) and not _is_blank(target.get("gender")):
        target["sex"] = target["gender"]
    if _is_blank(target.get("gender")) and not _is_blank(target.get("sex")):
        target["gender"] = target["sex"]

    if _is_blank(target.get("bmi")):
        computed = compute_bmi(
            height=_as_optional_number(target.get("height")),
            weight=_as_optional_number(target.get("weight")),
            height_unit=_as_optional_str(target.get("height_unit"))
            or _as_optional_str(patient.get("height_unit")),
            weight_unit=_as_optional_str(target.get("weight_unit"))
            or _as_optional_str(patient.get("weight_unit")),
        )
        if computed is not None:
            target["bmi"] = computed

    return merged
