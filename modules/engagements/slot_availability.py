"""Generate cabin time slots and occupancy-aware public slot_detail."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any

from core.exceptions import AppError

SLOT_UNAVAILABLE_CODE = "SLOT_UNAVAILABLE"
SLOT_UNAVAILABLE_MESSAGE = "No such Slot Available"

OccupancyMap = dict[tuple[str, str, str], int]


def slot_unavailable() -> AppError:
    return AppError(
        status_code=400,
        error_code=SLOT_UNAVAILABLE_CODE,
        message=SLOT_UNAVAILABLE_MESSAGE,
    )


def slot_detail_is_configured(slot_detail: Any) -> bool:
    return bool(slot_detail)


def format_hhmm(value: time) -> str:
    return f"{value.hour:02d}:{value.minute:02d}"


def parse_hhmm(value: str) -> time | None:
    text = (value or "").strip()
    if not text:
        return None
    parts = text.split(":")
    if len(parts) not in (2, 3):
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return None
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return time(hour=hour, minute=minute)


def coerce_time(value: Any) -> time | None:
    if value is None:
        return None
    if isinstance(value, time):
        return time(hour=value.hour, minute=value.minute)
    if isinstance(value, datetime):
        return time(hour=value.hour, minute=value.minute)
    if isinstance(value, str):
        return parse_hhmm(value)
    return None


def occupancy_key(cabin_key: str, slot_date: date, slot_time: time) -> tuple[str, str, str]:
    return (cabin_key, slot_date.isoformat(), format_hhmm(slot_time))


def occupancy_map_from_rows(rows: list[tuple]) -> OccupancyMap:
    occupancy: OccupancyMap = {}
    for cabin_key, slot_date, slot_time, count in rows:
        key_text = (cabin_key or "").strip()
        parsed_time = coerce_time(slot_time)
        if not key_text or slot_date is None or parsed_time is None:
            continue
        occupancy[occupancy_key(key_text, slot_date, parsed_time)] = int(count or 0)
    return occupancy


def generate_slot_starts(
    start_time: Any,
    end_time: Any,
    slot_duration: int,
    breaks: Any = None,
) -> list[time]:
    start = coerce_time(start_time)
    end = coerce_time(end_time)
    duration_minutes = int(slot_duration or 0)
    if start is None or end is None or duration_minutes <= 0 or end <= start:
        return []

    duration = timedelta(minutes=duration_minutes)
    break_windows: list[tuple[time, time]] = []
    for raw_break in breaks or []:
        if isinstance(raw_break, dict):
            br_start = coerce_time(raw_break.get("start_time"))
            br_end = coerce_time(raw_break.get("end_time"))
        else:
            br_start = coerce_time(getattr(raw_break, "start_time", None))
            br_end = coerce_time(getattr(raw_break, "end_time", None))
        if br_start is None or br_end is None or br_end <= br_start:
            continue
        break_windows.append((br_start, br_end))

    slots: list[time] = []
    current = datetime.combine(date.min, start)
    end_dt = datetime.combine(date.min, end)
    while current + duration <= end_dt:
        slot_t = time(hour=current.hour, minute=current.minute)
        in_break = any(br_start <= slot_t < br_end for br_start, br_end in break_windows)
        if not in_break:
            slots.append(slot_t)
        current += duration
    return slots


def _cabin_is_active(cabin: dict[str, Any]) -> bool:
    return cabin.get("is_active", True) is not False


def normalize_date_entry(raw: Any) -> dict[str, Any]:
    """Normalize a date value to ``{is_enable, cabins}`` (legacy list → enabled)."""
    if isinstance(raw, list):
        return {"is_enable": True, "cabins": raw}
    if isinstance(raw, dict) and ("cabins" in raw or "is_enable" in raw):
        cabins = raw.get("cabins")
        return {
            "is_enable": raw.get("is_enable", True) is not False,
            "cabins": cabins if isinstance(cabins, list) else [],
        }
    return {"is_enable": True, "cabins": []}


def date_entry_is_enabled(entry: dict[str, Any] | None) -> bool:
    if entry is None:
        return True
    return entry.get("is_enable", True) is not False


def get_cabins_from_date_entry(entry: dict[str, Any] | None) -> list[Any]:
    if entry is None:
        return []
    cabins = entry.get("cabins")
    return cabins if isinstance(cabins, list) else []


def get_date_entry(section_data: Any, date_key: str) -> dict[str, Any] | None:
    if not isinstance(section_data, dict):
        return None
    raw = section_data.get(date_key)
    if raw is None:
        return None
    return normalize_date_entry(raw)


def iter_section_date_cabins(section: Any) -> list[tuple[str, dict[str, Any], list[Any]]]:
    """Yield ``(date_key, entry, cabins)`` for each date in a section."""
    if not isinstance(section, dict):
        return []
    result: list[tuple[str, dict[str, Any], list[Any]]] = []
    for date_key, raw in section.items():
        entry = normalize_date_entry(raw)
        result.append((str(date_key), entry, get_cabins_from_date_entry(entry)))
    return result


def resolve_consultation_cabin_display_name(
    slot_detail: Any,
    *,
    consultation_date: date,
    cabin_key: str | None,
) -> str | None:
    """Return cabin_name for display; fall back to cabin_key when name is unavailable."""
    key = (cabin_key or "").strip()
    if not key:
        return None
    cabin = find_active_cabin(
        slot_detail,
        section="consultation",
        date_key=consultation_date.isoformat(),
        cabin_key=key,
    )
    if cabin is None:
        return key
    name = (cabin.get("cabin_name") or "").strip()
    return name or key


def find_active_cabin(
    slot_detail: Any,
    *,
    section: str,
    date_key: str,
    cabin_key: str,
) -> dict[str, Any] | None:
    if not isinstance(slot_detail, dict):
        return None
    section_data = slot_detail.get(section) or {}
    if not isinstance(section_data, dict):
        return None
    entry = get_date_entry(section_data, date_key)
    if entry is None or not date_entry_is_enabled(entry):
        return None
    cabins = get_cabins_from_date_entry(entry)
    wanted = (cabin_key or "").strip()
    if not wanted:
        return None
    for cabin in cabins:
        if not isinstance(cabin, dict):
            continue
        if (cabin.get("cabin_key") or "").strip() != wanted:
            continue
        if not _cabin_is_active(cabin):
            return None
        return cabin
    return None


def require_available_blood_collection_slot(
    slot_detail: Any,
    *,
    collection_date: date,
    cabin_key: str | None,
    slot_time: time,
) -> dict[str, Any]:
    date_key = collection_date.isoformat()
    cabin = find_active_cabin(
        slot_detail,
        section="blood_collection",
        date_key=date_key,
        cabin_key=cabin_key or "",
    )
    if cabin is None:
        raise slot_unavailable()

    starts = generate_slot_starts(
        cabin.get("start_time"),
        cabin.get("end_time"),
        int(cabin.get("slot_duration") or 0),
        cabin.get("breaks") or [],
    )
    wanted = coerce_time(slot_time)
    if wanted is None or wanted not in starts:
        raise slot_unavailable()

    try:
        capacity = int(cabin.get("capacity_per_slot") or 0)
    except (TypeError, ValueError) as exc:
        raise slot_unavailable() from exc
    if capacity <= 0:
        raise slot_unavailable()
    return cabin


def require_available_consultation_slot(
    slot_detail: Any,
    *,
    expert_type: str,
    consultation_date: date,
    cabin_key: str | None,
    slot_time: time,
) -> dict[str, Any]:
    date_key = consultation_date.isoformat()
    cabin = find_active_cabin(
        slot_detail,
        section="consultation",
        date_key=date_key,
        cabin_key=cabin_key or "",
    )
    if cabin is None:
        raise slot_unavailable()

    wanted_type = (expert_type or "").strip()
    cabin_type = (cabin.get("expert_type") or "").strip()
    if not wanted_type or cabin_type != wanted_type:
        raise slot_unavailable()

    starts = generate_slot_starts(
        cabin.get("start_time"),
        cabin.get("end_time"),
        int(cabin.get("slot_duration") or 0),
        cabin.get("breaks") or [],
    )
    wanted = coerce_time(slot_time)
    if wanted is None or wanted not in starts:
        raise slot_unavailable()

    try:
        capacity = int(cabin.get("capacity_per_slot") or 0)
    except (TypeError, ValueError) as exc:
        raise slot_unavailable() from exc
    if capacity <= 0:
        raise slot_unavailable()
    return cabin


def build_public_slot_detail(
    slot_detail: Any,
    occupancy: OccupancyMap | None = None,
    consultation_occupancy: OccupancyMap | None = None,
) -> dict[str, Any] | None:
    if not slot_detail_is_configured(slot_detail) or not isinstance(slot_detail, dict):
        return None

    occupancy = occupancy or {}
    consultation_occupancy = consultation_occupancy or {}
    result: dict[str, Any] = {}
    for section_key in ("blood_collection", "consultation"):
        section = slot_detail.get(section_key)
        if not isinstance(section, dict) or not section:
            continue
        public_section: dict[str, dict[str, Any]] = {}
        for date_key, entry, cabins in iter_section_date_cabins(section):
            if not date_entry_is_enabled(entry):
                public_section[str(date_key)] = {"is_enable": False, "cabins": []}
                continue
            try:
                slot_date = date.fromisoformat(str(date_key))
            except ValueError:
                continue
            public_cabins: list[dict[str, Any]] = []
            for cabin in cabins:
                if not isinstance(cabin, dict) or not _cabin_is_active(cabin):
                    continue
                duration = int(cabin.get("slot_duration") or 0)
                try:
                    capacity = int(cabin.get("capacity_per_slot") or 0)
                except (TypeError, ValueError):
                    capacity = 0
                cabin_key = (cabin.get("cabin_key") or "").strip()
                starts = generate_slot_starts(
                    cabin.get("start_time"),
                    cabin.get("end_time"),
                    duration,
                    cabin.get("breaks") or [],
                )
                section_occupancy = consultation_occupancy if section_key == "consultation" else occupancy
                available_slots = []
                for slot_t in starts:
                    count = section_occupancy.get(occupancy_key(cabin_key, slot_date, slot_t), 0)
                    available_slots.append(
                        {
                            "slot": format_hhmm(slot_t),
                            "spot_left": max(0, capacity - int(count)),
                        }
                    )
                public_cabin: dict[str, Any] = {
                    "cabin_name": cabin.get("cabin_name") or "",
                    "cabin_key": cabin_key,
                    "slot_duration": duration,
                    "available_slots": available_slots,
                }
                if section_key == "consultation":
                    public_cabin["expert_type"] = (cabin.get("expert_type") or "").strip()
                public_cabins.append(public_cabin)
            public_section[str(date_key)] = {"is_enable": True, "cabins": public_cabins}
        if public_section:
            result[section_key] = public_section
    return result
