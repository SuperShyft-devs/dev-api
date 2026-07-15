"""On-the-fly consultation slot calculation from weekly availability + overrides."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta, time
from typing import Any

from modules.experts.models import Expert, ExpertAvailabilityModel, ExpertAvailabilityOverrideModel


def _time_to_minutes(t: time) -> int:
    return t.hour * 60 + t.minute


def _minutes_to_hhmm(m: int) -> str:
    return f"{m // 60:02d}:{m % 60:02d}"


def _subtract_intervals(
    windows: list[tuple[int, int]],
    blocked: list[tuple[int, int]],
) -> list[tuple[int, int]]:
    result = list(windows)
    for b_start, b_end in blocked:
        next_result: list[tuple[int, int]] = []
        for w_start, w_end in result:
            if b_end <= w_start or b_start >= w_end:
                next_result.append((w_start, w_end))
                continue
            if w_start < b_start:
                next_result.append((w_start, b_start))
            if b_end < w_end:
                next_result.append((b_end, w_end))
        result = next_result
    return result


def bookable_starts_in_window(
    window_start: int,
    window_end: int,
    slot_duration: int,
    buffer_time: int,
) -> list[int]:
    stride = max(5, slot_duration + max(0, buffer_time))
    starts: list[int] = []
    m = window_start
    while m + slot_duration <= window_end:
        starts.append(m)
        m += stride
    return starts


def compute_expert_day_slots(
    *,
    day: date,
    blocks: list[ExpertAvailabilityModel],
    overrides: list[ExpertAvailabilityOverrideModel],
    default_duration: int,
) -> list[tuple[str, int]]:
    """Return list of (start_time HH:MM, duration) for one expert on one day."""
    day_overrides = [o for o in overrides if o.override_date == day]
    available_ovs = [o for o in day_overrides if (o.status or "").lower() == "available"]
    unavailable_ovs = [o for o in day_overrides if (o.status or "").lower() == "unavailable"]
    booked_ovs = [o for o in day_overrides if (o.status or "").lower() == "booked"]

    weekday = day.weekday()  # Mon=0..Sun=6
    # Admin UI uses Sunday=0 … Saturday=6 (JS getDay)
    day_of_week = (weekday + 1) % 7

    weekly = [b for b in blocks if b.day_of_week == day_of_week]

    if available_ovs:
        windows: list[tuple[int, int, int, int]] = []
        for o in available_ovs:
            if o.start_time is None or o.end_time is None:
                continue
            duration = default_duration
            if weekly:
                duration = weekly[0].slot_duration or default_duration
            buffer = o.buffer_time if o.buffer_time is not None else (
                weekly[0].buffer_time if weekly else 5
            )
            windows.append(
                (
                    _time_to_minutes(o.start_time),
                    _time_to_minutes(o.end_time),
                    duration,
                    buffer if buffer is not None else 5,
                )
            )
    else:
        windows = []
        for b in weekly:
            windows.append(
                (
                    _time_to_minutes(b.start_time),
                    _time_to_minutes(b.end_time),
                    b.slot_duration or default_duration,
                    b.buffer_time if b.buffer_time is not None else 5,
                )
            )

    blocked = [
        (_time_to_minutes(o.start_time), _time_to_minutes(o.end_time))
        for o in unavailable_ovs
        if o.start_time is not None and o.end_time is not None
    ]

    # Group windows by (duration, buffer) after subtracting unavailable ranges
    starts_out: list[tuple[str, int]] = []
    booked_starts = {
        _time_to_minutes(o.start_time)
        for o in booked_ovs
        if o.start_time is not None
    }

    for w_start, w_end, duration, buffer in windows:
        open_ranges = _subtract_intervals([(w_start, w_end)], blocked)
        for r_start, r_end in open_ranges:
            for start_m in bookable_starts_in_window(r_start, r_end, duration, buffer):
                if start_m in booked_starts:
                    continue
                starts_out.append((_minutes_to_hhmm(start_m), duration))

    # Dedupe by start_time (prefer first duration)
    seen: set[str] = set()
    unique: list[tuple[str, int]] = []
    for start, duration in sorted(starts_out, key=lambda x: x[0]):
        if start in seen:
            continue
        seen.add(start)
        unique.append((start, duration))
    return unique


def aggregate_slots(
    expert_slots: list[tuple[str, str, int]],
) -> dict[str, list[dict[str, Any]]]:
    """Aggregate (date_iso, start_time, duration) across experts into date → slots with counts."""
    buckets: dict[tuple[str, str, int], int] = defaultdict(int)
    for date_iso, start_time, duration in expert_slots:
        buckets[(date_iso, start_time, duration)] += 1

    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (date_iso, start_time, duration), count in sorted(buckets.items(), key=lambda x: (x[0][0], x[0][1])):
        by_date[date_iso].append(
            {
                "start_time": start_time,
                "duration": duration,
                "available_slot": count,
            }
        )
    return dict(by_date)


def next_n_days(n: int = 7, *, start: date | None = None) -> list[date]:
    base = start or date.today()
    return [base + timedelta(days=i) for i in range(n)]


def is_slot_available_for_expert(
    *,
    day: date,
    slot_hhmm: str,
    blocks: list[ExpertAvailabilityModel],
    overrides: list[ExpertAvailabilityOverrideModel],
    default_duration: int,
) -> bool:
    slots = compute_expert_day_slots(
        day=day,
        blocks=blocks,
        overrides=overrides,
        default_duration=default_duration,
    )
    return any(start == slot_hhmm for start, _ in slots)


def expert_effective_on(expert: Expert, day: date) -> bool:
    if (expert.status or "").lower() != "active":
        return False
    if expert.effective_from and day < expert.effective_from:
        return False
    if expert.effective_until and day > expert.effective_until:
        return False
    return True


def parse_slot_time(slot: str) -> time:
    parts = slot.strip().split(":")
    hour = int(parts[0])
    minute = int(parts[1])
    return time(hour=hour, minute=minute)
