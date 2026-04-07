"""Resolve the diagnostics reference CSV directory (checked-in exports under dev-api)."""

from __future__ import annotations

from pathlib import Path


def resolve_diagnostics_csv_dir() -> Path:
    # db/seed/diagnostics_csv.py -> parents[2] == dev-api root
    return (Path(__file__).resolve().parents[2] / "db" / "seed" / "csv").resolve()
