"""Resolve CSV directory and load diagnostics reference data from exported CSV files.

Default directory: parent of ``dev-api`` (workspace root), e.g. ``D:/supershyft/`` when
``dev-api`` lives at ``D:/supershyft/dev-api``.

Override with env ``DIAGNOSTICS_CSV_DIR`` (absolute path to folder containing the CSVs).
"""

from __future__ import annotations

import os
from pathlib import Path


def resolve_diagnostics_csv_dir() -> Path:
    explicit = os.environ.get("DIAGNOSTICS_CSV_DIR", "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    # db/seed/diagnostics_csv.py -> parents[2] == dev-api, parents[3] == workspace root
    return Path(__file__).resolve().parents[3]
