"""Diagnostics seed rows not covered by CSV exports (idempotent upserts)."""

from __future__ import annotations

from db.seed.diagnostics_operations import SeedDiagSample

DIAG_SAMPLES: tuple[SeedDiagSample, ...] = (
    SeedDiagSample(1, 15, "Blood", None, 1),
)
