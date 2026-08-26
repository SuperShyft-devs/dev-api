#!/usr/bin/env python3
"""End-to-end CLI runner for the packaged Camp Report Intelligence Engine.

Usage::

    python test_intelligence.py ./sample_camp_report.json
    python test_intelligence.py ./sample_camp_report.json -o ./output.json
    python test_intelligence.py ./sample_camp_report.json --no-print-json

Loads a Camp Report JSON file, calls the single-file production engine
``enrich_camp_report_with_intelligence``, validates structure preservation,
prints a summary, and writes an enriched JSON file (never overwrites input).

This script does not implement intelligence logic.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_DIR = Path(__file__).resolve().parent
if str(_DIR) not in sys.path:
    sys.path.insert(0, str(_DIR))

from camp_intelligence_engine import (  # noqa: E402
    INTELLIGENCE_CAMP_SECTIONS,
    enrich_camp_report_with_intelligence,
)

FORBIDDEN_TOP_LEVEL = frozenset({"profile", "leadership_cards", "concerns", "positives"})


def _load_report(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise SystemExit(f"Input must be a JSON object (got {type(payload).__name__})")
    return payload


def _default_output_path(input_path: Path) -> Path:
    return input_path.with_name(f"{input_path.stem}_enriched{input_path.suffix or '.json'}")


def _section_fields(section: Any) -> tuple[Any, Any, Any]:
    if not isinstance(section, dict):
        return None, None, None
    return section.get("data"), section.get("name"), section.get("description")


def validate_enrichment(original: dict[str, Any], enriched: dict[str, Any]) -> list[str]:
    """Return a list of validation failure messages (empty = pass)."""
    errors: list[str] = []

    if list(enriched.keys()) != list(original.keys()):
        errors.append(
            "top-level keys differ: "
            f"input={list(original.keys())!r} output={list(enriched.keys())!r}"
        )

    for key in FORBIDDEN_TOP_LEVEL:
        if key in enriched:
            errors.append(f"forbidden top-level key present: {key!r}")

    for key, original_section in original.items():
        enriched_section = enriched.get(key)
        o_data, o_name, o_desc = _section_fields(original_section)
        e_data, e_name, e_desc = _section_fields(enriched_section)

        if isinstance(original_section, dict) and "data" in original_section:
            if e_data != o_data:
                errors.append(f"{key}: data changed")
            if e_name != o_name:
                errors.append(f"{key}: name changed")
            if e_desc != o_desc:
                errors.append(f"{key}: description changed")

        if not isinstance(enriched_section, dict):
            continue

        has_intel = "intelligence" in enriched_section
        if key in INTELLIGENCE_CAMP_SECTIONS:
            if not has_intel:
                errors.append(f"{key}: expected intelligence on mapped section")
        else:
            if has_intel:
                errors.append(f"{key}: unexpected intelligence on unmapped section")

    return errors


def summarize(
    *,
    input_path: Path,
    output_path: Path,
    original: dict[str, Any],
    enriched: dict[str, Any],
    errors: list[str],
) -> None:
    with_intel: list[str] = []
    without_intel: list[str] = []
    preserved = True

    for key, original_section in original.items():
        enriched_section = enriched.get(key)
        if isinstance(enriched_section, dict) and "intelligence" in enriched_section:
            with_intel.append(key)
        else:
            without_intel.append(key)

        if isinstance(original_section, dict) and "data" in original_section:
            o_data, o_name, o_desc = _section_fields(original_section)
            e_data, e_name, e_desc = _section_fields(enriched_section)
            if (e_data, e_name, e_desc) != (o_data, o_name, o_desc):
                preserved = False

    print("=" * 72)
    print("Camp Report Intelligence — end-to-end enrichment run")
    print("=" * 72)
    print(f"Input file:              {input_path}")
    print(f"Output file:             {output_path}")
    print(f"Top-level sections:      {len(original)}")
    print(f"Sections with intel:     {len(with_intel)}")
    for key in with_intel:
        print(f"  + {key}")
    print(f"Sections without intel:  {len(without_intel)}")
    for key in without_intel:
        print(f"  - {key}")
    print(f"data/name/description:   {'preserved' if preserved else 'CHANGED'}")
    if errors:
        print(f"Validation:              FAILED ({len(errors)} issue(s))")
        for msg in errors:
            print(f"  ! {msg}")
    else:
        print("Validation:              PASSED")
    print("=" * 72)
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run enrich_camp_report_with_intelligence on a Camp Report JSON file.",
    )
    parser.add_argument(
        "camp_report_json",
        type=Path,
        help="Path to input Camp Report JSON (e.g. sample_camp_report.json)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output path (default: <input_stem>_enriched.json next to the input)",
    )
    parser.add_argument(
        "--no-print-json",
        action="store_true",
        help="Skip printing the full enriched JSON to stdout (still writes the file)",
    )
    args = parser.parse_args(argv)

    input_path = args.camp_report_json.expanduser().resolve()
    if not input_path.is_file():
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        return 1

    output_path = (
        args.output.expanduser().resolve()
        if args.output is not None
        else _default_output_path(input_path)
    )
    if output_path.resolve() == input_path.resolve():
        print("ERROR: output path must differ from the input file", file=sys.stderr)
        return 1

    original = _load_report(input_path)
    print(f"Running enrich_camp_report_with_intelligence on {input_path.name} …")
    enriched = enrich_camp_report_with_intelligence(original)
    errors = validate_enrichment(original, enriched)

    summarize(
        input_path=input_path,
        output_path=output_path,
        original=original,
        enriched=enriched,
        errors=errors,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(enriched, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    print(f"Wrote enriched JSON → {output_path}")

    if not args.no_print_json:
        print()
        print(json.dumps(enriched, indent=2, ensure_ascii=False))

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
