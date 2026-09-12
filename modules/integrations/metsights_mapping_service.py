"""Read-only Metsights blood mapping metadata for admin visualization."""

from __future__ import annotations

from typing import Any

from db.seed.blood_parameter_key_aliases import (
    HEALTHIANS_TO_METSIGHTS_PARAMETER_KEY,
    METSIGHTS_TO_HEALTHIANS_PARAMETER_KEYS,
)
from db.seed.blood_parameters_registry import (
    ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
    ADVANCED_BLOOD_PARAMETER_FIELDS,
    BLOOD_PARAMETER_CATEGORY_KEY,
    BLOOD_PARAMETER_FIELDS,
    BLOOD_PARAMETER_INTERNAL_FALLBACKS,
    FIELD_BY_KEY,
    METSIGHTS_PACKAGE_BLOOD_CATEGORIES,
    PRO_FEMALE_HORMONE_PLACEHOLDERS,
)
from db.seed.metsights_sync_registry import PACKAGE_METSIGHTS_CATEGORY_LINKS
from common.blood_unit_normalizer import UNIT_SYNONYM_EXAMPLES


def build_metsights_blood_mapping_payload() -> dict[str, Any]:
    """Return static mapping configuration for admin UI."""
    key_aliases = [
        {
            "healthians_key": src,
            "metsights_key": dst,
            "catalog_unit": None,
            "metsights_units": [
                {"code": code, "label": label}
                for code, label in (FIELD_BY_KEY.get(dst).units if dst in FIELD_BY_KEY else ())
            ],
        }
        for src, dst in sorted(HEALTHIANS_TO_METSIGHTS_PARAMETER_KEY.items())
    ]

    hormone_placeholders = [
        {
            "question_key": key,
            "value": value,
            "unit_code": unit_code,
            "when": "METSIGHTS_PRO + female + Healthians value missing",
        }
        for key, (value, unit_code) in PRO_FEMALE_HORMONE_PLACEHOLDERS.items()
    ]

    internal_fallbacks = [
        {
            "question_key": key,
            "value": value,
            "unit_code": unit_code,
            "when": "Full report push retry; missing mandatory field",
        }
        for key, (value, unit_code) in sorted(BLOOD_PARAMETER_INTERNAL_FALLBACKS.items())
    ]

    package_matrix = []
    for package_code in ("METSIGHTS_BASIC", "METSIGHTS_PRO"):
        package_matrix.append(
            {
                "package_code": package_code,
                "assessment_type_code": "1" if package_code == "METSIGHTS_BASIC" else "2",
                "blood_categories": list(METSIGHTS_PACKAGE_BLOOD_CATEGORIES.get(package_code, ())),
                "all_metsights_categories": PACKAGE_METSIGHTS_CATEGORY_LINKS.get(package_code, []),
                "hormones_required": package_code == "METSIGHTS_PRO",
            }
        )

    return {
        "flow": [
            "Healthians getBookingDigitalValue",
            "build_grouped_from_healthians → IHR parameter_key + unit string",
            "build_parameter_value_map + key aliases",
            "draft_blood_parameters_from_report → unit → OPTIONS code",
            "questionnaire_responses",
            "push_scale_emit → Metsights blood / advanced-blood",
        ],
        "package_matrix": package_matrix,
        "key_aliases": key_aliases,
        "reverse_aliases": {
            k: list(v) for k, v in sorted(METSIGHTS_TO_HEALTHIANS_PARAMETER_KEYS.items())
        },
        "unit_synonyms": UNIT_SYNONYM_EXAMPLES,
        "hormone_placeholders": hormone_placeholders,
        "internal_fallbacks": internal_fallbacks,
        "category_keys": {
            "blood": BLOOD_PARAMETER_CATEGORY_KEY,
            "advanced_blood": ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
        },
        "counts": {
            "blood_fields": len(BLOOD_PARAMETER_FIELDS),
            "advanced_fields": len(ADVANCED_BLOOD_PARAMETER_FIELDS),
            "key_aliases": len(HEALTHIANS_TO_METSIGHTS_PARAMETER_KEY),
        },
    }
