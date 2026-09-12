"""Healthians / diagnostic ``parameter_key`` → Metsights ``question_key`` aliases.

Single source of truth for draft, read, and admin visualization.
"""

from __future__ import annotations

# Healthians catalog / live IHR key → Metsights OPTIONS field name
HEALTHIANS_TO_METSIGHTS_PARAMETER_KEY: dict[str, str] = {
    # hormones (PCOS-critical)
    "lh": "lh_value",
    "fsh": "fsh_value",
    "total_testosterone": "testosterone",
    # spelling / naming gaps
    "hemoglobin": "haemoglobin",
    "wbc": "wbc_value",
    # liver
    "alp": "alkaline_phosphatase",
    "sgot_ast": "ast_value",
    "sgpt_alt": "alt_value",
    "bilirubin_total": "total_bilirubin",
    "bilirubin_direct": "direct_bilirubin",
    "ggpt_value": "ggt_value",
    "lactate_dehydrogenase": "ldh_value",
    "proteins_serum": "total_protein",
    # kidney / metabolic
    "bun_urea_nitrogen": "bun_value",
    "egfr": "egfr_value",
    "calcium_total": "calcium",
    "fasting_sugar": "glucose_fasting",
    "post_prandial_sugar": "glucose_random",
    "hba1c": "glycated_haemoglobin",
    # lipids
    "cholesterol_total": "total_cholesterol",
    "hdl_cholestrol_direct": "hdlc_value",
    "ldl_cholestrol": "ldlc_value",
    # thyroid
    "t3": "triiodothyronine",
    "t4": "thyroxine",
    "tsh": "tsh_value",
    # inflammation / special
    "crp": "crp_value",
    "hs-crp": "hscrp_value",
    "esr": "esr_value",
    "ck/cpk": "cpk_value",
    "pt_inr": "ptinr_value",
    "vitamin_d_total-25_hydroxy": "vitamin_d",
    # CBC
    "rbc": "rbc_count",
    "red_blood_cells": "rbc_count",
    "platelate_count": "platelets",
    "mpv_mean_platelate_count": "mpv_value",
}

# Reverse lookup (Metsights → list of Healthians keys) for admin viz
METSIGHTS_TO_HEALTHIANS_PARAMETER_KEYS: dict[str, tuple[str, ...]] = {}
for _src, _dst in HEALTHIANS_TO_METSIGHTS_PARAMETER_KEY.items():
    METSIGHTS_TO_HEALTHIANS_PARAMETER_KEYS.setdefault(_dst, ())
    existing = METSIGHTS_TO_HEALTHIANS_PARAMETER_KEYS[_dst]
    METSIGHTS_TO_HEALTHIANS_PARAMETER_KEYS[_dst] = (*existing, _src)


def resolve_metsights_parameter_key(parameter_key: str) -> str:
    """Map a Healthians/diagnostic key to Metsights question_key when aliased."""
    key = (parameter_key or "").strip()
    if not key:
        return key
    return HEALTHIANS_TO_METSIGHTS_PARAMETER_KEY.get(key, key)
