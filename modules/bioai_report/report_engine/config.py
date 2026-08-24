"""Central configuration for BioReport content assembly and render limits.

All array truncation and version labels live here so builders stay free of
hardcoded magic numbers.
"""

from __future__ import annotations

# --- Content limits (frontend must not slice) ---
TOP_LIFESTYLE_TIPS = 3
TOP_DIET_TIPS = 3
TOP_FOODS = 5
TOP_EXERCISE = 3
TOP_MONITORING = 2
TOP_ACTIONABLE_INSIGHTS = 5
TOP_STRENGTHS = 3
TOP_RISKS = 3

# --- Version metadata ---
ENGINE_VERSION = "1.0.0"
TEMPLATE_VERSION = "1.0.0"
