"""Camp Report Intelligence Engine — public API.

Production code loads the single-file engine. ``intelligence_src/`` is the
readable multi-file source that engine is flattened from.
"""

from .camp_intelligence_engine import (
    INTELLIGENCE_CAMP_SECTIONS,
    enrich_camp_report_with_intelligence,
    generate_report_insights,
)

__all__ = [
    "INTELLIGENCE_CAMP_SECTIONS",
    "enrich_camp_report_with_intelligence",
    "generate_report_insights",
]
