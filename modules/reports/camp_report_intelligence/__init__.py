"""Camp Report Intelligence Engine — public API.

Production code loads the single-file engine. ``intelligence_src/`` is the
readable multi-file source that engine is flattened from.
"""

from .camp_intelligence_engine import (
    INTELLIGENCE_CAMP_SECTIONS,
    enrich_camp_report_with_intelligence,
    generate_report_insights,
)
from .intelligence_src.assembly import (
    enrich_camp_report_section_with_intelligence,
    generate_camp_section_intelligence,
    resolve_intelligence_section,
)

__all__ = [
    "INTELLIGENCE_CAMP_SECTIONS",
    "enrich_camp_report_with_intelligence",
    "enrich_camp_report_section_with_intelligence",
    "generate_camp_section_intelligence",
    "generate_report_insights",
    "resolve_intelligence_section",
]
