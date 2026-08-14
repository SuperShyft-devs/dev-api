"""Compatibility shim.

The Camp Report Intelligence Engine lives in
``modules.reports.camp_report_intelligence``. This package remains as a
working multi-file copy; the public API re-exports the production engine.
"""

from modules.reports.camp_report_intelligence import (
    INTELLIGENCE_CAMP_SECTIONS,
    enrich_camp_report_with_intelligence,
    generate_report_insights,
)

__all__ = [
    "generate_report_insights",
    "enrich_camp_report_with_intelligence",
    "INTELLIGENCE_CAMP_SECTIONS",
]
