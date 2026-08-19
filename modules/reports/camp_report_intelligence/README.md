# Camp Report Intelligence Engine

A generic Camp Report Intelligence Engine. It accepts Camp Report JSON and
enriches existing report sections with:

* concern / observation
* explanation / insight
* recommendation
* confidence

The engine is data-driven. It works with arbitrary future Camp Reports that
follow the existing Camp Report schema — not only the sample fixture in this
folder.

## Architecture

```text
Camp Report JSON
      ↓
Analyzer
      ↓
Profile / Scoring
      ↓
Reasoning
      ↓
Insight Generator
      ↓
Formatter
      ↓
Camp Report Assembly
      ↓
Enriched Camp Report JSON
```

Production usage loads the single-file engine. The original multi-file sources
are kept under `intelligence_src/` as a readable reference.

## Production usage

```python
from camp_intelligence_engine import enrich_camp_report_with_intelligence

enriched_report = enrich_camp_report_with_intelligence(report_json)
```

The raw intelligence payload (profile, concerns, leadership cards) is also
available:

```python
from camp_intelligence_engine import generate_report_insights

payload = generate_report_insights(report_json)
```

Call these from the `camp_report_intelligence` directory, or add that directory
to `sys.path` before importing.

## CLI testing

From this folder:

```bash
python test_intelligence.py sample_camp_report.json
```

Optional flags:

```bash
python test_intelligence.py sample_camp_report.json -o ./output.json
python test_intelligence.py sample_camp_report.json --no-print-json
```

From the repository root:

```bash
python modules/reports/camp_report_intelligence/test_intelligence.py \
    modules/reports/camp_report_intelligence/sample_camp_report.json \
    --no-print-json
```

## Source structure

* `camp_intelligence_engine.py` — production single-file engine
* `test_intelligence.py` — end-to-end CLI test runner
* `sample_camp_report.json` — sample Camp Report input
* `intelligence_src/` — readable developer/reference source modules

## Important contract

* Input Camp Report `data` is not modified.
* Existing section names and keys remain unchanged.
* `section.data`, `section.name`, and `section.description` are preserved.
* Intelligence is attached onto existing Camp Report sections only.
* Unmapped sections (`meta`, `kpis`, `blood_and_lab_intelligence`,
  `company_average_scores`, `ranking`, …) are left unchanged.
* Profile / leadership / executive payloads are not added as new top-level
  Camp Report sections.
* Missing or optional sections continue to be skipped; the engine does not
  create them.
* The engine is generic and data-driven. It must work with future Camp Reports,
  not just `sample_camp_report.json`.
