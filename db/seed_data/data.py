from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class SeedDiagnosticTestGroup:
    group_name: str


@dataclass(frozen=True)
class SeedDiagnosticPackage:
    package_name: str


@dataclass(frozen=True)
class SeedDiagnosticTest:
    test_name: str
    parameter_key: str | None
    unit: str | None
    meaning: str | None
    lower_range_male: float | None
    higher_range_male: float | None
    lower_range_female: float | None
    higher_range_female: float | None
    causes_when_high: str | None
    causes_when_low: str | None
    effects_when_high: str | None
    effects_when_low: str | None
    what_to_do_when_low: str | None
    what_to_do_when_high: str | None


def _slug_parameter_key(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


DIAGNOSTIC_TEST_GROUPS: tuple[SeedDiagnosticTestGroup, ...] = (
    SeedDiagnosticTestGroup("Liver profile"),
    SeedDiagnosticTestGroup("Kidney function test"),
    SeedDiagnosticTestGroup("Kidney function test advance"),
    SeedDiagnosticTestGroup("Kidney function test with K"),
    SeedDiagnosticTestGroup("Thyroid profile"),
    SeedDiagnosticTestGroup("Iron Studies"),
    SeedDiagnosticTestGroup("Iron studies with Ferritin"),
    SeedDiagnosticTestGroup("Lipid profile"),
    SeedDiagnosticTestGroup("Complete Hemogram"),
    SeedDiagnosticTestGroup("Vitamin Profile"),
    SeedDiagnosticTestGroup("Diabetes profile"),
    SeedDiagnosticTestGroup("HBA1C"),
    SeedDiagnosticTestGroup("Hormones"),
    SeedDiagnosticTestGroup("CA-19.9 (Pancreatic cancer marker )"),
    SeedDiagnosticTestGroup("CA 72.4 (STOMACH CANCER MARKER)"),
    SeedDiagnosticTestGroup("CA 125 (OVARIAN CANCER MARKER )"),
    SeedDiagnosticTestGroup("CA 15.3 ( BREAST CANCER MARKER )"),
    SeedDiagnosticTestGroup("CEA (Carcino embryonic antigen )"),
    SeedDiagnosticTestGroup("PSA -Free"),
    SeedDiagnosticTestGroup("AFP"),
    SeedDiagnosticTestGroup("Insulin fasting"),
    SeedDiagnosticTestGroup("HOMA Index with Beta cells"),
    SeedDiagnosticTestGroup("Cortisol"),
    SeedDiagnosticTestGroup("Cortisol ,Serum"),
)


DIAGNOSTIC_TEST_PACKAGES: tuple[SeedDiagnosticPackage, ...] = (
    SeedDiagnosticPackage("Full body with vitamins"),
    SeedDiagnosticPackage("Full body without vitamins"),
    SeedDiagnosticPackage("Men peak performance"),
    SeedDiagnosticPackage("Women peak performance"),
)


# Diagnostic tests you provided (hemogram sample).
DIAGNOSTIC_TESTS: tuple[SeedDiagnosticTest, ...] = (
    SeedDiagnosticTest(
        test_name="Haemoglobin (Hb)",
        parameter_key="haemoglobin",
        unit="g/dL",
        meaning="Carries oxygen in blood",
        lower_range_male=13.0,
        higher_range_male=17.0,
        lower_range_female=12.0,
        higher_range_female=15.0,
        causes_when_high="dehydration, lung disease",
        causes_when_low="iron deficiency, blood loss",
        effects_when_high="headache",
        effects_when_low="tiredness",
        what_to_do_when_low="Check iron, B12, folate; improve diet",
        what_to_do_when_high="Hydrate, check lungs, smoking",
    ),
    SeedDiagnosticTest(
        test_name="Total RBC",
        parameter_key="total_rbc",
        unit="million/uL",
        meaning="Number of red blood cells",
        lower_range_male=4.5,
        higher_range_male=5.5,
        lower_range_female=3.8,
        higher_range_female=4.8,
        causes_when_high="dehydration",
        causes_when_low="anemia, bleeding",
        effects_when_high=None,
        effects_when_low="weakness",
        what_to_do_when_low="Check iron, B12",
        what_to_do_when_high="Hydrate, check cause",
    ),
    SeedDiagnosticTest(
        test_name="Haematocrit (HCT)",
        parameter_key="haematocrit_hct",
        unit="%",
        meaning="% of RBC in blood",
        lower_range_male=40.0,
        higher_range_male=50.0,
        lower_range_female=36.0,
        higher_range_female=46.0,
        causes_when_high="dehydration",
        causes_when_low="anemia",
        effects_when_high=None,
        effects_when_low="less oxygen",
        what_to_do_when_low="Improve nutrition",
        what_to_do_when_high="Drink fluids",
    ),
    SeedDiagnosticTest(
        test_name="MCV",
        parameter_key="mcv",
        unit="fL",
        meaning="Size of RBC",
        lower_range_male=83.0,
        higher_range_male=101.0,
        lower_range_female=83.0,
        higher_range_female=101.0,
        causes_when_high="B12 deficiency",
        causes_when_low="iron deficiency",
        effects_when_high="Helps identify anemia type",
        effects_when_low="Helps identify anemia type",
        what_to_do_when_low="Check iron",
        what_to_do_when_high="Check B12",
    ),
    SeedDiagnosticTest(
        test_name="MCH",
        parameter_key="mch",
        unit="pg",
        meaning="Hb per RBC",
        lower_range_male=27.0,
        higher_range_male=32.0,
        lower_range_female=27.0,
        higher_range_female=32.0,
        causes_when_high="B12 issue",
        causes_when_low="iron deficiency",
        effects_when_high=None,
        effects_when_low="iron issue",
        what_to_do_when_low="Confirm iron",
        what_to_do_when_high="Check B12",
    ),
    SeedDiagnosticTest(
        test_name="MCHC",
        parameter_key="mchc",
        unit="g/dL",
        meaning="Hb concentration",
        lower_range_male=31.5,
        higher_range_male=34.5,
        lower_range_female=31.5,
        higher_range_female=34.5,
        causes_when_high="rare",
        causes_when_low="iron deficiency",
        effects_when_high=None,
        effects_when_low="pale cells",
        what_to_do_when_low="Check iron",
        what_to_do_when_high="Check disorder",
    ),
    SeedDiagnosticTest(
        test_name="RDW",
        parameter_key="rdw",
        unit="%",
        meaning="RBC size variation",
        lower_range_male=11.6,
        higher_range_male=14.0,
        lower_range_female=11.6,
        higher_range_female=14.0,
        causes_when_high="mixed deficiency",
        causes_when_low="normal",
        effects_when_high="nutrition issue",
        effects_when_low=None,
        what_to_do_when_low="Usually none",
        what_to_do_when_high="Check iron, B12",
    ),
    SeedDiagnosticTest(
        test_name="Platelet Count",
        parameter_key="platelet_count",
        unit="thousand/uL",
        meaning="Clotting cells",
        lower_range_male=150.0,
        higher_range_male=410.0,
        lower_range_female=150.0,
        higher_range_female=410.0,
        causes_when_high="inflammation",
        causes_when_low="infection",
        effects_when_high=None,
        effects_when_low="bleeding risk",
        what_to_do_when_low="Avoid injury",
        what_to_do_when_high="Check cause",
    ),
    SeedDiagnosticTest(
        test_name="WBC Count",
        parameter_key="wbc_count",
        unit="thousand/uL",
        meaning="Infection cells",
        lower_range_male=4.0,
        higher_range_male=10.0,
        lower_range_female=4.0,
        higher_range_female=10.0,
        causes_when_high="infection",
        causes_when_low="weak immunity",
        effects_when_high="Infection risk",
        effects_when_low="Infection risk",
        what_to_do_when_low="Check doctor",
        what_to_do_when_high="Find infection",
    ),
    SeedDiagnosticTest(
        test_name="Neutrophils",
        parameter_key="neutrophils",
        unit="%",
        meaning="Fight bacteria",
        lower_range_male=40.0,
        higher_range_male=80.0,
        lower_range_female=40.0,
        higher_range_female=80.0,
        causes_when_high="infection",
        causes_when_low="severe illness",
        effects_when_high="Infection risk",
        effects_when_low="Infection risk",
        what_to_do_when_low="Avoid exposure",
        what_to_do_when_high="Check infection",
    ),
    SeedDiagnosticTest(
        test_name="Lymphocytes",
        parameter_key="lymphocytes",
        unit="%",
        meaning="Fight viruses",
        lower_range_male=20.0,
        higher_range_male=40.0,
        lower_range_female=20.0,
        higher_range_female=40.0,
        causes_when_high="viral",
        causes_when_low="weak immunity",
        effects_when_high="Viral response",
        effects_when_low="Viral response",
        what_to_do_when_low="Improve immunity",
        what_to_do_when_high="Monitor",
    ),
    SeedDiagnosticTest(
        test_name="Monocytes",
        parameter_key="monocytes",
        unit="%",
        meaning="Clean-up cells",
        lower_range_male=2.0,
        higher_range_male=10.0,
        lower_range_female=2.0,
        higher_range_female=10.0,
        causes_when_high="chronic infection",
        causes_when_low="Recovery phase",
        effects_when_high=None,
        effects_when_low="Recovery phase",
        what_to_do_when_low="Usually none",
        what_to_do_when_high="Check infection",
    ),
    SeedDiagnosticTest(
        test_name="Eosinophils",
        parameter_key="eosinophils",
        unit="%",
        meaning="Allergy cells",
        lower_range_male=1.0,
        higher_range_male=6.0,
        lower_range_female=1.0,
        higher_range_female=6.0,
        causes_when_high="allergy",
        causes_when_low=None,
        effects_when_high="Allergy symptoms",
        effects_when_low=None,
        what_to_do_when_low=None,
        what_to_do_when_high="Treat allergy",
    ),
    SeedDiagnosticTest(
        test_name="Basophils",
        parameter_key="basophils",
        unit="%",
        meaning="Allergy response",
        lower_range_male=0.0,
        higher_range_male=2.0,
        lower_range_female=0.0,
        higher_range_female=2.0,
        causes_when_high="inflammation",
        causes_when_low="mild",
        effects_when_high=None,
        effects_when_low="Mild",
        what_to_do_when_low=None,
        what_to_do_when_high="Check cause",
    ),
)

