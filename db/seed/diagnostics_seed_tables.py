"""Fixed-ID diagnostics seed rows (packages, groups, junctions, reasons, tags, samples)."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from db.seed.diagnostics_operations import (
    SeedDiagGroup,
    SeedDiagGroupTest,
    SeedDiagPackage,
    SeedDiagPackageTestGroup,
    SeedDiagReason,
    SeedDiagSample,
    SeedDiagTag,
)

_UTC = timezone.utc

DIAG_PACKAGES: tuple[SeedDiagPackage, ...] = (
    SeedDiagPackage(
        diagnostic_package_id=10,
        reference_id=None,
        package_name="Haemogram",
        diagnostic_provider=None,
        no_of_tests=None,
        status="inactive",
        created_at=datetime(2026, 3, 25, 18, 31, 31, tzinfo=_UTC),
        report_duration_hours=None,
        collection_type=None,
        about_text=None,
        bookings_count=0,
        price=None,
        original_price=None,
        is_most_popular=False,
        gender_suitability="both",
    ),
    SeedDiagPackage(
        diagnostic_package_id=1,
        reference_id=None,
        package_name="Full Body Checkup",
        diagnostic_provider="Healthians",
        no_of_tests=120,
        status="inactive",
        created_at=datetime(2026, 3, 17, 7, 3, 1, tzinfo=_UTC),
        report_duration_hours=48,
        collection_type="home_collection",
        about_text="About this package",
        bookings_count=2000,
        price=Decimal("2000"),
        original_price=Decimal("5000"),
        is_most_popular=True,
        gender_suitability="both",
    ),
    SeedDiagPackage(
        diagnostic_package_id=6,
        reference_id=None,
        package_name="Supershyft Basic",
        diagnostic_provider=None,
        no_of_tests=None,
        status="active",
        created_at=datetime(2026, 3, 25, 17, 50, 44, tzinfo=_UTC),
        report_duration_hours=None,
        collection_type="home_collection",
        about_text=(
            "This package is designed to give you a comprehensive snapshot of your health through key blood markers. "
            "It helps identify early signs of imbalances, deficiencies, or underlying conditions often before symptoms appear.\n\n"
            "By analyzing essential biomarkers, this test supports proactive health management, allowing you to make informed "
            "decisions about your lifestyle, nutrition, and overall well-being."
        ),
        bookings_count=0,
        price=None,
        original_price=None,
        is_most_popular=False,
        gender_suitability="both",
    ),
    SeedDiagPackage(
        diagnostic_package_id=7,
        reference_id=None,
        package_name="Supershyft Core",
        diagnostic_provider=None,
        no_of_tests=None,
        status="active",
        created_at=datetime(2026, 3, 25, 17, 50, 44, tzinfo=_UTC),
        report_duration_hours=None,
        collection_type="home_collection",
        about_text=(
            "This package offers a comprehensive view of your health by combining essential blood tests with key vitamin markers. "
            "It helps uncover hidden deficiencies, metabolic imbalances, and early signs of lifestyle-related conditions often "
            "before noticeable symptoms arise.\n\n"
            "With insights into organ function, blood health, sugar levels, and vital nutrients, this test empowers you to take "
            "a proactive approach to your well-being, energy levels, and long-term health."
        ),
        bookings_count=0,
        price=None,
        original_price=None,
        is_most_popular=False,
        gender_suitability="both",
    ),
    SeedDiagPackage(
        diagnostic_package_id=8,
        reference_id=None,
        package_name="Supershyft Elite Men",
        diagnostic_provider=None,
        no_of_tests=None,
        status="active",
        created_at=datetime(2026, 3, 25, 17, 50, 44, tzinfo=_UTC),
        report_duration_hours=None,
        collection_type="home_collection",
        about_text=(
            "This is a Men Peak Package is designed to give a complete picture of men's health, combining essential blood tests "
            "with key vitamins like Vitamin D and B12, along with ESR to detect inflammation.\n\n"
            "It helps uncover early signs of fatigue, hormonal imbalance, metabolic issues, and underlying infections—supporting "
            "stronger energy, endurance, and overall vitality."
        ),
        bookings_count=0,
        price=None,
        original_price=None,
        is_most_popular=False,
        gender_suitability="male",
    ),
    SeedDiagPackage(
        diagnostic_package_id=9,
        reference_id=None,
        package_name="Supershyft Elite Women",
        diagnostic_provider=None,
        no_of_tests=None,
        status="active",
        created_at=datetime(2026, 3, 25, 17, 50, 44, tzinfo=_UTC),
        report_duration_hours=None,
        collection_type="home_collection",
        about_text=(
            "This is a Women Peak Package is crafted to support women's overall wellness with a comprehensive set of blood tests, "
            "including Iron Profile and Homocysteine, along with essential vitamins like D3 and B12.\n\n"
            "It focuses on nutritional balance, heart health, and metabolic function—helping detect deficiencies and risks that "
            "commonly affect women's energy, immunity, and long-term health."
        ),
        bookings_count=0,
        price=None,
        original_price=None,
        is_most_popular=False,
        gender_suitability="female",
    ),
    SeedDiagPackage(
        diagnostic_package_id=11,
        reference_id=None,
        package_name="Supershyft Peak Men",
        diagnostic_provider=None,
        no_of_tests=None,
        status="active",
        created_at=datetime(2026, 4, 2, 12, 59, 16, tzinfo=_UTC),
        report_duration_hours=None,
        collection_type="home_collection",
        about_text=(
            "An advanced full-body diagnostic suite crafted for those who prioritize precision, prevention, and peak health. "
            "This package integrates critical biomarkers across endocrine, metabolic, and systemic functions to uncover subtle "
            "imbalances before they become concerns. Designed with a premium preventive approach, it empowers you with clarity, "
            "control, and confidence in your health decisions. A perfect choice for individuals committed to optimizing longevity "
            "and overall well-being."
        ),
        bookings_count=0,
        price=None,
        original_price=None,
        is_most_popular=False,
        gender_suitability="male",
    ),
    SeedDiagPackage(
        diagnostic_package_id=12,
        reference_id=None,
        package_name="Supershyft Peak Women",
        diagnostic_provider=None,
        no_of_tests=None,
        status="active",
        created_at=datetime(2026, 4, 2, 13, 1, 33, tzinfo=_UTC),
        report_duration_hours=None,
        collection_type="home_collection",
        about_text=(
            "A meticulously curated comprehensive health panel designed to deliver deep insights into your metabolic, hormonal, "
            "and cardiovascular health. This package brings together advanced biomarkers and essential diagnostics to provide a "
            "360° view of your body's internal balance. Ideal for individuals seeking proactive health management and early risk "
            "detection, it blends precision with preventive care. Every parameter has been thoughtfully included to support long-term "
            "wellness, performance, and vitality."
        ),
        bookings_count=0,
        price=None,
        original_price=None,
        is_most_popular=False,
        gender_suitability="female",
    ),
    SeedDiagPackage(
        diagnostic_package_id=13,
        reference_id=None,
        package_name="Female Cancer Screening",
        diagnostic_provider=None,
        no_of_tests=None,
        status="active",
        created_at=datetime(2026, 4, 2, 13, 4, 27, tzinfo=_UTC),
        report_duration_hours=None,
        collection_type="home_collection",
        about_text=(
            "This package is designed to support early detection of cancer risks specific to women through targeted blood markers. "
            "It focuses on identifying subtle changes in the body that may indicate early warning signs, often before symptoms appear.\n\n"
            "With a preventive and proactive approach, this package helps monitor key health indicators while supporting overall "
            "wellness and long-term health."
        ),
        bookings_count=0,
        price=None,
        original_price=None,
        is_most_popular=False,
        gender_suitability="female",
    ),
    SeedDiagPackage(
        diagnostic_package_id=14,
        reference_id=None,
        package_name="Male Cancer Screening",
        diagnostic_provider=None,
        no_of_tests=None,
        status="active",
        created_at=datetime(2026, 4, 2, 13, 6, 13, tzinfo=_UTC),
        report_duration_hours=None,
        collection_type="home_collection",
        about_text=(
            "This package is tailored to detect early indicators of cancer risks commonly associated with men. It uses specific "
            "blood markers to identify abnormalities at an early stage, supporting timely diagnosis and intervention.\n\n"
            "This package is designed to help men take a proactive approach to their health and stay ahead of potential risks."
        ),
        bookings_count=0,
        price=None,
        original_price=None,
        is_most_popular=False,
        gender_suitability="male",
    ),
    SeedDiagPackage(
        diagnostic_package_id=15,
        reference_id=None,
        package_name="Allergy Package by ELISA Method",
        diagnostic_provider=None,
        no_of_tests=None,
        status="active",
        created_at=datetime(2026, 4, 2, 13, 18, 35, tzinfo=_UTC),
        report_duration_hours=None,
        collection_type="home_collection",
        about_text=(
            "The Allergy Comprehensive Package is designed to identify a wide range of allergic triggers using the ELISA method, "
            "covering environmental, food, and lifestyle-related allergens.\n\n"
            "By analyzing total IgE levels along with multiple allergen profiles, this package helps detect sensitivities that may "
            "be affecting your daily life often without clear or immediate symptoms. It provides a detailed understanding of your "
            "body's immune response, enabling better management of allergies and overall well-being."
        ),
        bookings_count=0,
        price=None,
        original_price=None,
        is_most_popular=False,
        gender_suitability="both",
    ),
)

_R6 = (
    "Detecting early warning signs of common health issues\n"
    "Monitoring vital functions like metabolism, organ health, and nutrient levels\n"
    "Supporting preventive care instead of reactive treatment\n"
    "Providing actionable insights to improve your daily habits"
)
_R7 = (
    "Detecting early risks of diabetes, cholesterol imbalance, and thyroid disorders\n"
    "Identifying Vitamin D3 and B12 deficiencies that can impact energy, mood, and immunity\n"
    "Monitoring vital organ health including liver and kidneys\n"
    "Supporting better lifestyle decisions with clear, actionable insights\n"
    "Helping prevent fatigue, weakness, and long-term health issues\n"
    "Enabling regular health tracking for ongoing wellness"
)
_R9 = (
    "Supports hormonal balance by identifying underlying nutritional deficiencies\n"
    "Evaluates iron levels to address common issues like fatigue and weakness\n"
    "Detects Vitamin D3 & B12 deficiencies that impact mood, energy, and bone health\n"
    "Measures Homocysteine for heart and metabolic health insights\n"
    "Monitors thyroid function, a key regulator of hormones and metabolism\n"
    "Tracks blood sugar levels (FBS, HbA1c) which influence hormonal stability\n"
    "Enables early detection of imbalances affecting menstrual and overall health"
)
_R11 = (
    "Comprehensive diabetes and insulin resistance screening\n"
    "In-depth cardiovascular and inflammatory risk assessment\n"
    "Extensive hormone panel for complete endocrine evaluation\n"
    "Covers essential micronutrients impacting energy and immunity\n"
    "Monitors vital organ health including liver and kidneys\n"
    "Detects early inflammation and cellular stress markers\n"
    "Supports personalized health and lifestyle optimization\n"
    "Suitable for high-performance individuals and preventive care seekers"
)
_R12 = (
    "Provides a complete evaluation of blood sugar control and metabolic health\n"
    "Assesses cardiovascular risk through advanced markers like homocysteine and HS-CRP\n"
    "Offers detailed hormonal profiling for performance, energy, and reproductive health\n"
    "Includes key vitamin and mineral levels essential for immunity and recovery\n"
    "Screens liver and kidney function for early signs of organ stress\n"
    "Evaluates inflammation and tissue health for underlying conditions\n"
    "Supports preventive healthcare with actionable insights\n"
    "Ideal for routine executive checkups or performance optimization"
)
_R13 = (
    "Screening markers linked to women-specific cancers\n"
    "Enabling early detection for timely medical attention\n"
    "Supporting preventive healthcare and routine check-ups\n"
    "Helping identify hidden risks even without symptoms\n"
    "Providing clarity and peace of mind about your health status"
)
_R14 = (
    "Screening markers linked to men-specific cancers\n"
    "Detecting early warning signs before symptoms develop\n"
    "Supporting proactive and preventive health management\n"
    "Helping identify potential risks early for timely follow-up\n"
    "Encouraging regular health monitoring for long-term well-being"
)
_R15 = (
    "Identifying a wide range of allergens including food, dust, pets, and environmental triggers\n"
    "Measuring total IgE levels to assess allergic response\n"
    "Helping pinpoint root causes of symptoms like sneezing, rashes, or breathing issues\n"
    "Supporting personalized lifestyle and dietary adjustments\n"
    "Enabling better allergy management and prevention\n"
    "Providing comprehensive insights across 15 detailed allergen profiles"
)

DIAG_REASONS: tuple[SeedDiagReason, ...] = (
    SeedDiagReason(2, 1, 2, "this is perfect"),
    SeedDiagReason(1, 1, 1, "Bcoz this is good"),
    SeedDiagReason(3, 6, 1, _R6),
    SeedDiagReason(4, 7, 1, _R7),
    SeedDiagReason(5, 9, 1, _R9),
    SeedDiagReason(6, 11, 1, _R11),
    SeedDiagReason(7, 12, 1, _R12),
    SeedDiagReason(8, 13, 1, _R13),
    SeedDiagReason(9, 14, 1, _R14),
    SeedDiagReason(10, 15, 1, _R15),
)

DIAG_TAGS: tuple[SeedDiagTag, ...] = (
    SeedDiagTag(1, 1, "male", None),
    SeedDiagTag(2, 1, "fullbody", None),
)

DIAG_GROUPS: tuple[SeedDiagGroup, ...] = (
    SeedDiagGroup(30, "Iron Studies", None),
    SeedDiagGroup(33, "Complete Hemogram", None),
    SeedDiagGroup(34, "Vitamin Profile", None),
    SeedDiagGroup(36, "HBA1C", None),
    SeedDiagGroup(44, "AFP", None),
    SeedDiagGroup(45, "Insulin fasting", None),
    SeedDiagGroup(46, "HOMA Index with Beta cells", None),
    SeedDiagGroup(48, "Cortisol ,Serum", None),
    SeedDiagGroup(26, "Kidney Function", None),
    SeedDiagGroup(25, "Liver Profile", None),
    SeedDiagGroup(27, "Kidney Function - Advance", None),
    SeedDiagGroup(28, "Kidney Function with K", None),
    SeedDiagGroup(29, "Thyroid Profile", None),
    SeedDiagGroup(31, "Iron Studies with Ferritin", None),
    SeedDiagGroup(32, "Lipid Profile", None),
    SeedDiagGroup(35, "Diabetes Profile", None),
    SeedDiagGroup(47, "Cortisol (Evening)", None),
    SeedDiagGroup(50, "Urine Routine and Microscopy", None),
    SeedDiagGroup(51, "Allergy- Drugs", None),
    SeedDiagGroup(52, "Allergy- Pets", None),
    SeedDiagGroup(53, "Allergy- Dust Mites", None),
    SeedDiagGroup(54, "Allergy- Insects", None),
    SeedDiagGroup(55, "Allergy- Grass Mix", None),
    SeedDiagGroup(56, "Allergy- Moulds And Yeast", None),
    SeedDiagGroup(57, "Allergy- Weed And Tree", None),
    SeedDiagGroup(58, "Allergy- Non Veg Food", None),
    SeedDiagGroup(59, "Allergy- Veg Food And Fruits", None),
    SeedDiagGroup(60, "Allergy- Occupational", None),
    SeedDiagGroup(61, "Allergy- Parasites", None),
    SeedDiagGroup(62, "Allergy- Seeds, Nuts, Legumes", None),
    SeedDiagGroup(63, "Allergy- Dairy Products", None),
    SeedDiagGroup(64, "Allergy- Spices", None),
    SeedDiagGroup(65, "IGE Total", None),
    SeedDiagGroup(66, "Inflammatory Markers", None),
    SeedDiagGroup(67, "Sleep Markers", None),
    SeedDiagGroup(68, "Hormones- Advance", None),
    SeedDiagGroup(69, "Muscle & Tissue Health", None),
    SeedDiagGroup(70, "Hormones (Male)", None),
    SeedDiagGroup(37, "Hormones (Female)", None),
    SeedDiagGroup(43, "Prostate Cancer", None),
    SeedDiagGroup(42, "Colorectal Cancer", None),
    SeedDiagGroup(41, "Breast Cancer", None),
    SeedDiagGroup(40, "Ovarian Cancer", None),
    SeedDiagGroup(39, "Stomach Cancer", None),
    SeedDiagGroup(38, "Pancreatic Cancer", None),
)

# (id, group_id, test_id, display_order) — from your diagnostic_test_group_tests export
DIAG_GROUP_TESTS: tuple[SeedDiagGroupTest, ...] = tuple(
    SeedDiagGroupTest(r[0], r[1], r[2], r[3])
    for r in (
        (102, 37, 64, 1),
        (42, 26, 29, 1),
        (43, 26, 39, 2),
        (44, 26, 40, 3),
        (7, 26, 26, 4),
        (46, 26, 41, 5),
        (47, 26, 42, 6),
        (48, 26, 28, 7),
        (49, 26, 43, 8),
        (139, 50, 87, 8),
        (140, 50, 88, 9),
        (141, 50, 89, 10),
        (142, 50, 90, 11),
        (50, 26, 44, 9),
        (51, 26, 27, 10),
        (52, 27, 29, 1),
        (53, 27, 39, 2),
        (54, 27, 40, 3),
        (55, 27, 26, 4),
        (56, 27, 41, 5),
        (57, 27, 42, 6),
        (58, 27, 28, 7),
        (59, 27, 43, 8),
        (60, 27, 44, 9),
        (61, 27, 27, 10),
        (62, 27, 45, 11),
        (1, 25, 23, 1),
        (14, 25, 36, 2),
        (2, 25, 19, 3),
        (3, 25, 18, 4),
        (41, 25, 21, 5),
        (29, 25, 37, 6),
        (26, 25, 15, 7),
        (63, 28, 29, 1),
        (64, 28, 39, 2),
        (65, 28, 40, 3),
        (66, 28, 26, 4),
        (67, 28, 41, 5),
        (68, 28, 42, 6),
        (27, 25, 16, 8),
        (4, 25, 20, 9),
        (5, 25, 24, 10),
        (6, 25, 25, 11),
        (28, 25, 38, 12),
        (69, 28, 28, 7),
        (70, 28, 43, 8),
        (71, 28, 44, 9),
        (72, 28, 27, 10),
        (73, 28, 45, 11),
        (74, 28, 46, 12),
        (75, 29, 47, 1),
        (76, 29, 48, 2),
        (77, 29, 49, 3),
        (78, 30, 50, 1),
        (79, 30, 51, 2),
        (80, 30, 52, 3),
        (81, 30, 53, 4),
        (82, 31, 50, 1),
        (83, 31, 51, 2),
        (84, 31, 52, 3),
        (85, 31, 53, 4),
        (86, 31, 54, 5),
        (87, 32, 30, 1),
        (88, 32, 32, 2),
        (89, 32, 33, 3),
        (90, 32, 34, 4),
        (91, 32, 35, 5),
        (92, 32, 31, 6),
        (93, 32, 55, 7),
        (94, 32, 56, 8),
        (95, 32, 57, 9),
        (96, 34, 58, 1),
        (97, 34, 59, 2),
        (176, 39, 114, 1),
        (100, 36, 62, 1),
        (101, 36, 63, 2),
        (103, 37, 65, 2),
        (104, 37, 66, 3),
        (105, 37, 67, 4),
        (106, 33, 68, 1),
        (107, 33, 69, 2),
        (108, 33, 70, 3),
        (109, 33, 71, 4),
        (110, 33, 72, 5),
        (111, 33, 73, 6),
        (112, 33, 1, 7),
        (113, 33, 5, 8),
        (114, 33, 6, 9),
        (115, 33, 4, 10),
        (116, 33, 74, 11),
        (117, 33, 75, 12),
        (118, 33, 8, 13),
        (119, 33, 9, 14),
        (120, 33, 7, 15),
        (121, 33, 10, 16),
        (122, 33, 13, 17),
        (123, 33, 11, 18),
        (124, 33, 12, 19),
        (125, 33, 14, 20),
        (126, 33, 76, 21),
        (127, 33, 77, 22),
        (128, 33, 2, 23),
        (129, 33, 78, 24),
        (130, 33, 79, 25),
        (143, 50, 91, 12),
        (132, 50, 81, 1),
        (133, 50, 82, 2),
        (134, 50, 83, 3),
        (135, 50, 98, 4),
        (136, 50, 84, 5),
        (137, 50, 85, 6),
        (138, 50, 86, 7),
        (139, 50, 87, 8),
        (140, 50, 88, 9),
        (141, 50, 89, 10),
        (142, 50, 90, 11),
        (144, 50, 92, 13),
        (145, 50, 93, 14),
        (146, 50, 94, 15),
        (147, 50, 95, 16),
        (148, 50, 96, 17),
        (149, 50, 97, 18),
        (150, 67, 100, 1),
        (151, 67, 101, 2),
        (152, 66, 102, 1),
        (154, 66, 103, 2),
        (155, 66, 104, 3),
        (164, 69, 107, 1),
        (165, 69, 108, 2),
        (166, 70, 105, 1),
        (167, 70, 67, 2),
        (98, 35, 60, 1),
        (177, 38, 113, 1),
        (170, 35, 62, 3),
        (171, 35, 63, 4),
        (172, 43, 118, 1),
        (173, 42, 117, 1),
        (174, 41, 116, 1),
        (175, 40, 115, 1),
        (178, 44, 119, 1),
        (159, 68, 64, 1),
        (160, 68, 65, 2),
        (161, 68, 66, 3),
        (162, 68, 105, 4),
        (163, 68, 106, 5),
        (184, 68, 67, 6),
    )
)

DIAG_PACKAGE_TEST_GROUPS: tuple[SeedDiagPackageTestGroup, ...] = tuple(
    SeedDiagPackageTestGroup(r[0], r[1], r[2], r[3])
    for r in (
        (56, 13, 40, 1),
        (57, 13, 41, 2),
        (58, 13, 38, 3),
        (59, 13, 42, 4),
        (1, 6, 25, 1),
        (5, 6, 32, 3),
        (6, 6, 33, 4),
        (7, 6, 30, 5),
        (8, 6, 29, 6),
        (9, 6, 35, 7),
        (2, 6, 26, 8),
        (10, 7, 25, 1),
        (11, 7, 28, 2),
        (12, 7, 29, 3),
        (13, 7, 30, 4),
        (14, 7, 32, 5),
        (15, 7, 33, 6),
        (16, 7, 34, 7),
        (17, 7, 35, 8),
        (18, 8, 25, 1),
        (19, 8, 28, 2),
        (20, 8, 29, 3),
        (21, 8, 30, 4),
        (22, 8, 33, 5),
        (23, 8, 32, 6),
        (24, 8, 34, 7),
        (25, 8, 35, 8),
        (26, 8, 50, 9),
        (27, 9, 25, 1),
        (28, 9, 28, 2),
        (29, 9, 29, 3),
        (30, 9, 30, 4),
        (31, 9, 32, 5),
        (32, 9, 33, 6),
        (33, 9, 34, 7),
        (34, 9, 35, 8),
        (36, 9, 37, 10),
        (37, 9, 50, 11),
        (38, 11, 25, 1),
        (39, 11, 28, 2),
        (40, 11, 29, 3),
        (41, 11, 30, 4),
        (42, 11, 32, 5),
        (43, 11, 33, 6),
        (44, 11, 34, 7),
        (45, 11, 35, 8),
        (46, 11, 50, 9),
        (47, 12, 25, 1),
        (48, 12, 28, 2),
        (49, 12, 29, 3),
        (50, 12, 32, 4),
        (51, 12, 33, 5),
        (52, 12, 31, 6),
        (53, 12, 34, 7),
        (54, 12, 35, 8),
        (55, 12, 50, 9),
        (60, 14, 42, 1),
        (61, 14, 44, 2),
        (62, 14, 38, 3),
        (66, 14, 43, 4),
        (67, 15, 51, 1),
        (68, 15, 52, 2),
        (69, 15, 53, 3),
        (70, 15, 54, 4),
        (71, 15, 55, 5),
        (72, 15, 56, 6),
        (73, 15, 57, 7),
        (74, 15, 59, 8),
        (75, 15, 58, 9),
        (76, 15, 60, 10),
        (77, 15, 61, 11),
        (78, 15, 62, 12),
        (79, 15, 63, 13),
        (80, 15, 64, 14),
        (95, 15, 65, 15),
    )
)

DIAG_SAMPLES: tuple[SeedDiagSample, ...] = (
    SeedDiagSample(1, 15, "Blood", None, 1),
)
