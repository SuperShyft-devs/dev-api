"""Questionnaire seed aligned with Metsights record API field keys and choice codes."""

from __future__ import annotations

from db.seed.seed_dataclasses import (
    SeedCategory,
    SeedCategoryQuestion,
    SeedOption,
    SeedPackageCategory,
    SeedQuestion,
)

METSIGHTS_CATEGORIES: tuple[SeedCategory, ...] = (
    SeedCategory(1, "physical_measurement", "Physical measurement", "active"),
    SeedCategory(2, "diet_lifestyle_parameter", "Diet & lifestyle", "active"),
    SeedCategory(3, "vital_parameter", "Vitals", "active"),
    SeedCategory(4, "fitness_parameter", "Fitness", "active"),
)

METSIGHTS_QUESTIONS: tuple[SeedQuestion, ...] = (
    SeedQuestion(1, "weight", "Weight", "scale", True, False, None, "active"),
    SeedQuestion(2, "height", "Height", "scale", True, False, None, "active"),
    SeedQuestion(3, "waist_circumference", "Waist circumference", "scale", True, False, None, "active"),
    SeedQuestion(4, "bmi", "BMI", "scale", False, True, None, "active"),
    SeedQuestion(5, "systolic_blood_pressure", "Systolic blood pressure", "scale", True, False, None, "active"),
    SeedQuestion(6, "diastolic_blood_pressure", "Diastolic blood pressure", "scale", True, False, None, "active"),
    SeedQuestion(7, "heart_rate", "Resting heart rate", "scale", False, False, None, "active"),
    SeedQuestion(8, "respiratory_rate", "Respiratory rate", "scale", False, False, None, "active"),
    SeedQuestion(9, "hrv_sdnn", "HRV SDNN", "scale", False, False, None, "active"),
    SeedQuestion(10, "living_region", "Living region", "single_choice", True, False, None, "active"),
    SeedQuestion(11, "diet_preference", "Diet preference", "single_choice", True, False, None, "active"),
    SeedQuestion(12, "food_groups", "Food groups consumed daily", "multi_choice", True, False, None, "active"),
    SeedQuestion(13, "healthy_breakfast_frequency", "Healthy breakfast frequency", "single_choice", True, False, None, "active"),
    SeedQuestion(14, "fresh_fruit_frequency", "Fresh fruit frequency", "single_choice", True, False, None, "active"),
    SeedQuestion(15, "fresh_vegetable_frequency", "Fresh vegetable frequency", "single_choice", True, False, None, "active"),
    SeedQuestion(16, "baked_goods_frequency", "Baked goods frequency", "single_choice", True, False, None, "active"),
    SeedQuestion(17, "red_meat_frequency", "Red meat frequency", "single_choice", True, False, None, "active"),
    SeedQuestion(18, "butter_dish_frequency", "Butter dish frequency", "single_choice", True, False, None, "active"),
    SeedQuestion(19, "dessert_frequency", "Dessert frequency", "single_choice", True, False, None, "active"),
    SeedQuestion(20, "caffeine_frequency", "Caffeine frequency", "single_choice", True, False, None, "active"),
    SeedQuestion(21, "caffeine_type", "Caffeine type", "single_choice", False, False, None, "active"),
    SeedQuestion(22, "iodized_salt_status", "Iodized salt", "single_choice", True, False, None, "active"),
    SeedQuestion(23, "extra_salt_frequency", "Extra salt frequency", "single_choice", True, False, None, "active"),
    SeedQuestion(24, "sitting_hours", "Daily sitting hours", "single_choice", True, False, None, "active"),
    SeedQuestion(25, "physical_activity_frequency", "Physical activity frequency", "single_choice", True, False, None, "active"),
    SeedQuestion(26, "sleeping_hours", "Sleep duration", "single_choice", True, False, None, "active"),
    SeedQuestion(27, "alcohol_frequency", "Alcohol frequency", "single_choice", True, False, None, "active"),
    SeedQuestion(28, "tobacco_frequency", "Tobacco frequency", "single_choice", True, False, None, "active"),
    SeedQuestion(29, "family_health_history", "Family health history", "multi_choice", True, False, None, "active"),
    SeedQuestion(30, "family_health_history_other", "Family health history (other)", "text", False, False, None, "active"),
    SeedQuestion(31, "diagnosed_diseases", "Diagnosed diseases", "multi_choice", True, False, None, "active"),
    SeedQuestion(32, "diagnosed_diseases_other", "Diagnosed diseases (other)", "text", False, False, None, "active"),
    SeedQuestion(33, "diagnosed_diseases_medications", "Medications", "multi_choice", False, False, None, "active"),
    SeedQuestion(34, "diagnosed_diseases_medications_other", "Medications (other)", "text", False, False, None, "active"),
    SeedQuestion(35, "hip_circumference", "Hip circumference", "scale", False, False, None, "active"),
    SeedQuestion(36, "body_fat", "Body fat", "scale", False, False, None, "active"),
    SeedQuestion(37, "exercise_frequency_week", "Exercise frequency per week", "single_choice", False, False, None, "active"),
    SeedQuestion(38, "exercise_level", "Exercise level", "single_choice", False, False, None, "active"),
    SeedQuestion(39, "water_intake_frequency", "Water intake frequency", "single_choice", False, False, None, "active"),
    SeedQuestion(40, "sickness_frequency", "Sickness frequency", "single_choice", False, False, None, "active"),
    SeedQuestion(41, "health_priorities", "Health priorities", "multi_choice", False, False, None, "active"),
    SeedQuestion(42, "goal_preference", "Goal preference", "single_choice", False, False, None, "active"),
    SeedQuestion(43, "weight_loss_goal", "Weight loss goal", "scale", False, False, None, "active"),
    SeedQuestion(44, "daily_active_duration", "Daily active duration", "scale", False, False, None, "active"),
)


def _build_category_questions() -> tuple[SeedCategoryQuestion, ...]:
    links: list[SeedCategoryQuestion] = []
    # Avoid PK collisions with legacy seed rows when pairs are re-mapped.
    lid = 10001
    for q in (1, 2, 3, 4, 35, 36):
        links.append(SeedCategoryQuestion(lid, 1, q))
        lid += 1
    for q in range(10, 35):
        links.append(SeedCategoryQuestion(lid, 2, q))
        lid += 1
    for q in (5, 6, 7, 8, 9):
        links.append(SeedCategoryQuestion(lid, 3, q))
        lid += 1
    fitness_q = sorted(
        {
            1,
            2,
            3,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            24,
            25,
            26,
            27,
            28,
            29,
            31,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
            43,
            44,
        }
    )
    for q in fitness_q:
        links.append(SeedCategoryQuestion(lid, 4, q))
        lid += 1
    return tuple(links)


METSIGHTS_CATEGORY_QUESTIONS: tuple[SeedCategoryQuestion, ...] = _build_category_questions()


def _build_options() -> tuple[SeedOption, ...]:
    out: list[SeedOption] = []
    oid = 1

    def freq_block(qid: int, n: int = 16) -> None:
        nonlocal oid
        for i in range(n):
            out.append(
                SeedOption(
                    oid,
                    qid,
                    str(i),
                    f"Code {i}",
                    None,
                )
            )
            oid += 1

    def multi_block(qid: int, n: int = 16) -> None:
        freq_block(qid, n)

    multi_block(12)
    freq_block(10, 8)
    freq_block(11, 8)
    for qid in range(13, 29):
        freq_block(qid)
    multi_block(29)
    multi_block(31)
    multi_block(33)
    for qid in (37, 38, 39, 40, 42):
        freq_block(qid)
    multi_block(41)

    scale_pairs = [
        (1, "kg"),
        (1, "lb"),
        (2, "cm"),
        (2, "in"),
        (3, "cm"),
        (3, "in"),
        (35, "cm"),
        (35, "in"),
        (36, "%"),
        (4, "kg/m²"),
        (5, "mmhg"),
        (6, "mmhg"),
        (7, "bpm"),
        (8, "breaths/min"),
        (9, "ms"),
        (43, "kg"),
        (43, "lb"),
        (44, "min"),
        (44, "h"),
    ]
    for qid, u in scale_pairs:
        out.append(SeedOption(oid, qid, u, u, None))
        oid += 1

    return tuple(out)


METSIGHTS_OPTIONS: tuple[SeedOption, ...] = _build_options()

METSIGHTS_PACKAGE_CATEGORIES: tuple[SeedPackageCategory, ...] = (
    SeedPackageCategory(10001, 2, 1),
    SeedPackageCategory(10002, 2, 2),
    SeedPackageCategory(10003, 2, 3),
    SeedPackageCategory(10004, 1, 1),
    SeedPackageCategory(10005, 1, 2),
    SeedPackageCategory(10006, 1, 3),
    SeedPackageCategory(10007, 3, 4),
)
