"""Questionnaire seed: SuperShyft copy and grouping with Metsights API field keys for sync."""

from __future__ import annotations

from db.seed.seed_dataclasses import (
    SeedCategory,
    SeedCategoryQuestion,
    SeedOption,
    SeedPackageCategory,
    SeedQuestion,
)

METSIGHTS_CATEGORIES: tuple[SeedCategory, ...] = (
    SeedCategory(1, "anthropometry", "Anthropometry", "active"),
    SeedCategory(2, "family_history", "Family History", "active"),
    SeedCategory(3, "lifestyle_habits", "Lifestyle & Habits", "active"),
    SeedCategory(4, "nutrition_log", "Nutrition Log", "active"),
    SeedCategory(5, "vitals", "Vitals", "active"),
)

METSIGHTS_QUESTIONS: tuple[SeedQuestion, ...] = (
    SeedQuestion(1, "height", "What is your height?", "scale", True, False, "Enter your height", "active"),
    SeedQuestion(2, "weight", "What is your body weight?", "scale", True, False, "Enter your weight", "active"),
    SeedQuestion(3, "waist_circumference", "What is your waist size?", "scale", True, False, "Measure around your waist", "active"),
    SeedQuestion(4, "hip_circumference", "What is your hip size?", "scale", True, False, "Measure around your hips", "active"),
    SeedQuestion(5, "body_fat", "What is your body fat percent?", "scale", False, False, "Approximate body fat percentage", "active"),
    SeedQuestion(6, "living_region", "Where have you lived most of your life?", "single_choice", True, False, "Select the environment you lived in most", "active"),
    SeedQuestion(7, "family_health_history", "Do any of your close blood relatives have the following health conditions?", "multiple_choice", True, False, "Select multiple or None", "active"),
    SeedQuestion(8, "diagnosed_diseases", "Are you diagnosed with the following diseases?", "multiple_choice", True, False, "Select multiple or None", "active"),
    SeedQuestion(9, "diagnosed_diseases_medications", "Are you taking medications for the following diseases?", "multiple_choice", False, False, "Select multiple or None", "active"),
    SeedQuestion(10, "family_health_history_other", "Family health history (other)", "text", False, False, None, "active"),
    SeedQuestion(11, "diagnosed_diseases_other", "Diagnosed diseases (other)", "text", False, False, None, "active"),
    SeedQuestion(12, "diagnosed_diseases_medications_other", "Medications (other)", "text", False, False, None, "active"),
    SeedQuestion(13, "sitting_hours", "How long do you sit continuously every day due to work or lifestyle?", "single_choice", True, False, None, "active"),
    SeedQuestion(14, "physical_activity_frequency", "How much time do you spend engaging in physical activity or exercise daily?", "single_choice", True, False, None, "active"),
    SeedQuestion(15, "exercise_frequency_week", "On a typical week, how much time do you dedicate to leisure activities, workouts or sports?", "single_choice", True, False, None, "active"),
    SeedQuestion(16, "exercise_level", "On an average week, how would you rate the intensity of your activities or workouts?", "single_choice", True, False, "Low, Moderate, High intensity based on effort", "active"),
    SeedQuestion(17, "local_walking_time", "How much time do you spend actively walking each day?", "single_choice", True, False, None, "active"),
    SeedQuestion(18, "sleeping_hours", "What is your average duration of good-quality sleep?", "single_choice", True, False, None, "active"),
    SeedQuestion(19, "alcohol_frequency", "What is your alcohol consumption?", "single_choice", False, False, None, "active"),
    SeedQuestion(20, "tobacco_frequency", "How often do you smoke cigarettes or tobacco?", "single_choice", False, False, None, "active"),
    SeedQuestion(21, "health_priorities", "What are your primary health and wellness priorities?", "multiple_choice", True, False, "Choose top 2 priorities", "active"),
    SeedQuestion(22, "goal_preference", "What aspect of your lifestyle changes would you like to prioritize?", "single_choice", True, False, None, "active"),
    SeedQuestion(23, "diet_preference", "What type of diet do you primarily consume?", "single_choice", True, False, None, "active"),
    SeedQuestion(24, "food_groups", "Which of the following food groups do you consume every day?", "multiple_choice", True, False, None, "active"),
    SeedQuestion(25, "healthy_breakfast_frequency", "How frequently do you have a healthy homemade breakfast in a week?", "single_choice", True, False, None, "active"),
    SeedQuestion(26, "fresh_fruit_frequency", "How frequently do you consume fresh fruits?", "single_choice", True, False, None, "active"),
    SeedQuestion(27, "baked_goods_frequency", "How frequently do you consume cookies, biscuits, bread, or cakes?", "single_choice", True, False, None, "active"),
    SeedQuestion(28, "fresh_vegetable_frequency", "How frequently do you consume fresh vegetables?", "single_choice", True, False, None, "active"),
    SeedQuestion(29, "dessert_frequency", "How frequently do you consume sugary drinks and desserts?", "single_choice", True, False, None, "active"),
    SeedQuestion(30, "iodized_salt_status", "Do you use iodized salt in your diet?", "single_choice", True, False, None, "active"),
    SeedQuestion(31, "extra_salt_frequency", "How often do you add extra salt to your food?", "single_choice", True, False, None, "active"),
    SeedQuestion(32, "caffeine_frequency", "What's your coffee or tea intake?", "single_choice", False, False, None, "active"),
    SeedQuestion(33, "caffeine_type", "What type of coffee or tea do you drink?", "multiple_choice", False, False, None, "active"),
    SeedQuestion(34, "butter_dish_frequency", "How frequently do you indulge in dishes rich in butter?", "single_choice", True, False, None, "active"),
    SeedQuestion(35, "red_meat_frequency", "How frequently do you consume red meat?", "single_choice", True, False, None, "active"),
    SeedQuestion(36, "water_intake_frequency", "How many glasses of water do you drink in a day?", "single_choice", True, False, None, "active"),
    SeedQuestion(37, "sickness_frequency", "How often do you fall sick in a year?", "single_choice", True, False, None, "active"),
    SeedQuestion(38, "systolic_blood_pressure", "Systolic Blood Pressure", "scale", True, False, "Top number of your blood pressure reading", "active"),
    SeedQuestion(39, "diastolic_blood_pressure", "Diastolic Blood Pressure", "scale", True, False, "Bottom number of your blood pressure reading", "active"),
)


def _build_category_questions() -> tuple[SeedCategoryQuestion, ...]:
    links: list[SeedCategoryQuestion] = []
    lid = 40001
    for q in range(1, 6):
        links.append(SeedCategoryQuestion(lid, 1, q))
        lid += 1
    for q in range(6, 13):
        links.append(SeedCategoryQuestion(lid, 2, q))
        lid += 1
    for q in range(13, 23):
        links.append(SeedCategoryQuestion(lid, 3, q))
        lid += 1
    for q in range(23, 38):
        links.append(SeedCategoryQuestion(lid, 4, q))
        lid += 1
    for q in range(38, 40):
        links.append(SeedCategoryQuestion(lid, 5, q))
        lid += 1
    return tuple(links)


METSIGHTS_CATEGORY_QUESTIONS: tuple[SeedCategoryQuestion, ...] = _build_category_questions()

# (question_id, [(option_value, display_name), ...])
_OPT_SPEC: list[tuple[int, list[tuple[str, str]]]] = [
    (1, [("cm", "Centimeters"), ("ft_in", "Feet/Inches")]),
    (2, [("kg", "Kilograms"), ("lb", "Pounds")]),
    (3, [("in", "Inches"), ("cm", "Centimeters")]),
    (4, [("in", "Inches"), ("cm", "Centimeters")]),
    (5, [("%", "Percentage")]),
    (6, [("inland", "Inland"), ("coastal", "Coastal")]),
    (
        7,
        [
            ("type_2_diabetes", "Type 2 Diabetes"),
            ("hypertension", "Hypertension"),
            ("fatty_liver", "Fatty Liver"),
            ("lipid_disorders", "Lipid Disorders"),
            ("heart_ailments", "Heart Ailments"),
            ("thyroid_disorders", "Thyroid Disorders"),
            ("pcos", "PCOS"),
            ("stroke", "Stroke"),
            ("mental_health", "Mental Health"),
            ("other", "Other"),
            ("none", "None"),
        ],
    ),
    (
        8,
        [
            ("type_2_diabetes", "Type 2 Diabetes"),
            ("hypertension", "Hypertension"),
            ("fatty_liver", "Fatty Liver"),
            ("lipid_disorders", "Lipid Disorders"),
            ("heart_ailments", "Heart Ailments"),
            ("thyroid_disorders", "Thyroid Disorders"),
            ("pcos", "PCOS"),
            ("stroke", "Stroke"),
            ("mental_health", "Mental Health"),
            ("other", "Other"),
            ("none", "None"),
        ],
    ),
    (
        9,
        [
            ("type_2_diabetes", "Type 2 Diabetes"),
            ("hypertension", "Hypertension"),
            ("fatty_liver", "Fatty Liver"),
            ("lipid_disorders", "Lipid Disorders"),
            ("heart_ailments", "Heart Ailments"),
            ("thyroid_disorders", "Thyroid Disorders"),
            ("pcos", "PCOS"),
            ("stroke", "Stroke"),
            ("mental_health", "Mental Health"),
            ("other", "Other"),
            ("none", "None"),
        ],
    ),
    (13, [("lt_1_hr", "Less than 1 hour"), ("1_4_hr", "1–4 hours"), ("gt_4_hr", "More than 4 hours")]),
    (
        14,
        [
            ("lt_30_min", "Less than 30 minutes a day"),
            ("30_60_min", "30–60 minutes a day"),
            ("gt_60_min", "More than 60 minutes a day"),
            ("rarely", "Rarely or never"),
        ],
    ),
    (
        15,
        [
            ("lt_1_hr", "Less than 1 hour"),
            ("1_3_hr", "1–3 hours"),
            ("4_8_hr", "4–8 hours"),
            ("gt_8_hr", "More than 8 hours"),
            ("rarely", "Rarely or never"),
        ],
    ),
    (16, [("low", "Low intensity"), ("moderate", "Moderate intensity"), ("high", "High intensity")]),
    (
        17,
        [
            ("lt_15_min", "Less than 15 mins"),
            ("15_30_min", "15–30 mins"),
            ("30_60_min", "30–60 mins"),
            ("1_2_hr", "1–2 hours"),
            ("gt_2_hr", "More than 2 hours"),
        ],
    ),
    (
        18,
        [
            ("lt_5_hr", "Less than 5 hours"),
            ("5_7_hr", "Between 5–7 hours"),
            ("7_9_hr", "Between 7–9 hours"),
            ("gt_9_hr", "More than 9 hours"),
        ],
    ),
    (
        19,
        [
            ("none", "I do not drink alcohol"),
            ("quit", "I quit alcohol"),
            ("1_2_6m", "1–2 times in 6 months"),
            ("1_2_3m", "1–2 times in 3 months"),
            ("3_per_week", "3 servings per week or less"),
            ("gt_3_week", "More than 3 servings per week"),
        ],
    ),
    (
        20,
        [
            ("none", "I do not smoke"),
            ("quit", "I quit smoking"),
            ("1_2_month", "1–2 times a month"),
            ("1_3_week", "1–3 times a week"),
            ("4_5_week", "4–5 times a week"),
            ("5_7_week", "5–7 times a week"),
            ("gt_7_week", "More than 7 times a week"),
        ],
    ),
    (
        21,
        [
            ("weight_loss", "Weight Loss"),
            ("muscle_gain", "Building Muscle Mass"),
            ("metabolic_health", "Improving Metabolic Health"),
            ("energy", "Increase Energy Levels"),
            ("endurance", "Improving Physical Endurance"),
            ("strength", "Increasing Strength"),
        ],
    ),
    (
        22,
        [
            ("diet", "Reducing daily diet intake"),
            ("habits", "Forming healthy habits"),
            ("activity", "Increasing physical activity"),
        ],
    ),
    (
        23,
        [
            ("veg", "Veg"),
            ("non_veg", "Non-Veg"),
            ("eggetarian", "Eggetarian"),
            ("jain", "Jain"),
            ("flexitarian", "Flexitarian"),
            ("pescatarian", "Pescatarian"),
        ],
    ),
    (
        24,
        [
            ("pulses", "Pulses / Legumes"),
            ("fruits", "Fresh Fruits"),
            ("vegetables", "Fresh Vegetables"),
            ("nuts", "Nuts / Seeds"),
            ("grains", "Whole Grains"),
            ("eggs", "Eggs"),
            ("milk", "Milk / Curd"),
            ("chicken_fish", "Chicken / Fish"),
            ("cruciferous", "Cruciferous Vegetables"),
            ("none", "None"),
        ],
    ),
    (25, [("gt_5", "More than 5 times"), ("lt_5", "Less than 5 times"), ("none", "Do not have breakfast")]),
    (
        26,
        [
            ("daily_1_2", "1–2 times per day"),
            ("weekly_2_3", "2–3 times a week"),
            ("weekly_1", "Once a week or less"),
            ("rare", "Rarely or never"),
            ("monthly", "1–2 times per month"),
        ],
    ),
    (
        27,
        [
            ("weekly_1", "Once a week or less"),
            ("weekly_2_3", "2–3 times a week"),
            ("gt_4_week", "4 or more times a week"),
            ("rare", "Rarely or never"),
            ("monthly", "1–2 times per month"),
        ],
    ),
    (
        28,
        [
            ("daily_1_2", "1–2 times per day"),
            ("weekly_2_3", "2–3 times a week"),
            ("weekly_1", "Once a week or less"),
            ("rare", "Rarely or never"),
            ("monthly", "1–2 times per month"),
        ],
    ),
    (
        29,
        [
            ("daily", "1–2 times per day"),
            ("weekly_2_3", "2–3 times a week"),
            ("gt_4_week", "4 or more times a week"),
            ("rare", "Rarely or never"),
            ("monthly", "1–2 times per month"),
        ],
    ),
    (30, [("yes", "Yes"), ("no", "No")]),
    (31, [("never", "Never"), ("rarely", "Rarely"), ("usually", "Usually")]),
    (
        32,
        [
            ("none", "I do not drink coffee or tea"),
            ("0_1_day", "0–1 cups per day"),
            ("1_2_day", "1–2 cups per day"),
            ("gt_2_day", "More than 2 cups per day"),
            ("weekly", "2–3 times a week"),
        ],
    ),
    (
        33,
        [
            ("tea_milk_sugar", "Tea with sugar & milk"),
            ("green_tea", "Green tea"),
            ("coffee_milk_sugar", "Coffee with sugar & milk"),
            ("milk_tea_no_sugar", "Milk tea without sugar"),
            ("black_coffee", "Black coffee"),
            ("black_tea", "Black tea"),
            ("milk_coffee_no_sugar", "Milk coffee without sugar"),
        ],
    ),
    (
        34,
        [
            ("weekly_1", "Once a week or less"),
            ("weekly_2_3", "2–3 times a week"),
            ("gt_4_week", "4 or more times a week"),
            ("rare", "Rarely or never"),
            ("monthly", "1–2 times per month"),
        ],
    ),
    (
        35,
        [
            ("weekly_1", "Once a week or less"),
            ("weekly_2_3", "2–3 times a week"),
            ("gt_4_week", "4 or more times a week"),
            ("rare", "Rarely or never"),
            ("monthly", "1–2 times per month"),
        ],
    ),
    (
        36,
        [
            ("lt_2", "Less than 2 glasses"),
            ("2", "2 glasses"),
            ("4", "4 glasses"),
            ("6", "6 glasses"),
            ("8", "8 glasses"),
            ("gt_8", "More than 8 glasses"),
        ],
    ),
    (
        37,
        [
            ("rare", "Rarely or Never"),
            ("1_2", "1 to 2 times"),
            ("2_3", "2 to 3 times"),
            ("4_5", "4 to 5 times"),
            ("gt_6", "More than 6 times"),
        ],
    ),
    (38, [("mmhg", "mmHg")]),
    (39, [("mmhg", "mmHg")]),
]


def _build_options() -> tuple[SeedOption, ...]:
    out: list[SeedOption] = []
    oid = 1
    for qid, pairs in _OPT_SPEC:
        for ov, dn in pairs:
            out.append(SeedOption(oid, qid, ov, dn, None))
            oid += 1
    return tuple(out)


METSIGHTS_OPTIONS: tuple[SeedOption, ...] = _build_options()

# Package 1–2: all five sections. Package 3 (FitPrint): Metsights fitness-parameters + anthropometry overlap; no vitals API.
METSIGHTS_PACKAGE_CATEGORIES: tuple[SeedPackageCategory, ...] = (
    SeedPackageCategory(10001, 1, 1),
    SeedPackageCategory(10002, 1, 2),
    SeedPackageCategory(10003, 1, 3),
    SeedPackageCategory(10004, 1, 4),
    SeedPackageCategory(10005, 1, 5),
    SeedPackageCategory(10006, 2, 1),
    SeedPackageCategory(10007, 2, 2),
    SeedPackageCategory(10008, 2, 3),
    SeedPackageCategory(10009, 2, 4),
    SeedPackageCategory(10010, 2, 5),
    SeedPackageCategory(10011, 3, 1),
    SeedPackageCategory(10012, 3, 3),
    SeedPackageCategory(10013, 3, 4),
)
