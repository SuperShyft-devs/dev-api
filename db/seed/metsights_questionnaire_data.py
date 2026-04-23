"""Questionnaire seed: SuperShyft grouping with Metsights API field keys and OPTIONS choice codes."""

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
    SeedQuestion(6, "living_region", "Where have you lived most of your life?", "single_choice", True, False, None, "active"),
    SeedQuestion(
        7,
        "family_health_history",
        "Do any of your close blood relatives have the following health conditions?",
        "multiple_choice",
        True,
        False,
        "Select multiple or None",
        "active",
    ),
    SeedQuestion(8, "diagnosed_diseases", "Are you diagnosed with the following diseases?", "multiple_choice", True, False, "Select multiple or None", "active"),
    SeedQuestion(9, "diagnosed_diseases_medications", "Are you taking medications for the following diseases?", "multiple_choice", False, False, "Select multiple or None", "active"),
    SeedQuestion(10, "family_health_history_other", "Family health history (other)", "text", False, False, None, "active"),
    SeedQuestion(11, "diagnosed_diseases_other", "Diagnosed diseases (other)", "text", False, False, None, "active"),
    SeedQuestion(12, "diagnosed_diseases_medications_other", "Medications (other)", "text", False, False, None, "active"),
    SeedQuestion(13, "sitting_hours", "How long do you sit continuously every day due to work or lifestyle?", "single_choice", True, False, None, "active"),
    SeedQuestion(
        14,
        "physical_activity_frequency",
        "How much time do you spend engaging in physical activity or exercise daily?",
        "single_choice",
        True,
        False,
        None,
        "active",
    ),
    SeedQuestion(
        15,
        "exercise_frequency_week",
        "On a typical week, how much time do you dedicate to leisure activities, workouts or sports?",
        "single_choice",
        True,
        False,
        None,
        "active",
    ),
    SeedQuestion(16, "exercise_level", "On an average week, how would you rate the intensity of your activities or workouts?", "single_choice", True, False, None, "active"),
    SeedQuestion(
        17,
        "daily_active_duration",
        "How much time do you spend actively walking each day?",
        "scale",
        False,
        False,
        "Includes commuting, breaks, and household chores.",
        "active",
    ),
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
    SeedQuestion(33, "caffeine_type", "What type of coffee or tea do you drink?", "single_choice", False, False, None, "active"),
    SeedQuestion(34, "butter_dish_frequency", "How frequently do you indulge in dishes rich in butter?", "single_choice", True, False, None, "active"),
    SeedQuestion(35, "red_meat_frequency", "How frequently do you consume red meat?", "single_choice", True, False, None, "active"),
    SeedQuestion(36, "water_intake_frequency", "How many glasses of water do you drink in a day?", "single_choice", True, False, None, "active"),
    SeedQuestion(37, "sickness_frequency", "How often do you fall sick in a year?", "single_choice", True, False, None, "active"),
    SeedQuestion(38, "systolic_blood_pressure", "Systolic Blood Pressure", "scale", True, False, "Top number of your blood pressure reading", "active"),
    SeedQuestion(39, "diastolic_blood_pressure", "Diastolic Blood Pressure", "scale", True, False, "Bottom number of your blood pressure reading", "active"),
    SeedQuestion(
        40,
        "weight_loss_goal",
        "What is your desired weight loss goal in upcoming months?",
        "scale",
        False,
        False,
        "FitPrint only; paired with weight_loss_goal_unit (kg/lb).",
        "active",
    ),
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
    # weight_loss_goal (Q40) belongs to the Lifestyle & Habits section so it
    # imports cleanly for the FitPrint package (which maps to categories 1/2/3/4).
    links.append(SeedCategoryQuestion(lid, 3, 40))
    lid += 1
    return tuple(links)


METSIGHTS_CATEGORY_QUESTIONS: tuple[SeedCategoryQuestion, ...] = _build_category_questions()

# (question_id, [(option_value, display_name), ...]) — option_value matches Metsights OPTIONS ``value`` strings.
_OPT_SPEC: list[tuple[int, list[tuple[str, str]]]] = [
    (1, [("0", "cm"), ("2", "ft/in")]),
    (2, [("0", "kg"), ("1", "lb")]),
    (3, [("0", "cm"), ("1", "in")]),
    (4, [("0", "cm"), ("1", "in")]),
    (5, [("0", "%")]),
    (6, [("0", "Coastal region"), ("1", "Inland region")]),
    (
        7,
        [
            ("0", "Type 2 diabetes"),
            ("1", "Hypertension"),
            ("2", "Fatty liver"),
            ("3", "Lipid disorders"),
            ("4", "Heart ailments"),
            ("5", "Thyroid disorders"),
            ("8", "PCOS"),
            ("6", "Stroke"),
            ("7", "Mental Health"),
            ("O", "Other"),
        ],
    ),
    (
        8,
        [
            ("0", "Type 2 diabetes"),
            ("1", "Hypertension"),
            ("2", "Fatty liver"),
            ("3", "Lipid disorders"),
            ("4", "Heart ailments"),
            ("5", "Thyroid disorders"),
            ("8", "PCOS"),
            ("6", "Stroke"),
            ("7", "Mental Health"),
            ("O", "Other"),
        ],
    ),
    (
        9,
        [
            ("0", "Type 2 diabetes"),
            ("1", "Hypertension"),
            ("2", "Fatty liver"),
            ("3", "Lipid disorders"),
            ("4", "Heart ailments"),
            ("5", "Thyroid disorders"),
            ("8", "PCOS"),
            ("6", "Stroke"),
            ("7", "Mental Health"),
            ("O", "Other"),
        ],
    ),
    (13, [("0", "Less than 1 hour"), ("1", "1-4 hours"), ("2", "More than 4 hours")]),
    (
        14,
        [
            ("5", "Rarely or never"),
            ("1", "Less than 30 minutes a day"),
            ("2", "30-60 minutes a day"),
            ("3", "More than 60 minutes a day"),
        ],
    ),
    (
        15,
        [
            ("0", "Rarely or never"),
            ("1", "Less than 1-hour"),
            ("2", "1 to 3 hours"),
            ("3", "4 to 8 hours"),
            ("4", "More than 8 hours"),
        ],
    ),
    (16, [("0", "Low-intensity"), ("1", "Moderate-intensity"), ("2", "High-intensity")]),
    (17, [("0", "minutes daily"), ("1", "hours daily")]),
    (
        18,
        [
            ("0", "Less than 5 hours"),
            ("1", "Between 5 to 7 hours"),
            ("2", "Between 7 to 9 hours"),
            ("3", "More than 9 hours"),
        ],
    ),
    (
        19,
        [
            ("0", "I do not drink alcohol"),
            ("1", "I quit alcohol"),
            ("2", "3 servings per week or less"),
            ("3", "More than 3 servings per week"),
        ],
    ),
    (
        20,
        [
            ("0", "I do not smoke"),
            ("1", "I quit smoking"),
            ("2", "1 to 3 times a week"),
            ("3", "5 to 7 times a week"),
            ("4", "More than 7 times a week"),
        ],
    ),
    (
        21,
        [
            ("0", "Weight Loss"),
            ("1", "Building Muscle Mass"),
            ("2", "Improving Metabolic Health"),
            ("3", "Increasing Energy Levels"),
            ("4", "Increasing Strength"),
            ("5", "Improving Physical Endurance"),
        ],
    ),
    (
        22,
        [
            ("0", "Reducing daily diet intake"),
            ("1", "Increasing physical activity"),
            ("2", "Forming healthy habits"),
        ],
    ),
    (
        23,
        [
            ("0", "Vegetarian"),
            ("1", "Non-Vegetarian"),
            ("2", "Eggetarian"),
            ("3", "Pescatarian"),
            ("4", "Flexitarian"),
            ("5", "Jain"),
        ],
    ),
    (
        24,
        [
            ("0", "Whole grains"),
            ("1", "Pulses/ Legumes"),
            ("2", "Whole Milk/ Curd"),
            ("3", "Fresh vegetables"),
            ("4", "Fresh fruits"),
            ("5", "Nuts/ Seeds"),
            ("6", "Eggs"),
            ("7", "Chicken/ Fish"),
            ("9", "Cruciferous (Cauliflower, Cabbage)"),
        ],
    ),
    (25, [("0", "Do not have breakfast"), ("1", "Less than 5 times"), ("2", "More than 5 times")]),
    (
        26,
        [
            ("0", "1-2 times per day"),
            ("2", "2-3 times a week"),
            ("3", "Once a week or less"),
            ("4", "1-2 times per month"),
            ("5", "Rarely or never"),
        ],
    ),
    (
        27,
        [
            ("1", "4 or more times a week"),
            ("2", "2-3 times a week"),
            ("3", "Once a week or less"),
            ("4", "1-2 times per month"),
            ("5", "Rarely or never"),
        ],
    ),
    (
        28,
        [
            ("0", "1-2 times per day"),
            ("2", "2-3 times a week"),
            ("3", "Once a week or less"),
            ("4", "1-2 times per month"),
            ("5", "Rarely or never"),
        ],
    ),
    (
        29,
        [
            ("1", "4 or more times a week"),
            ("2", "2-3 times a week"),
            ("3", "Once a week or less"),
            ("4", "1-2 times per month"),
            ("5", "Rarely or never"),
        ],
    ),
    (30, [("true", "Yes"), ("false", "No")]),
    (31, [("0", "Never"), ("1", "Rarely"), ("2", "Usually")]),
    (
        32,
        [
            ("0", "I do not drink coffee or tea"),
            ("1", "1-2 cups per day"),
            ("2", "More than 2 cups per day"),
        ],
    ),
    (
        33,
        [
            ("1", "Green Tea"),
            ("2", "Black Tea"),
            ("3", "Tea with sugar and milk"),
            ("4", "Black Coffee"),
            ("5", "Coffee with sugar and milk"),
            ("6", "Milk Coffee without sugar"),
            ("7", "Milk Tea without sugar"),
        ],
    ),
    (
        34,
        [
            ("1", "4 or more times a week"),
            ("2", "2-3 times a week"),
            ("3", "Once a week or less"),
            ("4", "1-2 times per month"),
            ("5", "Rarely or never"),
        ],
    ),
    (
        35,
        [
            ("1", "4 or more times a week"),
            ("2", "2-3 times a week"),
            ("3", "Once a week or less"),
            ("4", "1-2 times per month"),
            ("5", "Rarely or never"),
        ],
    ),
    (
        36,
        [
            ("0", "Less than 2 glasses"),
            ("1", "2 glasses"),
            ("2", "4 glasses"),
            ("3", "6 glasses"),
            ("4", "8 glasses"),
            ("5", "More than 8 glasses"),
        ],
    ),
    (
        37,
        [
            ("0", "Rarely or Never"),
            ("1", "1 to 2 times"),
            ("2", "2 to 3 times"),
            ("3", "4 to 5 times"),
            ("4", "More than 6 times"),
        ],
    ),
    (38, [("0", "mmHG")]),
    (39, [("0", "mmHG")]),
    (40, [("0", "kg"), ("1", "lb")]),
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

# Package 1–2: all five sections.
# Package 3 (FitPrint) mirrors the Metsights ``/fitness-parameters/`` payload,
# which includes Anthropometry (1), Family History (2), Lifestyle & Habits (3),
# and Nutrition Log (4). Vitals (5) are not captured for FitPrint.
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
    SeedPackageCategory(10014, 3, 2),
)
