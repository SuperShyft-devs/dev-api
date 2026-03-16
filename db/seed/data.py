from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class SeedUser:
    user_id: int
    first_name: str
    last_name: str
    age: int
    phone: str
    email: str
    date_of_birth: date
    gender: str
    address: str
    pin_code: str
    city: str
    state: str
    country: str
    referred_by: str | None
    is_participant: bool
    status: str


@dataclass(frozen=True)
class SeedEmployee:
    employee_id: int
    user_id: int
    role: str
    status: str


@dataclass(frozen=True)
class SeedAssessmentPackage:
    package_id: int
    package_code: str
    display_name: str
    status: str


@dataclass(frozen=True)
class SeedCategory:
    category_id: int
    category_key: str
    display_name: str
    status: str


@dataclass(frozen=True)
class SeedQuestion:
    question_id: int
    question_key: str
    question_text: str
    question_type: str
    is_required: bool
    is_read_only: bool
    help_text: str | None
    status: str


@dataclass(frozen=True)
class SeedCategoryQuestion:
    id: int
    category_id: int
    question_id: int


@dataclass(frozen=True)
class SeedOption:
    option_id: int
    question_id: int
    option_value: str
    display_name: str
    tooltip_text: str | None


@dataclass(frozen=True)
class SeedPackageCategory:
    id: int
    package_id: int
    category_id: int


DEFAULT_USERS: tuple[SeedUser, ...] = (
    SeedUser(
        user_id=1,
        first_name="Rishi",
        last_name="Nagar",
        age=30,
        phone="9898898912",
        email="rishi@supershyft.com",
        date_of_birth=date(1995, 1, 1),
        gender="male",
        address="Sher-E-Punjab, Pump House, Andheri East",
        pin_code="400059",
        city="Mumbai",
        state="Maharashtra",
        country="India",
        referred_by=None,
        is_participant=False,
        status="active",
    ),
)

DEFAULT_EMPLOYEES: tuple[SeedEmployee, ...] = (
    SeedEmployee(employee_id=1, user_id=1, role="admin", status="active"),
)

DEFAULT_ASSESSMENT_PACKAGES: tuple[SeedAssessmentPackage, ...] = (
    SeedAssessmentPackage(
        package_id=1,
        package_code="METSIGHTS_BASIC",
        display_name="Metsights Basic",
        status="active",
    ),
    SeedAssessmentPackage(
        package_id=2,
        package_code="METSIGHTS_PRO",
        display_name="Metsights Pro",
        status="active",
    ),
)

DEFAULT_CATEGORIES: tuple[SeedCategory, ...] = (
    SeedCategory(
        category_id=1,
        category_key="anthropometry",
        display_name="Anthropometry",
        status="active",
    ),
    SeedCategory(
        category_id=2,
        category_key="diet_and_lifestyle",
        display_name="Diet & Lifestyle",
        status="active",
    ),
    SeedCategory(
        category_id=3, category_key="vitals", display_name="Vitals", status="active"
    ),
)

DEFAULT_QUESTIONS: tuple[SeedQuestion, ...] = (
    # Anthropometry
    SeedQuestion(
        question_id=1,
        question_key="weight",
        question_text="What is your Weight",
        question_type="scale",
        is_required=True,
        is_read_only=False,
        help_text="Enter your weight (kg).",
        status="active",
    ),
    SeedQuestion(
        question_id=2,
        question_key="height",
        question_text="What is your Height",
        question_type="scale",
        is_required=True,
        is_read_only=False,
        help_text="Enter your height (cm or ft/in).",
        status="active",
    ),
    SeedQuestion(
        question_id=3,
        question_key="waist_circumference",
        question_text="Waist Circumference",
        question_type="scale",
        is_required=True,
        is_read_only=False,
        help_text="Enter your waist circumference (cm or in).",
        status="active",
    ),
    SeedQuestion(
        question_id=4,
        question_key="bmi",
        question_text="BMI",
        question_type="scale",
        is_required=False,
        is_read_only=True,
        help_text="Body Mass Index — calculated automatically (kg/m²).",
        status="active",
    ),
    # Vitals
    SeedQuestion(
        question_id=5,
        question_key="systolic_blood_pressure",
        question_text="Systolic Blood Pressure",
        question_type="scale",
        is_required=True,
        is_read_only=False,
        help_text="Enter systolic blood pressure in mmHg.",
        status="active",
    ),
    SeedQuestion(
        question_id=6,
        question_key="diastolic_blood_pressure",
        question_text="Diastolic Blood Pressure",
        question_type="scale",
        is_required=True,
        is_read_only=False,
        help_text="Enter diastolic blood pressure in mmHg.",
        status="active",
    ),
    SeedQuestion(
        question_id=7,
        question_key="resting_heart_rate",
        question_text="Resting Heart Rate",
        question_type="scale",
        is_required=False,
        is_read_only=False,
        help_text="Enter resting heart rate in bpm.",
        status="active",
    ),
    SeedQuestion(
        question_id=8,
        question_key="respiratory_rate",
        question_text="Respiratory Rate",
        question_type="scale",
        is_required=False,
        is_read_only=False,
        help_text="Enter respiratory rate in breaths per minute.",
        status="active",
    ),
    SeedQuestion(
        question_id=9,
        question_key="hrv_sdnn",
        question_text="Heart Rate Variability (HRV-SDNN)",
        question_type="scale",
        is_required=False,
        is_read_only=False,
        help_text="Enter heart rate variability (SDNN) in ms.",
        status="active",
    ),
    # Diet & Lifestyle
    SeedQuestion(
        question_id=10,
        question_key="region_lived",
        question_text="Where have you lived for most of your life?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=11,
        question_key="primary_diet_type",
        question_text="What type of diet do you primarily consume?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=12,
        question_key="daily_food_groups",
        question_text="Which of the following food groups do you consume every day? (Select all that apply)",
        question_type="multi_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=13,
        question_key="healthy_breakfast_frequency",
        question_text="How frequently do you have a healthy homemade breakfast in a week?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=14,
        question_key="fresh_fruit_frequency",
        question_text="How frequently do you consume fresh fruits?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=15,
        question_key="fresh_vegetable_frequency",
        question_text="How frequently do you consume fresh vegetables?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=16,
        question_key="cookies_biscuits_frequency",
        question_text="How frequently do you consume cookies, biscuits, bread, or cakes?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=17,
        question_key="red_meat_frequency",
        question_text="How frequently do you consume red meat (i.e., mutton, lamb, beef, pork)?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=18,
        question_key="market_butter_frequency",
        question_text="How frequently do you indulge in dishes that are rich in market butter?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=19,
        question_key="sugary_drinks_frequency",
        question_text="How frequently do you consume sugary drinks and desserts?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text="Example: soft drinks, ice cream, chocolate, cakes, pastries, candies, or sweets.",
        status="active",
    ),
    SeedQuestion(
        question_id=20,
        question_key="coffee_tea_intake",
        question_text="What's your daily coffee or tea intake?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=21,
        question_key="iodized_salt",
        question_text="Do you use iodized salt in your diet?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=22,
        question_key="extra_salt_frequency",
        question_text="How often do you add extra salt to your food?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=23,
        question_key="sitting_duration",
        question_text="How long do you sit continuously every day due to work or lifestyle?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=24,
        question_key="physical_activity_duration",
        question_text="How much time do you spend engaging in physical activity or exercise daily?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text="Brisk walking or bicycling or heavy lifting or games or yoga or meditation or cleaning",
        status="active",
    ),
    SeedQuestion(
        question_id=25,
        question_key="sleep_duration",
        question_text="What is your average duration of good quality sleep?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=26,
        question_key="alcohol_consumption",
        question_text="What is your weekly alcohol consumption?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text="1 serving = 125 mL of wine or 330 mL of beer or 40 mL of hard liquor",
        status="active",
    ),
    SeedQuestion(
        question_id=27,
        question_key="smoking_frequency",
        question_text="How often do you smoke cigarettes or tobacco in a week?",
        question_type="single_choice",
        is_required=True,
        is_read_only=False,
        help_text=None,
        status="active",
    ),
    SeedQuestion(
        question_id=28,
        question_key="family_health_conditions",
        question_text="Do any of your close blood relatives (i.e., parents or siblings) have the following health conditions?",
        question_type="multi_choice",
        is_required=True,
        is_read_only=False,
        help_text="Select multiple or None that apply.",
        status="active",
    ),
    SeedQuestion(
        question_id=29,
        question_key="diagnosed_diseases",
        question_text="Are you diagnosed with the following diseases?",
        question_type="multi_choice",
        is_required=True,
        is_read_only=False,
        help_text="Select multiple or None that apply.",
        status="active",
    ),
)

DEFAULT_CATEGORY_QUESTIONS: tuple[SeedCategoryQuestion, ...] = (
    # Anthropometry
    SeedCategoryQuestion(id=1, category_id=1, question_id=1),
    SeedCategoryQuestion(id=2, category_id=1, question_id=2),
    SeedCategoryQuestion(id=3, category_id=1, question_id=3),
    SeedCategoryQuestion(id=4, category_id=1, question_id=4),
    # Vitals
    SeedCategoryQuestion(id=5, category_id=3, question_id=5),
    SeedCategoryQuestion(id=6, category_id=3, question_id=6),
    SeedCategoryQuestion(id=7, category_id=3, question_id=7),
    SeedCategoryQuestion(id=8, category_id=3, question_id=8),
    SeedCategoryQuestion(id=9, category_id=3, question_id=9),
    # Diet & Lifestyle
    SeedCategoryQuestion(id=10, category_id=2, question_id=10),
    SeedCategoryQuestion(id=11, category_id=2, question_id=11),
    SeedCategoryQuestion(id=12, category_id=2, question_id=12),
    SeedCategoryQuestion(id=13, category_id=2, question_id=13),
    SeedCategoryQuestion(id=14, category_id=2, question_id=14),
    SeedCategoryQuestion(id=15, category_id=2, question_id=15),
    SeedCategoryQuestion(id=16, category_id=2, question_id=16),
    SeedCategoryQuestion(id=17, category_id=2, question_id=17),
    SeedCategoryQuestion(id=18, category_id=2, question_id=18),
    SeedCategoryQuestion(id=19, category_id=2, question_id=19),
    SeedCategoryQuestion(id=20, category_id=2, question_id=20),
    SeedCategoryQuestion(id=21, category_id=2, question_id=21),
    SeedCategoryQuestion(id=22, category_id=2, question_id=22),
    SeedCategoryQuestion(id=23, category_id=2, question_id=23),
    SeedCategoryQuestion(id=24, category_id=2, question_id=24),
    SeedCategoryQuestion(id=25, category_id=2, question_id=25),
    SeedCategoryQuestion(id=26, category_id=2, question_id=26),
    SeedCategoryQuestion(id=27, category_id=2, question_id=27),
    SeedCategoryQuestion(id=28, category_id=2, question_id=28),
    SeedCategoryQuestion(id=29, category_id=2, question_id=29),
)

DEFAULT_OPTIONS: tuple[SeedOption, ...] = (
    # Region lived (single_choice)
    SeedOption(
        option_id=1,
        question_id=10,
        option_value="coastal",
        display_name="Coastal region",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=2,
        question_id=10,
        option_value="inland",
        display_name="Inland region",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=3,
        question_id=11,
        option_value="vegetarian",
        display_name="Vegetarian",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=4,
        question_id=11,
        option_value="non_vegetarian",
        display_name="Non-Vegetarian",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=5,
        question_id=11,
        option_value="eggetarian",
        display_name="Eggetarian",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=6,
        question_id=11,
        option_value="pescatarian",
        display_name="Pescatarian",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=7,
        question_id=11,
        option_value="flexitarian",
        display_name="Flexitarian",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=8,
        question_id=11,
        option_value="jain",
        display_name="Jain",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=9,
        question_id=12,
        option_value="whole_grains",
        display_name="Whole grains",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=10,
        question_id=12,
        option_value="pulses_legumes",
        display_name="Pulses/ Legumes",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=11,
        question_id=12,
        option_value="whole_milk_curd",
        display_name="Whole Milk/ Curd",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=12,
        question_id=12,
        option_value="fresh_vegetables",
        display_name="Fresh vegetables",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=13,
        question_id=12,
        option_value="fresh_fruits",
        display_name="Fresh fruits",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=14,
        question_id=12,
        option_value="nuts_seeds",
        display_name="Nuts/ Seeds",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=15,
        question_id=12,
        option_value="eggs",
        display_name="Eggs",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=16,
        question_id=12,
        option_value="chicken_fish",
        display_name="Chicken/ Fish",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=17,
        question_id=12,
        option_value="cruciferous",
        display_name="Cruciferous (Cauliflower, Cabbage)",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=18,
        question_id=12,
        option_value="none",
        display_name="None",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=19,
        question_id=13,
        option_value="do_not_have_breakfast",
        display_name="Do not have breakfast",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=20,
        question_id=13,
        option_value="less_than_5_times",
        display_name="Less than 5 times",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=21,
        question_id=13,
        option_value="more_than_5_times",
        display_name="More than 5 times",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=22,
        question_id=14,
        option_value="1_2_times_per_day",
        display_name="1-2 times per day",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=23,
        question_id=14,
        option_value="2_3_times_a_week",
        display_name="2-3 times a week",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=24,
        question_id=14,
        option_value="once_a_week_or_less",
        display_name="Once a week or less",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=25,
        question_id=14,
        option_value="1_2_times_per_month",
        display_name="1-2 times per month",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=26,
        question_id=14,
        option_value="rarely_or_never",
        display_name="Rarely or never",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=27,
        question_id=15,
        option_value="1_2_times_per_day",
        display_name="1-2 times per day",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=28,
        question_id=15,
        option_value="2_3_times_a_week",
        display_name="2-3 times a week",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=29,
        question_id=15,
        option_value="once_a_week_or_less",
        display_name="Once a week or less",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=30,
        question_id=15,
        option_value="1_2_times_per_month",
        display_name="1-2 times per month",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=31,
        question_id=15,
        option_value="rarely_or_never",
        display_name="Rarely or never",
        tooltip_text=None,
    ),
    # cookies/biscuits (q16) — "4 or more" instead of "1-2 per day"
    SeedOption(
        option_id=32,
        question_id=16,
        option_value="4_or_more_times_a_week",
        display_name="4 or more times a week",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=33,
        question_id=16,
        option_value="2_3_times_a_week",
        display_name="2-3 times a week",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=34,
        question_id=16,
        option_value="once_a_week_or_less",
        display_name="Once a week or less",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=35,
        question_id=16,
        option_value="1_2_times_per_month",
        display_name="1-2 times per month",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=36,
        question_id=16,
        option_value="rarely_or_never",
        display_name="Rarely or never",
        tooltip_text=None,
    ),
    # red meat (q17)
    SeedOption(
        option_id=37,
        question_id=17,
        option_value="4_or_more_times_a_week",
        display_name="4 or more times a week",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=38,
        question_id=17,
        option_value="2_3_times_a_week",
        display_name="2-3 times a week",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=39,
        question_id=17,
        option_value="once_a_week_or_less",
        display_name="Once a week or less",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=40,
        question_id=17,
        option_value="1_2_times_per_month",
        display_name="1-2 times per month",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=41,
        question_id=17,
        option_value="rarely_or_never",
        display_name="Rarely or never",
        tooltip_text=None,
    ),
    # market butter (q18)
    SeedOption(
        option_id=42,
        question_id=18,
        option_value="4_or_more_times_a_week",
        display_name="4 or more times a week",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=43,
        question_id=18,
        option_value="2_3_times_a_week",
        display_name="2-3 times a week",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=44,
        question_id=18,
        option_value="once_a_week_or_less",
        display_name="Once a week or less",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=45,
        question_id=18,
        option_value="1_2_times_per_month",
        display_name="1-2 times per month",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=46,
        question_id=18,
        option_value="rarely_or_never",
        display_name="Rarely or never",
        tooltip_text=None,
    ),
    # sugary drinks (q19)
    SeedOption(
        option_id=47,
        question_id=19,
        option_value="4_or_more_times_a_week",
        display_name="4 or more times a week",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=48,
        question_id=19,
        option_value="2_3_times_a_week",
        display_name="2-3 times a week",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=49,
        question_id=19,
        option_value="once_a_week_or_less",
        display_name="Once a week or less",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=50,
        question_id=19,
        option_value="1_2_times_per_month",
        display_name="1-2 times per month",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=51,
        question_id=19,
        option_value="rarely_or_never",
        display_name="Rarely or never",
        tooltip_text=None,
    ),
    # coffee/tea (q20)
    SeedOption(
        option_id=52,
        question_id=20,
        option_value="no_coffee_or_tea",
        display_name="I do not drink coffee or tea",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=53,
        question_id=20,
        option_value="1_2_cups_per_day",
        display_name="1-2 cups per day",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=54,
        question_id=20,
        option_value="more_than_2_cups_per_day",
        display_name="More than 2 cups per day",
        tooltip_text=None,
    ),
    # iodized salt (q21)
    SeedOption(
        option_id=55,
        question_id=21,
        option_value="yes",
        display_name="Yes",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=56,
        question_id=21,
        option_value="no",
        display_name="No",
        tooltip_text=None,
    ),
    # extra salt (q22)
    SeedOption(
        option_id=57,
        question_id=22,
        option_value="never",
        display_name="Never",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=58,
        question_id=22,
        option_value="rarely",
        display_name="Rarely",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=59,
        question_id=22,
        option_value="usually",
        display_name="Usually",
        tooltip_text=None,
    ),
    # sitting duration (q23)
    SeedOption(
        option_id=60,
        question_id=23,
        option_value="less_than_1_hour",
        display_name="Less than 1 hour",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=61,
        question_id=23,
        option_value="1_4_hours",
        display_name="1-4 hours",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=62,
        question_id=23,
        option_value="more_than_4_hours",
        display_name="More than 4 hours",
        tooltip_text=None,
    ),
    # physical activity (q24)
    SeedOption(
        option_id=63,
        question_id=24,
        option_value="rarely_or_never",
        display_name="Rarely or never",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=64,
        question_id=24,
        option_value="less_than_30_min_a_day",
        display_name="Less than 30 minutes a day",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=65,
        question_id=24,
        option_value="30_60_min_a_day",
        display_name="30-60 minutes a day",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=66,
        question_id=24,
        option_value="more_than_60_min_a_day",
        display_name="More than 60 minutes a day",
        tooltip_text=None,
    ),
    # sleep duration (q25)
    SeedOption(
        option_id=67,
        question_id=25,
        option_value="less_than_5_hours",
        display_name="Less than 5 hours",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=68,
        question_id=25,
        option_value="5_to_7_hours",
        display_name="Between 5 to 7 hours",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=69,
        question_id=25,
        option_value="7_to_9_hours",
        display_name="Between 7 to 9 hours",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=70,
        question_id=25,
        option_value="more_than_9_hours",
        display_name="More than 9 hours",
        tooltip_text=None,
    ),
    # alcohol (q26)
    SeedOption(
        option_id=71,
        question_id=26,
        option_value="no_alcohol",
        display_name="I do not drink alcohol",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=72,
        question_id=26,
        option_value="quit_alcohol",
        display_name="I quit alcohol",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=73,
        question_id=26,
        option_value="3_servings_per_week_or_less",
        display_name="3 servings per week or less",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=74,
        question_id=26,
        option_value="more_than_3_servings_per_week",
        display_name="More than 3 servings per week",
        tooltip_text=None,
    ),
    # smoking (q27)
    SeedOption(
        option_id=75,
        question_id=27,
        option_value="do_not_smoke",
        display_name="I do not smoke",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=76,
        question_id=27,
        option_value="quit_smoking",
        display_name="I quit smoking",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=77,
        question_id=27,
        option_value="1_to_3_times_a_week",
        display_name="1 to 3 times a week",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=78,
        question_id=27,
        option_value="5_to_7_times_a_week",
        display_name="5 to 7 times a week",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=79,
        question_id=27,
        option_value="more_than_7_times_a_week",
        display_name="More than 7 times a week",
        tooltip_text=None,
    ),
    # family health conditions (q28)
    SeedOption(
        option_id=80,
        question_id=28,
        option_value="type_2_diabetes",
        display_name="Type 2 diabetes",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=81,
        question_id=28,
        option_value="hypertension",
        display_name="Hypertension",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=82,
        question_id=28,
        option_value="fatty_liver",
        display_name="Fatty liver",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=83,
        question_id=28,
        option_value="lipid_disorders",
        display_name="Lipid disorders",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=84,
        question_id=28,
        option_value="heart_ailments",
        display_name="Heart ailments",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=85,
        question_id=28,
        option_value="thyroid_disorders",
        display_name="Thyroid disorders",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=86,
        question_id=28,
        option_value="pcos",
        display_name="PCOS",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=87,
        question_id=28,
        option_value="stroke",
        display_name="Stroke",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=88,
        question_id=28,
        option_value="mental_health",
        display_name="Mental Health",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=89,
        question_id=28,
        option_value="other",
        display_name="Other",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=90,
        question_id=28,
        option_value="none",
        display_name="None",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=91,
        question_id=29,
        option_value="type_2_diabetes",
        display_name="Type 2 diabetes",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=92,
        question_id=29,
        option_value="hypertension",
        display_name="Hypertension",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=93,
        question_id=29,
        option_value="fatty_liver",
        display_name="Fatty liver",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=94,
        question_id=29,
        option_value="lipid_disorders",
        display_name="Lipid disorders",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=95,
        question_id=29,
        option_value="heart_ailments",
        display_name="Heart ailments",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=96,
        question_id=29,
        option_value="thyroid_disorders",
        display_name="Thyroid disorders",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=97,
        question_id=29,
        option_value="pcos",
        display_name="PCOS",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=98,
        question_id=29,
        option_value="stroke",
        display_name="Stroke",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=99,
        question_id=29,
        option_value="mental_health",
        display_name="Mental Health",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=100,
        question_id=29,
        option_value="other",
        display_name="Other",
        tooltip_text=None,
    ),
    SeedOption(
        option_id=101,
        question_id=29,
        option_value="none",
        display_name="None",
        tooltip_text=None,
    ),
)

DEFAULT_PACKAGE_CATEGORIES: tuple[SeedPackageCategory, ...] = (
    SeedPackageCategory(id=1, package_id=2, category_id=1),
    SeedPackageCategory(id=2, package_id=2, category_id=2),
    SeedPackageCategory(id=3, package_id=2, category_id=3),
    SeedPackageCategory(id=1, package_id=1, category_id=1),
    SeedPackageCategory(id=2, package_id=1, category_id=2),
    SeedPackageCategory(id=3, package_id=1, category_id=3),
)
