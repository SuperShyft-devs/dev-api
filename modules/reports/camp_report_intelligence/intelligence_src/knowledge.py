"""Shared severity bands — single source of truth.

Faithful port of src/intelligence/knowledge/severity.ts.
"""

from __future__ import annotations

import hashlib

from typing import Dict, List, Literal, Optional, Sequence, Tuple

from .models import (
    DiseaseInterventions,
    DiseaseKnowledge,
    HealthGraphEdge,
    InterventionId,
    InterventionKnowledge,
    LifestyleKnowledge,
    MetricId,
    ScoringWeights,
    SeverityBand,
)

SEVERITY_THRESHOLDS: List[Tuple[float, SeverityBand]] = [
    (10, "very_low"),
    (20, "low"),
    (40, "moderate"),
    (60, "high"),
    (float("inf"), "very_high"),
]

SEVERITY_WEIGHT: dict = {
    "very_low": 0.1,
    "low": 0.3,
    "moderate": 0.55,
    "high": 0.8,
    "very_high": 1,
}


def severity_from_prevalence(prevalence: float) -> SeverityBand:
    pct = max(0.0, min(100.0, prevalence))
    for max_value, band in SEVERITY_THRESHOLDS:
        if pct <= max_value:
            return band
    return "very_high"


def tone_from_severity(
    band: SeverityBand,
    mode: Literal["concern", "positive", "mixed", "leadership"],
) -> Literal["concern", "positive", "neutral"]:
    if mode == "positive":
        return "positive"
    if mode == "mixed":
        return "neutral"
    if band in ("very_low", "low"):
        return "positive"
    return "concern"


# Configurable scoring weights — never hardcode inside scoring functions.
SCORING_WEIGHTS = ScoringWeights(
    prevalence=0.5,
    intensity=0.25,
    severity=0.25,
    modifiability_boost=0.15,
    data_quality_floor=0.5,
)

# Node activation thresholds for graph evaluation (burden 0-100).
GRAPH_NODE_ACTIVATION_THRESHOLD = 30
GRAPH_STRENGTH_ACTIVATION_THRESHOLD = 55
GRAPH_EDGE_MIN_ACTIVATION = 0.25

CONFIDENCE_BANDS = {
    "high": 0.75,
    "moderate": 0.5,
}

TOP_RISK_LIMIT = 5
STRENGTH_LIMIT = 5


"""Faithful port of src/intelligence/knowledge/diseases.ts (including diseaseMetricFromName)."""

DISEASE_KNOWLEDGE: Dict[str, DiseaseKnowledge] = {
    "type_2_diabetes": DiseaseKnowledge(
        id="type_2_diabetes",
        display_name="Type 2 Diabetes",
        cluster="metabolic",
        clinical_focus=(
            "Rising diabetes risk suggests worsening blood sugar control across the workforce and a greater likelihood "
            "of developing long-term metabolic and cardiovascular complications."
        ),
        workplace_relevance="Strong predictor of future healthcare burden, productivity impact, and chronic disease progression.",
        lifestyle_drivers=["weight", "activity", "nutrition", "sleep", "stress"],
        related_metrics=["obesity", "physical_activity", "nutrition", "sleep", "nafld", "dyslipidemia"],
        biomarkers=["HbA1c", "Fasting Blood Sugar", "Fasting Insulin", "Triglycerides", "HDL", "ALT"],
        interventions=DiseaseInterventions(
            high=["metabolic_screening", "nutrition_refined_carb", "movement_programme"],
            medium=["sleep_health", "stress_management", "nutrition_whole_food"],
        ),
        modifiability=0.85,
        medical_frame_id="type_2_diabetes",
    ),

    "hypertension": DiseaseKnowledge(
        id="hypertension",
        display_name="Hypertension",
        cluster="cardiovascular",
        clinical_focus=(
            "Increasing blood-pressure risk suggests greater strain on cardiovascular health and a higher likelihood "
            "of future heart disease, stroke, and kidney-related complications."
        ),
        workplace_relevance="Often develops without noticeable symptoms, making regular screening essential for early detection and prevention.",
        lifestyle_drivers=["nutrition", "stress", "weight", "activity", "sleep", "alcohol", "smoking"],
        related_metrics=["cardiac_health", "dyslipidemia", "obesity", "sleep", "physical_activity"],
        biomarkers=["Creatinine", "eGFR", "Fasting Blood Sugar", "LDL", "HDL", "Triglycerides"],
        interventions=DiseaseInterventions(
            high=["bp_screening", "nutrition_sodium", "stress_management"],
            medium=["movement_programme", "sleep_health", "alcohol_moderation"],
        ),
        modifiability=0.8,
        medical_frame_id="hypertension",
    ),

    "obesity": DiseaseKnowledge(
        id="obesity",
        display_name="Obesity",
        cluster="metabolic",
        clinical_focus=(
            "Higher obesity levels indicate increasing weight-related health risk across the workforce, with potential "
            "effects on diabetes, heart health, liver health, and overall metabolic function."
        ),
        workplace_relevance="Major contributor to several chronic conditions and an important focus for preventive health programmes.",
        lifestyle_drivers=["nutrition", "activity", "sleep", "stress"],
        related_metrics=["physical_activity", "nutrition", "type_2_diabetes", "nafld", "dyslipidemia"],
        biomarkers=["Fasting Blood Sugar", "HbA1c", "Triglycerides", "HDL", "ALT"],
        interventions=DiseaseInterventions(
            high=["weight_management", "movement_programme", "nutrition_whole_food"],
            medium=["sleep_health", "nutrition_refined_carb"],
        ),
        modifiability=0.9,
        medical_frame_id="obesity",
    ),

    "pcos_pcod": DiseaseKnowledge(
        id="pcos_pcod",
        display_name="PCOS/PCOD",
        cluster="hormonal",
        clinical_focus=(
            "Higher PCOS-related risk highlights the connection between hormonal health, insulin resistance, and "
            "metabolic wellbeing among female employees."
        ),
        workplace_relevance="Common women's-health condition associated with metabolic changes and an increased long-term diabetes risk.",
        lifestyle_drivers=["nutrition", "activity", "weight", "stress", "sleep"],
        related_metrics=["obesity", "type_2_diabetes", "nutrition", "physical_activity"],
        biomarkers=["Fasting Insulin", "Fasting Blood Sugar", "HbA1c", "Triglycerides", "Vitamin D", "TSH"],
        interventions=DiseaseInterventions(
            high=["womens_health", "nutrition_refined_carb", "movement_programme"],
            medium=["stress_management", "sleep_health"],
        ),
        modifiability=0.7,
        medical_frame_id="pcos_pcod",
    ),

    "nafld": DiseaseKnowledge(
        id="nafld",
        display_name="NAFLD",
        cluster="metabolic",
        clinical_focus=(
            "Increasing fatty-liver risk suggests changes in metabolic health that may be associated with excess weight, "
            "poor glucose control, and abnormal lipid levels."
        ),
        workplace_relevance="Early metabolic health indicator that often progresses silently until more advanced stages.",
        lifestyle_drivers=["weight", "nutrition", "activity", "alcohol"],
        related_metrics=["obesity", "type_2_diabetes", "nutrition", "dyslipidemia"],
        biomarkers=["ALT", "AST", "GGT", "Fasting Blood Sugar", "HbA1c", "Triglycerides"],
        interventions=DiseaseInterventions(
            high=["liver_screening", "nutrition_refined_carb", "weight_management"],
            medium=["alcohol_moderation", "movement_programme"],
        ),
        modifiability=0.85,
        medical_frame_id="nafld",
    ),

    "cardiac_health": DiseaseKnowledge(
        id="cardiac_health",
        display_name="Cardiac Health",
        cluster="cardiovascular",
        clinical_focus=(
            "Rising cardiac-risk indicators suggest that multiple factors affecting heart health may be occurring "
            "together, increasing the likelihood of future cardiovascular complications."
        ),
        workplace_relevance="Key preventive health priority because cardiovascular disease remains a leading cause of serious illness and healthcare costs.",
        lifestyle_drivers=["nutrition", "activity", "stress", "sleep", "weight", "smoking"],
        related_metrics=["dyslipidemia", "hypertension", "type_2_diabetes", "oxidative_stress", "obesity"],
        biomarkers=["LDL", "HDL", "Triglycerides", "hs-CRP", "Fasting Blood Sugar", "HbA1c"],
        interventions=DiseaseInterventions(
            high=["cardiac_screening", "nutrition_heart_healthy", "smoking_cessation"],
            medium=["movement_programme", "stress_management", "sleep_health"],
        ),
        modifiability=0.75,
        medical_frame_id="cardiac_health",
    ),

    "thyroid_health": DiseaseKnowledge(
        id="thyroid_health",
        display_name="Thyroid Health",
        cluster="hormonal",
        clinical_focus=(
            "Higher thyroid-related risk may indicate changes in thyroid function that can influence energy levels, "
            "metabolism, weight regulation, and overall wellbeing."
        ),
        workplace_relevance="Common endocrine condition that can contribute to fatigue, reduced concentration, and lower daily performance if left unmanaged.",
        lifestyle_drivers=["stress"],
        related_metrics=["dyslipidemia", "sleep"],
        biomarkers=["TSH", "Free T4", "Free T3", "LDL", "Vitamin D", "Vitamin B12"],
        interventions=DiseaseInterventions(
            high=["thyroid_screening", "clinical_review"],
            medium=["womens_health", "stress_management"],
        ),
        modifiability=0.45,
        medical_frame_id="thyroid_health",
    ),

    "dyslipidemia": DiseaseKnowledge(
        id="dyslipidemia",
        display_name="Dyslipidemia",
        cluster="cardiovascular",
        clinical_focus=(
            "Increasing dyslipidemia risk indicates less favourable cholesterol and triglyceride patterns, which can "
            "contribute to the gradual development of cardiovascular disease."
        ),
        workplace_relevance="Well-established cardiovascular risk factor that responds effectively to early screening and lifestyle improvement.",
        lifestyle_drivers=["nutrition", "activity", "weight", "alcohol", "smoking"],
        related_metrics=["cardiac_health", "nutrition", "obesity", "physical_activity", "type_2_diabetes"],
        biomarkers=["LDL", "HDL", "Triglycerides", "Total Cholesterol", "Non-HDL Cholesterol"],
        interventions=DiseaseInterventions(
            high=["lipid_screening", "nutrition_heart_healthy", "movement_programme"],
            medium=["weight_management", "alcohol_moderation", "smoking_cessation"],
        ),
        modifiability=0.85,
        medical_frame_id="dyslipidemia",
    ),

    "metabolic_syndrome": DiseaseKnowledge(
        id="metabolic_syndrome",
        display_name="Metabolic Syndrome",
        cluster="metabolic",
        clinical_focus=(
            "Higher metabolic-syndrome risk suggests that weight, blood sugar, blood pressure, and lipid-related "
            "abnormalities are occurring together, increasing the likelihood of future chronic disease."
        ),
        workplace_relevance="Comprehensive indicator of metabolic health that reflects the combined burden of multiple lifestyle-related risk factors.",
        lifestyle_drivers=["weight", "activity", "nutrition", "stress", "sleep"],
        related_metrics=[
            "obesity",
            "type_2_diabetes",
            "dyslipidemia",
            "hypertension",
            "nafld",
            "physical_activity",
            "nutrition",
        ],
        biomarkers=["Fasting Blood Sugar", "HbA1c", "Triglycerides", "HDL", "hs-CRP"],
        interventions=DiseaseInterventions(
            high=["metabolic_screening", "nutrition_refined_carb", "movement_programme"],
            medium=["weight_management", "sleep_health", "stress_management"],
        ),
        modifiability=0.9,
        medical_frame_id="metabolic_syndrome",
    ),
}

def get_disease_knowledge(id: str) -> Optional[DiseaseKnowledge]:
    return DISEASE_KNOWLEDGE.get(id)


def is_disease_metric(id: MetricId) -> bool:
    return id in DISEASE_KNOWLEDGE


def disease_metric_from_name(name: str) -> Optional[MetricId]:
    """Map display names from dashboard API to metric ids."""
    normalized = name.strip().lower()
    for disease in DISEASE_KNOWLEDGE.values():
        if disease.display_name.lower() == normalized:
            return disease.id
    aliases: Dict[str, MetricId] = {
        "type 2 diabetes": "type_2_diabetes",
        "diabetes": "type_2_diabetes",
        "pcos/pcod": "pcos_pcod",
        "pcos": "pcos_pcod",
        "pcod": "pcos_pcod",
        "fatty liver": "nafld",
        "cardiac health": "cardiac_health",
        "thyroid health": "thyroid_health",
        "metabolic syndrome": "metabolic_syndrome",
    }
    return aliases.get(normalized)


"""Faithful port of src/intelligence/knowledge/lifestyle.ts."""

LIFESTYLE_KNOWLEDGE: Dict[str, LifestyleKnowledge] = {
    "physical_activity": LifestyleKnowledge(
        id="physical_activity",
        display_name="Physical Activity",
        cluster="lifestyle",
        clinical_focus=(
            "Low physical activity increases future metabolic, cardiovascular, and obesity risk across the "
            "workforce."
        ),
        poor_labels=["Less than 30mins", "Rarely or Never"],
        healthy_labels=["More than 60 mins", "30-60mins"],
        high_severity_labels=["Rarely or Never"],
        interventions=DiseaseInterventions(high=["movement_programme"], medium=["weight_management"]),
        modifiability=0.95,
        medical_frame_id="physical_activity",
    ),
    "sleep": LifestyleKnowledge(
        id="sleep",
        display_name="Sleep",
        cluster="recovery",
        clinical_focus="Insufficient or irregular sleep impairs recovery, cognitive performance, and metabolic regulation.",
        poor_labels=["Less than 5", "5-7", "More than 9"],
        healthy_labels=["7-9"],
        high_severity_labels=["Less than 5"],
        interventions=DiseaseInterventions(high=["sleep_health"], medium=["stress_management", "recovery_programme"]),
        modifiability=0.8,
        medical_frame_id="sleep",
    ),
    "nutrition": LifestyleKnowledge(
        id="nutrition",
        display_name="Nutrition",
        cluster="lifestyle",
        clinical_focus=(
            "Nutrition patterns are a primary modifiable lever for lipids, glucose regulation, and cardiovascular "
            "risk."
        ),
        poor_labels=[],
        healthy_labels=[],
        interventions=DiseaseInterventions(
            high=["nutrition_heart_healthy", "nutrition_whole_food"],
            medium=["nutrition_refined_carb", "nutrition_sodium"],
        ),
        modifiability=0.9,
        medical_frame_id="nutrition",
    ),
    "oxidative_stress": LifestyleKnowledge(
        id="oxidative_stress",
        display_name="Oxidative Stress",
        cluster="recovery",
        clinical_focus=(
            "Elevated oxidative stress indicates cellular strain linked to poor recovery, fatigue, and chronic "
            "disease risk."
        ),
        poor_labels=["High", "Very High", "high", "veryHigh"],
        healthy_labels=["Low", "low"],
        high_severity_labels=["Very High", "veryHigh"],
        interventions=DiseaseInterventions(
            high=["recovery_programme", "nutrition_whole_food"],
            medium=["sleep_health", "movement_programme", "stress_management"],
        ),
        modifiability=0.75,
        medical_frame_id="oxidative_stress",
    ),
    "overall_risk": LifestyleKnowledge(
        id="overall_risk",
        display_name="Overall Risk",
        cluster="metabolic",
        clinical_focus="Overall risk distribution summarises workforce metabolic and lifestyle vulnerability.",
        poor_labels=["Increased Risk", "High risk"],
        healthy_labels=["Optimal", "Low risk"],
        high_severity_labels=["High risk"],
        interventions=DiseaseInterventions(
            high=["target_high_risk", "scale_preventive_care"], medium=["maintain_wellness"]
        ),
        modifiability=0.7,
        medical_frame_id="overall_risk",
    ),
    "metabolic_age": LifestyleKnowledge(
        id="metabolic_age",
        display_name="Metabolic Age",
        cluster="metabolic",
        clinical_focus="Elevated metabolic age gaps indicate accelerated biological ageing relative to chronological age.",
        poor_labels=[],
        healthy_labels=[],
        interventions=DiseaseInterventions(
            high=["movement_programme", "nutrition_whole_food"], medium=["sleep_health", "weight_management"]
        ),
        modifiability=0.8,
        medical_frame_id="metabolic_age",
    ),
    "bmi_waist": LifestyleKnowledge(
        id="bmi_waist",
        display_name="BMI & Waist",
        cluster="metabolic",
        clinical_focus="Central adiposity and elevated BMI are upstream drivers of metabolic and cardiac disease.",
        poor_labels=[],
        healthy_labels=[],
        interventions=DiseaseInterventions(
            high=["weight_management", "movement_programme"], medium=["nutrition_whole_food"]
        ),
        modifiability=0.9,
        medical_frame_id="bmi_waist",
    ),
}


def get_lifestyle_knowledge(id: str) -> Optional[LifestyleKnowledge]:
    return LIFESTYLE_KNOWLEDGE.get(id)


def get_modifiability(metric_id: MetricId) -> float:
    knowledge = LIFESTYLE_KNOWLEDGE.get(metric_id)
    if knowledge is not None:
        return knowledge.modifiability
    # disease lookup deferred to knowledge index to avoid circular imports in consumers
    return 0.7


"""Faithful port of src/intelligence/knowledge/interventions.ts."""

INTERVENTIONS: Dict[InterventionId, InterventionKnowledge] = {
    "metabolic_screening": InterventionKnowledge(
        id="metabolic_screening",
        phrase="Include annual metabolic screening (HbA1c and fasting blood glucose) for at-risk employees",
        category="screening",
    ),
    "lipid_screening": InterventionKnowledge(
        id="lipid_screening",
        phrase="Incorporate lipid profile screening into routine preventive health assessments",
        category="screening",
    ),
    "bp_screening": InterventionKnowledge(
        id="bp_screening",
        phrase="Conduct regular blood pressure screening to support early detection",
        category="screening",
    ),
    "thyroid_screening": InterventionKnowledge(
        id="thyroid_screening",
        phrase="Include thyroid function testing (TSH) as part of routine health evaluations",
        category="screening",
    ),
    "liver_screening": InterventionKnowledge(
        id="liver_screening",
        phrase="Include liver function assessment during routine metabolic health screening",
        category="screening",
    ),
    "cardiac_screening": InterventionKnowledge(
        id="cardiac_screening",
        phrase="Perform comprehensive cardiovascular risk assessment for high-risk groups",
        category="screening",
    ),
    "nutrition_refined_carb": InterventionKnowledge(
        id="nutrition_refined_carb",
        phrase="Encourage dietary patterns that reduce refined carbohydrate intake",
        category="nutrition",
    ),
    "nutrition_heart_healthy": InterventionKnowledge(
        id="nutrition_heart_healthy",
        phrase="Promote heart-healthy eating habits rich in whole grains, fruits, and vegetables",
        category="nutrition",
    ),
    "nutrition_sodium": InterventionKnowledge(
        id="nutrition_sodium",
        phrase="Promote lower sodium intake through nutrition education and healthier food choices",
        category="nutrition",
    ),
    "nutrition_whole_food": InterventionKnowledge(
        id="nutrition_whole_food",
        phrase="Encourage balanced meals based on whole foods and appropriate portion sizes",
        category="nutrition",
    ),
    "movement_programme": InterventionKnowledge(
        id="movement_programme",
        phrase="Promote regular physical activity through daily movement and active breaks",
        category="activity",
    ),
    "weight_management": InterventionKnowledge(
        id="weight_management",
        phrase="Support healthy weight management through nutrition, activity, and lifestyle coaching",
        category="weight",
    ),
    "sleep_health": InterventionKnowledge(
        id="sleep_health",
        phrase="Promote healthy sleep habits to improve recovery, wellbeing, and overall health",
        category="sleep",
    ),
    "stress_management": InterventionKnowledge(
        id="stress_management",
        phrase="Provide stress management resources and encourage healthy work-life balance",
        category="stress",
    ),
    "recovery_programme": InterventionKnowledge(
        id="recovery_programme",
        phrase="Support recovery through programmes focused on sleep, stress reduction, and healthy daily habits",
        category="recovery",
    ),
    "womens_health": InterventionKnowledge(
        id="womens_health",
        phrase="Strengthen women's health awareness through education, screening, and confidential support services",
        category="womens_health",
    ),
    "smoking_cessation": InterventionKnowledge(
        id="smoking_cessation",
        phrase="Provide evidence-based smoking cessation support and counselling",
        category="lifestyle",
    ),
    "alcohol_moderation": InterventionKnowledge(
        id="alcohol_moderation",
        phrase="Promote responsible alcohol consumption through education and counselling",
        category="lifestyle",
    ),
    "maintain_wellness": InterventionKnowledge(
        id="maintain_wellness",
        phrase="Continue preventive wellness initiatives to maintain current health outcomes",
        category="maintain",
    ),
    "target_high_risk": InterventionKnowledge(
        id="target_high_risk",
        phrase="Provide targeted health coaching and regular follow-up for employees at higher health risk",
        category="strategy",
    ),
    "scale_preventive_care": InterventionKnowledge(
        id="scale_preventive_care",
        phrase="Expand preventive health programmes through integrated clinical and lifestyle support",
        category="strategy",
    ),
    "clinical_review": InterventionKnowledge(
        id="clinical_review",
        phrase="Recommend clinical evaluation for employees with persistent or significant health concerns",
        category="clinical",
    ),
}


def intervention_phrase(id: InterventionId) -> str:
    knowledge = INTERVENTIONS.get(id)
    return knowledge.phrase if knowledge else "Prioritize targeted preventive interventions"


"""Concise medical frames — one line of clinical meaning per id.

Faithful port of src/intelligence/knowledge/medicalFrames.ts.
"""

MEDICAL_FRAMES: Dict[str, str] = {
    "overall_risk": "Overall risk distribution reflects the workforce's current metabolic health and future lifestyle-related disease risk.",
    "overall_risk_healthy": "A predominantly healthy risk profile reflects good overall health and effective preventive practices.",

    "type_2_diabetes": (
        "Rising diabetes risk suggests worsening blood sugar control and a greater likelihood of long-term metabolic complications."
    ),

    "hypertension": (
        "Increasing blood-pressure risk suggests a higher likelihood of cardiovascular disease, stroke, and related health complications."
    ),

    "obesity": (
        "Higher obesity levels indicate increasing weight-related health risk and a greater chance of developing chronic metabolic conditions."
    ),

    "pcos_pcod": (
        "Higher PCOS indicators highlight the need for greater attention to hormonal health and metabolic wellbeing among female employees."
    ),

    "nafld": (
        "Increasing fatty-liver risk may reflect early metabolic changes associated with excess weight and poor glucose regulation."
    ),

    "cardiac_health": (
        "Rising cardiac-risk indicators suggest multiple cardiovascular risk factors may be developing together across the workforce."
    ),

    "thyroid_health": (
        "Higher thyroid-related risk may contribute to fatigue, reduced energy, and changes in metabolic function."
    ),

    "dyslipidemia": (
        "Increasing dyslipidemia reflects unhealthy cholesterol patterns that can raise long-term cardiovascular risk."
    ),

    "metabolic_syndrome": (
        "Higher metabolic-syndrome prevalence suggests several metabolic risk factors are occurring together, increasing future chronic disease risk."
    ),

    "physical_activity": (
        "Low physical activity is associated with poorer metabolic health and a higher risk of obesity, diabetes, and cardiovascular disease."
    ),

    "physical_activity_healthy": (
        "Regular physical activity supports healthy metabolism, cardiovascular fitness, and overall wellbeing."
    ),

    "sleep": (
        "Poor or irregular sleep can affect recovery, cognitive performance, hormone balance, and metabolic health."
    ),

    "sleep_healthy": (
        "Healthy sleep habits support physical recovery, mental performance, and long-term wellbeing."
    ),

    "nutrition": (
        "Healthy nutrition plays an important role in maintaining blood sugar, cholesterol levels, and cardiovascular health."
    ),

    "nutrition_healthy": (
        "Balanced eating habits help maintain healthy metabolism and reduce cardiovascular risk."
    ),

    "oxidative_stress": (
        "Higher oxidative stress reflects increased cellular damage that may contribute to fatigue, inflammation, and chronic disease."
    ),

    "oxidative_stress_healthy": (
        "Lower oxidative stress supports healthy cellular function, recovery, and overall wellbeing."
    ),

    "recovery_strain": (
        "Poor sleep together with elevated oxidative stress suggests reduced recovery and increased physical strain."
    ),

    "cardio_nutrition": (
        "Dietary patterns contributing to unhealthy lipid levels may increase overall cardiovascular risk."
    ),

    "movement_priority": (
        "Low physical activity together with excess weight highlights the importance of increasing daily movement."
    ),

    "metabolic_cluster": (
        "Several metabolic risk factors are present together, suggesting a broader pattern rather than isolated health concerns."
    ),

    "cardio_cluster": (
        "Blood pressure, lipid abnormalities, and cardiac indicators together suggest an increased cardiovascular health priority."
    ),

    "workforce_resilience": (
        "Healthy physical activity and sleep habits provide a strong foundation for long-term health and recovery."
    ),

    "positive_wins": (
        "Healthy risk profiles across several areas highlight strengths that should be maintained through ongoing preventive care."
    ),

    "metabolic_age": (
        "A higher metabolic age than chronological age may indicate declining metabolic health and increased future disease risk."
    ),

    "bmi_waist": (
        "Higher BMI and waist circumference are associated with increased metabolic and cardiovascular health risk."
    ),

    "participation": (
        "Participation levels reflect the reach and engagement of the workforce health assessment."
    ),

    "maintain": (
        "Current health patterns support continuing existing preventive initiatives while maintaining regular monitoring."
    ),

    "disease_generic": (
        "Higher disease risk highlights the importance of preventive screening, healthy lifestyle practices, and appropriate follow-up."
    ),
}

def medical_frame(id: str) -> str:
    return MEDICAL_FRAMES.get(id, MEDICAL_FRAMES["disease_generic"])


def frame_id_for_metric(metric_id: MetricId, healthy: bool) -> str:
    if healthy:
        healthy_key = f"{metric_id}_healthy"
        if healthy_key in MEDICAL_FRAMES:
            return healthy_key
    return metric_id if metric_id in MEDICAL_FRAMES else "disease_generic"


"""
Health Relationship Graph — static edge definitions.
Runtime evaluates one-hop activations against MetricScores.
Extracted and compressed from the Medical Knowledge Layer cross-links.

Faithful port of src/intelligence/knowledge/graphEdges.ts — ALL 26 edges.
"""

GRAPH_EDGES: List[HealthGraphEdge] = [
    # Recovery
    HealthGraphEdge(
        id="sleep_oxidative_recovery",
        from_="sleep",
        to="oxidative_stress",
        type="reinforces",
        weight=0.9,
        min_confidence="high",
        primary_lever="recovery_programme",
        effect_id="recovery_strain",
    ),
    HealthGraphEdge(
        id="oxidative_sleep_recovery",
        from_="oxidative_stress",
        to="sleep",
        type="reinforces",
        weight=0.85,
        min_confidence="high",
        primary_lever="recovery_programme",
        effect_id="recovery_strain",
    ),
    # Movement / obesity
    HealthGraphEdge(
        id="activity_obesity_movement",
        from_="physical_activity",
        to="obesity",
        type="drives",
        weight=0.95,
        min_confidence="high",
        primary_lever="movement_programme",
        effect_id="movement_priority",
    ),
    HealthGraphEdge(
        id="obesity_activity_movement",
        from_="obesity",
        to="physical_activity",
        type="drives",
        weight=0.9,
        min_confidence="high",
        primary_lever="movement_programme",
        effect_id="movement_priority",
    ),
    HealthGraphEdge(
        id="activity_diabetes",
        from_="physical_activity",
        to="type_2_diabetes",
        type="drives",
        weight=0.85,
        min_confidence="high",
        primary_lever="movement_programme",
        effect_id="movement_priority",
    ),
    # Cardio-nutrition
    HealthGraphEdge(
        id="nutrition_dyslipidemia",
        from_="nutrition",
        to="dyslipidemia",
        type="drives",
        weight=0.95,
        min_confidence="high",
        primary_lever="nutrition_heart_healthy",
        effect_id="cardio_nutrition",
    ),
    HealthGraphEdge(
        id="dyslipidemia_nutrition",
        from_="dyslipidemia",
        to="nutrition",
        type="reinforces",
        weight=0.9,
        min_confidence="high",
        primary_lever="nutrition_heart_healthy",
        effect_id="cardio_nutrition",
    ),
    HealthGraphEdge(
        id="nutrition_diabetes",
        from_="nutrition",
        to="type_2_diabetes",
        type="drives",
        weight=0.85,
        min_confidence="high",
        primary_lever="nutrition_refined_carb",
        effect_id="cardio_nutrition",
    ),
    # Metabolic cluster
    HealthGraphEdge(
        id="obesity_diabetes",
        from_="obesity",
        to="type_2_diabetes",
        type="drives",
        weight=0.95,
        min_confidence="high",
        primary_lever="weight_management",
        effect_id="metabolic_cluster",
    ),
    HealthGraphEdge(
        id="obesity_nafld",
        from_="obesity",
        to="nafld",
        type="drives",
        weight=0.9,
        min_confidence="high",
        primary_lever="weight_management",
        effect_id="metabolic_cluster",
    ),
    HealthGraphEdge(
        id="diabetes_dyslipidemia",
        from_="type_2_diabetes",
        to="dyslipidemia",
        type="clusters_with",
        weight=0.85,
        min_confidence="high",
        primary_lever="metabolic_screening",
        effect_id="metabolic_cluster",
    ),
    HealthGraphEdge(
        id="mets_obesity",
        from_="metabolic_syndrome",
        to="obesity",
        type="clusters_with",
        weight=0.95,
        min_confidence="high",
        primary_lever="movement_programme",
        effect_id="metabolic_cluster",
    ),
    HealthGraphEdge(
        id="mets_diabetes",
        from_="metabolic_syndrome",
        to="type_2_diabetes",
        type="clusters_with",
        weight=0.95,
        min_confidence="high",
        primary_lever="metabolic_screening",
        effect_id="metabolic_cluster",
    ),
    HealthGraphEdge(
        id="nafld_diabetes",
        from_="nafld",
        to="type_2_diabetes",
        type="reinforces",
        weight=0.8,
        min_confidence="high",
        primary_lever="nutrition_refined_carb",
        effect_id="metabolic_cluster",
    ),
    # Cardio cluster
    HealthGraphEdge(
        id="dyslipidemia_cardiac",
        from_="dyslipidemia",
        to="cardiac_health",
        type="drives",
        weight=0.95,
        min_confidence="high",
        primary_lever="lipid_screening",
        effect_id="cardio_cluster",
    ),
    HealthGraphEdge(
        id="hypertension_cardiac",
        from_="hypertension",
        to="cardiac_health",
        type="drives",
        weight=0.95,
        min_confidence="high",
        primary_lever="bp_screening",
        effect_id="cardio_cluster",
    ),
    HealthGraphEdge(
        id="hypertension_dyslipidemia",
        from_="hypertension",
        to="dyslipidemia",
        type="clusters_with",
        weight=0.85,
        min_confidence="high",
        primary_lever="cardiac_screening",
        effect_id="cardio_cluster",
    ),
    HealthGraphEdge(
        id="diabetes_cardiac",
        from_="type_2_diabetes",
        to="cardiac_health",
        type="drives",
        weight=0.9,
        min_confidence="high",
        primary_lever="cardiac_screening",
        effect_id="cardio_cluster",
    ),
    HealthGraphEdge(
        id="mets_hypertension",
        from_="metabolic_syndrome",
        to="hypertension",
        type="clusters_with",
        weight=0.85,
        min_confidence="high",
        primary_lever="scale_preventive_care",
        effect_id="cardio_cluster",
    ),
    # Sleep / hypertension
    HealthGraphEdge(
        id="sleep_hypertension",
        from_="sleep",
        to="hypertension",
        type="reinforces",
        weight=0.7,
        min_confidence="moderate",
        primary_lever="sleep_health",
        effect_id="recovery_strain",
    ),
    # Protective
    HealthGraphEdge(
        id="activity_sleep_resilience",
        from_="physical_activity",
        to="sleep",
        type="protects",
        weight=0.8,
        min_confidence="moderate",
        primary_lever="maintain_wellness",
        effect_id="workforce_resilience",
    ),
    HealthGraphEdge(
        id="sleep_activity_resilience",
        from_="sleep",
        to="physical_activity",
        type="protects",
        weight=0.8,
        min_confidence="moderate",
        primary_lever="maintain_wellness",
        effect_id="workforce_resilience",
    ),
    # Oxidative / cardiac
    HealthGraphEdge(
        id="oxidative_cardiac",
        from_="oxidative_stress",
        to="cardiac_health",
        type="indicates",
        weight=0.65,
        min_confidence="moderate",
        primary_lever="recovery_programme",
        effect_id="recovery_strain",
    ),
    # PCOS metabolic
    HealthGraphEdge(
        id="pcos_diabetes",
        from_="pcos_pcod",
        to="type_2_diabetes",
        type="drives",
        weight=0.8,
        min_confidence="high",
        primary_lever="nutrition_refined_carb",
        effect_id="metabolic_cluster",
    ),
    HealthGraphEdge(
        id="pcos_obesity",
        from_="pcos_pcod",
        to="obesity",
        type="reinforces",
        weight=0.75,
        min_confidence="high",
        primary_lever="womens_health",
        effect_id="metabolic_cluster",
    ),
]


"""Runtime guardrails — organisational wellness insights only.

Faithful port of src/intelligence/knowledge/guardrails.ts.
"""

GUARDRAILS: Dict[str, bool] = {
    "noPrescriptionMedications": True,
    "noIndividualDiagnosis": True,
    "noTreatmentPlans": True,
    "clinicalReviewIsGenericOnly": True,
}

DISCLAIMER = "Insights are organisational wellness signals, not individual medical diagnoses or treatment advice."


def get_modifiability(metric_id: MetricId) -> float:
    lifestyle = get_lifestyle_knowledge(metric_id)
    if lifestyle is not None:
        return lifestyle.modifiability
    disease = get_disease_knowledge(metric_id)
    if disease is not None:
        return disease.modifiability
    return 0.7


_DISPLAY_NAME_FALLBACKS: Dict[str, str] = {
    "overall_risk": "Overall risk",
    "physical_activity": "Physical activity",
    "sleep": "Sleep",
    "nutrition": "Nutrition",
    "oxidative_stress": "Oxidative stress",
    "metabolic_age": "Metabolic age",
    "bmi_waist": "BMI and waist",
    "positive_wins": "Positive health wins",
    "participation": "Participation",
}


def get_display_name(metric_id: MetricId) -> str:
    disease = get_disease_knowledge(metric_id)
    if disease is not None:
        return disease.display_name
    lifestyle = get_lifestyle_knowledge(metric_id)
    if lifestyle is not None:
        return lifestyle.display_name
    # Never return snake_case ids to callers
    return _DISPLAY_NAME_FALLBACKS.get(metric_id, "This health indicator")


class _LeverBundle(dict):
    """Dict that also supports attribute access (``.high``/``.medium``), so
    callers can use either ``get_default_levers(id)["high"]`` or
    ``get_default_levers(id).high`` — mirrors TS's plain ``{ high, medium }``
    object, which supports both styles from calling code perspectives."""

    def __getattr__(self, item: str):
        try:
            return self[item]
        except KeyError as exc:  # pragma: no cover - defensive
            raise AttributeError(item) from exc


def get_default_levers(metric_id: MetricId) -> Dict[str, list]:
    disease = get_disease_knowledge(metric_id)
    if disease is not None:
        return _LeverBundle(high=disease.interventions.high, medium=disease.interventions.medium)
    lifestyle = get_lifestyle_knowledge(metric_id)
    if lifestyle is not None:
        return _LeverBundle(high=lifestyle.interventions.high, medium=lifestyle.interventions.medium)
    return _LeverBundle(high=["target_high_risk"], medium=["maintain_wellness"])


# ===========================================================================
# Narrative phrase libraries (generation assets) — from medical_knowledge_layer.md
# Added for the narrative content-quality upgrade. Reasoning/scoring untouched.
# ===========================================================================

"""Curated narrative phrase libraries — ported verbatim from
medical_knowledge_layer.md (sections 10-16). These are *generation assets*:
the reasoning engine selects from them deterministically; it never invents new
clinical facts. Every phrase here already exists, and is owned by, the Medical
Knowledge Layer. Interpretation clauses are stored clause-shaped (no leading
ellipsis, no trailing period) so they join after an observation with a comma,
exactly like SHORT_WHY."""


DISEASE_INTERPRETATION_PHRASES: Dict[str, List[str]] = {
"type_2_diabetes": [
        "showing early signs of declining metabolic health across the workforce",
        "suggesting reduced insulin sensitivity in a growing number of employees",
        "highlighting an increased likelihood of future cardiovascular complications",
        "reflecting lifestyle patterns that may gradually impair blood sugar control",
        "reinforcing the value of early preventive measures before disease develops",
        "suggesting that untreated metabolic risk may increase future healthcare needs",
        "highlighting the importance of improving overall metabolic health",
        "consistent with an early stage where lifestyle changes can still make a significant impact",
        "emphasising the close relationship between physical activity and glucose regulation",
        "indicating that healthy weight management and balanced nutrition remain the most effective preventive strategies",
],
"hypertension": [
    "showing a gradual increase in cardiovascular and stroke risk",
    "suggesting persistent blood pressure changes across the workforce",
    "highlighting the close relationship between stress levels and blood pressure",
    "reflecting lifestyle patterns that may contribute to long-term cardiovascular disease",
    "reinforcing the importance of regular blood pressure screening",
    "suggesting that prolonged work-related stress may influence vascular health",
    "highlighting the need to strengthen cardiovascular health initiatives",
    "consistent with an early stage where timely intervention can improve outcomes",
    "emphasising the importance of maintaining healthy weight, activity, and sodium intake",
    "suggesting that uncontrolled blood pressure may gradually affect kidney function",
],

"obesity": [
    "showing increasing weight-related health concerns across the workforce",
    "suggesting a gradual rise in metabolic risk across multiple health areas",
    "highlighting the relationship between physical activity and healthy body composition",
    "reflecting lifestyle habits that may contribute to long-term health complications",
    "reinforcing the importance of healthy weight management as a preventive priority",
    "suggesting a greater likelihood of future diabetes and cardiovascular disease",
    "highlighting body weight as an important indicator of overall metabolic health",
    "consistent with a condition that responds well to sustained lifestyle improvements",
    "emphasising nutrition and regular physical activity as key preventive measures",
    "suggesting excess body weight may also influence liver, joint, and overall metabolic health",
],

"pcos_pcod": [
    "showing the close relationship between hormonal balance and metabolic health",
    "suggesting insulin resistance may contribute to hormonal changes in affected women",
    "highlighting the increased long-term risk of diabetes and cardiovascular disease",
    "reflecting a common condition that often benefits from early lifestyle management",
    "reinforcing the importance of dedicated women's health awareness and support",
    "suggesting nutrition and regular physical activity play an important role in management",
    "highlighting the need for greater focus on women's metabolic wellbeing",
    "consistent with a condition that often improves through sustained lifestyle changes",
    "emphasising the value of confidential and accessible women's health services",
    "suggesting that untreated metabolic changes may increase future health risks",
],

"nafld": [
    "showing early metabolic changes that may affect liver health",
    "suggesting insulin resistance may be contributing to fatty liver changes",
    "highlighting the increased likelihood of future diabetes and cardiovascular disease",
    "reflecting a stage of liver disease where lifestyle changes can still be highly effective",
    "reinforcing the importance of early identification and intervention",
    "suggesting excess body weight and poor dietary habits are important contributing factors",
    "highlighting liver health as an important part of overall metabolic wellbeing",
    "consistent with a condition that often improves through healthy lifestyle modifications",
    "emphasising balanced nutrition as a key component of liver health",
    "suggesting that untreated fatty liver disease may gradually progress over time",
],

"cardiac_health": [
    "showing increasing cardiovascular risk across multiple health indicators",
    "suggesting several contributing factors may be increasing the likelihood of future cardiac events",
    "highlighting the long-term risk of heart attack and stroke",
    "reflecting the combined influence of cholesterol, blood pressure, weight, and lifestyle",
    "reinforcing the importance of comprehensive cardiovascular prevention",
    "suggesting future healthcare needs may increase if cardiovascular risk remains unmanaged",
    "highlighting heart health as a major focus for preventive care",
    "consistent with a condition that responds well to early lifestyle improvement",
    "emphasising nutrition, physical activity, and routine screening as key preventive measures",
    "suggesting that addressing multiple cardiovascular risk factors together provides greater long-term benefit",
],

"thyroid_health": [
    "showing changes that may contribute to fatigue and reduced energy levels",
    "suggesting thyroid dysfunction may be affecting overall wellbeing",
    "highlighting a condition that can be identified through routine screening",
    "reflecting a common endocrine disorder that may influence metabolism",
    "reinforcing the importance of early thyroid function assessment",
    "suggesting women may experience a higher burden of thyroid-related conditions",
    "highlighting the value of including thyroid health in routine wellness programmes",
    "consistent with a medical condition that often responds well to appropriate treatment",
    "emphasising routine thyroid screening for employees with persistent symptoms",
    "suggesting thyroid dysfunction may also influence cholesterol levels and metabolic health",
],

"dyslipidemia": [
    "showing increasing cholesterol-related cardiovascular risk",
    "suggesting unhealthy lipid levels may be becoming more common across the workforce",
    "highlighting the long-term risk of heart attack and stroke",
    "reflecting dietary and lifestyle habits that influence cholesterol balance",
    "reinforcing the importance of routine lipid screening and prevention",
    "suggesting nutrition remains one of the most effective ways to improve lipid health",
    "highlighting cardiovascular health as an important preventive priority",
    "consistent with an early stage where lifestyle improvement can produce measurable benefits",
    "emphasising elevated triglycerides as an important metabolic health indicator",
    "suggesting that improving cholesterol levels can significantly reduce cardiovascular risk",
],

"metabolic_syndrome": [
    "showing several metabolic risk factors occurring together across the workforce",
    "suggesting multiple health risks are developing simultaneously rather than independently",
    "highlighting a substantially higher likelihood of future diabetes and cardiovascular disease",
    "reflecting the combined effects of body weight, blood sugar, blood pressure, and lipid imbalance",
    "reinforcing the importance of comprehensive lifestyle-based prevention",
    "suggesting overall metabolic health may be gradually declining",
    "highlighting metabolic health as a key priority for long-term disease prevention",
    "consistent with a condition that responds well to coordinated lifestyle improvements",
    "emphasising nutrition and physical activity as the foundation of risk reduction",
    "suggesting that managing all metabolic risk factors together leads to better long-term health outcomes",
],
}

DISEASE_POSITIVE_NARRATIVES: Dict[str, List[str]] = {
    "type_2_diabetes": [
        "Blood sugar regulation remains stable across most of the workforce.",
        "Metabolic health appears well maintained, with a low overall diabetes risk.",
        "Current findings suggest healthy glucose regulation among most employees.",
        "Blood sugar markers are generally within expected healthy ranges.",
        "Healthy lifestyle habits appear to be supporting metabolic wellbeing.",
        "The workforce shows a favourable metabolic health profile.",
        "Diabetes risk remains low across the assessed population.",
        "Early metabolic indicators reflect good preventive health practices.",
        "Balanced nutrition and regular activity appear to support healthy glucose control.",
        "Current metabolic health provides a strong foundation for long-term wellbeing.",
    ],

    "hypertension": [
        "Blood pressure remains well managed across most of the workforce.",
        "Cardiovascular health indicators are generally within healthy ranges.",
        "Current findings suggest a low overall risk of hypertension.",
        "Healthy lifestyle habits appear to support normal blood pressure levels.",
        "Hypertension is not a major health concern within this cohort.",
        "The workforce demonstrates good overall cardiovascular health.",
        "Blood pressure measurements are reassuring across the assessed population.",
        "Preventive health practices appear to support healthy vascular function.",
        "Heart health remains a positive aspect of the workforce profile.",
        "Blood pressure risk remains consistently low across the cohort.",
    ],

    "obesity": [
        "Body weight remains within healthy ranges for most employees.",
        "Healthy body composition reflects positive lifestyle habits.",
        "Current findings indicate a low overall burden of obesity.",
        "Weight-related health indicators are favourable across the workforce.",
        "Regular physical activity and balanced nutrition appear to support healthy body weight.",
        "The workforce demonstrates good overall physical health.",
        "Obesity is not a major concern within this cohort.",
        "Body composition measurements are reassuring across the assessed population.",
        "Healthy weight management is a positive feature of the workforce profile.",
        "Weight-related health risk remains consistently low.",
    ],

    "pcos_pcod": [
        "Hormonal and metabolic health appears stable among female employees.",
        "PCOS-related risk remains low within the assessed female population.",
        "Women's metabolic health indicators are generally reassuring.",
        "Insulin sensitivity appears favourable across most female employees.",
        "Healthy lifestyle patterns may be supporting hormonal wellbeing.",
        "Women's health remains a positive aspect of the workforce profile.",
        "PCOS-related concerns are limited within this cohort.",
        "Female metabolic markers are largely within expected healthy ranges.",
        "Current findings support good hormonal health among most women assessed.",
        "Overall metabolic wellbeing among female employees appears well maintained.",
    ],

    "nafld": [
        "Liver health appears healthy across most of the workforce.",
        "Fatty liver risk remains low based on current findings.",
        "Current metabolic patterns appear supportive of healthy liver function.",
        "Liver-related biomarkers are generally within expected ranges.",
        "Balanced nutrition and physical activity appear to support liver health.",
        "The workforce demonstrates good overall liver and metabolic health.",
        "Fatty liver disease is not a major concern within this cohort.",
        "Liver function markers are reassuring across the assessed population.",
        "Healthy metabolic habits appear to be protecting liver function.",
        "Current liver health provides a positive foundation for long-term wellbeing.",
    ],

    "cardiac_health": [
        "Heart health remains favourable across most of the workforce.",
        "Cardiovascular risk indicators are generally within healthy ranges.",
        "Current findings suggest a low overall risk of heart disease.",
        "Healthy lifestyle habits appear to support cardiovascular wellbeing.",
        "Cardiac health is not a major concern within this cohort.",
        "The workforce demonstrates good overall cardiovascular fitness.",
        "Heart-health markers are reassuring across the assessed population.",
        "Preventive health practices appear to support long-term cardiovascular health.",
        "Cardiovascular wellbeing remains a positive feature of the workforce profile.",
        "Heart disease risk remains consistently low across the cohort.",
    ],

    "thyroid_health": [
        "Thyroid function appears stable across most of the workforce.",
        "Current findings suggest a low overall risk of thyroid disorders.",
        "Thyroid-related biomarkers are generally within healthy ranges.",
        "Endocrine health appears well maintained across the assessed population.",
        "Thyroid-related concerns are limited within this cohort.",
        "Routine screening suggests healthy thyroid function in most employees.",
        "Energy-related symptoms linked to thyroid disease are likely to be uncommon.",
        "Thyroid health remains reassuring across the workforce.",
        "Overall endocrine health represents a positive aspect of the workforce profile.",
        "Current thyroid status supports healthy metabolic function.",
    ],

    "dyslipidemia": [
        "Cholesterol levels remain healthy across most of the workforce.",
        "Lipid markers are generally within recommended ranges.",
        "Current findings suggest a low overall risk of dyslipidemia.",
        "Healthy nutrition and activity patterns appear to support lipid balance.",
        "Abnormal cholesterol levels are not a major concern within this cohort.",
        "The workforce demonstrates good overall lipid health.",
        "Cholesterol and triglyceride measurements are reassuring.",
        "Preventive lifestyle habits appear to support healthy lipid levels.",
        "Lipid health remains a positive aspect of the workforce profile.",
        "Blood lipid risk remains consistently low across the assessed population.",
    ],

    "metabolic_syndrome": [
        "Overall metabolic health appears favourable across the workforce.",
        "Combined metabolic risk remains low based on current findings.",
        "The workforce demonstrates healthy patterns across multiple metabolic indicators.",
        "Blood sugar, blood pressure, and lipid markers are generally well controlled.",
        "Healthy nutrition and regular physical activity appear to support metabolic wellbeing.",
        "Metabolic syndrome is not a major concern within this cohort.",
        "Current metabolic markers are reassuring across the assessed population.",
        "Preventive health practices appear to be supporting long-term metabolic health.",
        "Overall metabolic wellbeing remains a positive feature of the workforce profile.",
        "Current findings suggest a strong foundation for maintaining long-term metabolic health.",
    ],
}

DISEASE_LEADERSHIP_TAKEAWAYS: Dict[str, List[str]] = {
    "type_2_diabetes": [
        "If resources are limited, prioritising healthier nutrition and regular physical activity will have the greatest impact on reducing future diabetes risk.",
        "Improving metabolic health today can reduce future healthcare costs and long-term disease burden.",
        "The early stages of diabetes provide an important opportunity where timely intervention can prevent disease progression.",
        "Routine metabolic screening combined with targeted lifestyle support offers long-term health and economic benefits.",
        "Addressing body weight, nutrition, and physical activity together is more effective than focusing on a single factor.",
    ],

    "hypertension": [
        "If resources are limited, reducing sodium intake, improving physical activity, and managing stress should be prioritised.",
        "Routine blood pressure screening allows early detection before complications develop.",
        "Workplace stress and unhealthy lifestyle habits can both contribute to rising blood pressure and should be addressed together.",
        "Healthy weight management and regular exercise remain two of the most effective strategies for reducing hypertension risk.",
        "Early preventive action helps reduce the future burden of cardiovascular and kidney disease.",
    ],

    "obesity": [
        "If resources are limited, investing in healthier eating habits and regular physical activity will provide the greatest long-term benefit.",
        "Reducing obesity can improve several related health conditions, including diabetes, heart disease, and fatty liver disease.",
        "Maintaining a healthy body weight benefits multiple aspects of metabolic and cardiovascular health.",
        "Sustainable improvements are more likely when healthy food choices and opportunities for physical activity are easily accessible.",
        "Consistent daily movement is generally more effective than short-term intensive programmes for maintaining long-term weight control.",
    ],

    "pcos_pcod": [
        "If resources are limited, nutrition programmes that improve insulin sensitivity can provide meaningful benefits for women with PCOS.",
        "Increasing awareness and access to women's health services can improve early identification and long-term management.",
        "Improving insulin resistance may also reduce the future risk of diabetes in affected women.",
        "Inclusive nutrition and physical activity programmes can support both hormonal and metabolic health.",
        "Confidential and accessible women's health support encourages earlier engagement and better health outcomes.",
    ],

    "nafld": [
        "If resources are limited, reducing excess sugar intake and supporting healthy weight loss should be prioritised.",
        "Early identification of fatty liver disease creates an opportunity to prevent future metabolic complications.",
        "Fatty liver disease often reflects broader metabolic health concerns that should be addressed comprehensively.",
        "Even modest and sustained weight loss can significantly improve liver health.",
        "Routine liver health screening helps identify individuals who may benefit from early intervention.",
    ],

    "cardiac_health": [
        "If resources are limited, improving nutrition, physical activity, and cholesterol management will provide the greatest reduction in cardiovascular risk.",
        "Cardiovascular health should remain a priority because it has a major impact on long-term health outcomes and healthcare costs.",
        "Managing body weight, blood pressure, cholesterol, and blood sugar together provides greater benefit than addressing each factor separately.",
        "Routine cardiovascular screening supports earlier identification of employees at increased risk.",
        "A comprehensive prevention strategy is more effective than focusing on individual cardiovascular risk factors in isolation.",
    ],

    "thyroid_health": [
        "If resources are limited, incorporating thyroid function testing into routine health assessments can improve early detection.",
        "Thyroid disorders are often identified through screening rather than lifestyle assessment alone.",
        "Recognising thyroid dysfunction early can help distinguish medical conditions from fatigue or work-related stress.",
        "Because thyroid disorders are more common in women, awareness programmes can complement broader women's health initiatives.",
        "Timely diagnosis and treatment can significantly improve energy levels, wellbeing, and daily functioning.",
    ],

    "dyslipidemia": [
        "If resources are limited, promoting heart-healthy nutrition should be a key strategy for improving lipid health.",
        "Managing cholesterol levels provides a measurable way to reduce long-term cardiovascular risk.",
        "Improvements in nutrition and physical activity can lead to meaningful changes in lipid profiles over time.",
        "Raised triglycerides should be addressed alongside cholesterol as part of overall cardiovascular risk reduction.",
        "Combining routine lipid screening with lifestyle support enables earlier intervention and better long-term outcomes.",
    ],

    "metabolic_syndrome": [
        "If resources are limited, integrated programmes that improve nutrition, physical activity, and healthy weight will benefit multiple metabolic risk factors simultaneously.",
        "Metabolic syndrome provides a comprehensive picture of overall metabolic health rather than a single disease.",
        "Addressing risk factors early helps prevent progression to diabetes, cardiovascular disease, and other chronic conditions.",
        "The same core lifestyle interventions improve blood sugar, blood pressure, cholesterol, and body weight together.",
        "A coordinated prevention strategy focused on metabolic health can deliver broad and sustainable health benefits across the workforce.",
    ],
}

DISEASE_SEVERITY_LANGUAGE: Dict[str, Dict[str, str]] = {
    "type_2_diabetes": {
        "very_low": "currently a low health concern",
        "low": "should continue to be monitored through routine metabolic screening",
        "moderate": "requires targeted lifestyle interventions to reduce future diabetes risk",
        "high": "should become a key focus of preventive health programmes",
        "very_high": "requires immediate action to reduce the growing burden of diabetes and its complications",
    },

    "hypertension": {
        "very_low": "currently a low cardiovascular concern",
        "low": "should be monitored through regular blood pressure assessments",
        "moderate": "requires targeted action to improve blood pressure control",
        "high": "should become a priority for cardiovascular risk reduction",
        "very_high": "requires immediate intervention to reduce the risk of cardiovascular and kidney complications",
    },

    "obesity": {
        "very_low": "currently a low weight-related health concern",
        "low": "should continue to be monitored through routine health assessments",
        "moderate": "requires focused support for healthy weight management",
        "high": "should become a major focus of workplace health initiatives",
        "very_high": "requires immediate action as it contributes to multiple chronic health conditions",
    },

    "pcos_pcod": {
        "very_low": "currently a low concern within the female workforce",
        "low": "should remain part of routine women's health awareness",
        "moderate": "requires targeted education and preventive support for affected women",
        "high": "should become an important women's health priority",
        "very_high": "requires dedicated clinical support and early intervention programmes",
    },

    "nafld": {
        "very_low": "currently a low liver health concern",
        "low": "should continue to be monitored during routine metabolic assessments",
        "moderate": "requires targeted interventions to improve liver and metabolic health",
        "high": "should become an important component of metabolic disease prevention",
        "very_high": "requires immediate action to prevent progression of liver and metabolic disease",
    },

    "cardiac_health": {
        "very_low": "currently a low cardiovascular concern",
        "low": "should continue to be monitored through preventive screening",
        "moderate": "requires focused action to reduce cardiovascular risk",
        "high": "should become a leading priority within workforce health programmes",
        "very_high": "requires immediate and comprehensive action to reduce the risk of serious cardiovascular events",
    },

    "thyroid_health": {
        "very_low": "currently a low endocrine health concern",
        "low": "should remain part of routine thyroid screening",
        "moderate": "requires increased awareness and timely thyroid function assessment",
        "high": "should prompt regular screening and appropriate clinical evaluation",
        "very_high": "requires immediate clinical assessment and follow-up for affected employees",
    },

    "dyslipidemia": {
        "very_low": "currently a low lipid-related health concern",
        "low": "should continue to be monitored through routine lipid assessments",
        "moderate": "requires targeted interventions to improve cholesterol and triglyceride levels",
        "high": "should become an important focus for cardiovascular disease prevention",
        "very_high": "requires immediate action to reduce long-term cardiovascular risk",
    },

    "metabolic_syndrome": {
        "very_low": "currently a low overall metabolic concern",
        "low": "should continue to be monitored through regular metabolic assessments",
        "moderate": "requires coordinated lifestyle interventions addressing multiple risk factors",
        "high": "should become a major priority for preventive health programmes",
        "very_high": "requires immediate, coordinated action to reduce the combined burden of metabolic disease",
    },
}

def select_variant(variants: Sequence[str], *key_parts: object) -> str:
    """Deterministically pick one phrase from `variants` using a stable hash of
    `key_parts`. Stable across processes/runs (uses hashlib, never the salted
    builtin hash()). Empty/one-element pools return safely. The same key always
    yields the same phrase — selection depends only on the caller-supplied key
    (metric, severity, section, profile token, role)."""
    pool = [v for v in variants if v and v.strip()]
    if not pool:
        return ""
    if len(pool) == 1:
        return pool[0]
    key = "|".join("" if p is None else str(p) for p in key_parts)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return pool[int(digest, 16) % len(pool)]

# Same organisational action, varied medically-authored phrasing. The action
# (and therefore the reasoning/lever) is identical across a lever's variants —
# only the surface verb/wording changes, so no recommendation ever shifts to a
# different intervention. Verbs drawn from the Medical Knowledge Layer register.
LEVER_ACTION_VARIANTS: Dict[str, List[str]] = {
    "metabolic_screening": [
        "Include annual metabolic screening for employees at increased risk",
        "Offer routine HbA1c and fasting blood glucose screening as part of preventive care",
        "Integrate annual metabolic assessments into regular health check-ups",
    ],

    "lipid_screening": [
        "Include routine lipid profile testing alongside heart-health initiatives",
        "Incorporate lipid screening into regular preventive health assessments",
        "Make lipid profile evaluation a routine component of cardiovascular prevention",
    ],

    "bp_screening": [
        "Encourage regular blood pressure screening with appropriate follow-up",
        "Include routine blood pressure assessment during preventive health checks",
        "Support early identification through regular blood pressure monitoring",
    ],

    "thyroid_screening": [
        "Include thyroid function testing in routine preventive health assessments",
        "Offer thyroid screening for employees with symptoms or elevated risk",
        "Incorporate TSH testing into standard health check programmes",
    ],

    "liver_screening": [
        "Include liver function tests during routine metabolic health assessments",
        "Monitor liver health alongside metabolic screening programmes",
        "Incorporate liver function evaluation into preventive health check-ups",
    ],

    "cardiac_screening": [
        "Provide comprehensive cardiovascular screening for higher-risk employees",
        "Include cardiovascular risk assessment in routine preventive health programmes",
        "Strengthen preventive care through regular heart health screening",
    ],

    "nutrition_refined_carb": [
        "Encourage reduced intake of refined carbohydrates through nutrition education",
        "Promote healthier food choices by limiting refined carbohydrate consumption",
        "Support balanced eating habits that reduce refined carbohydrate intake",
    ],

    "nutrition_heart_healthy": [
        "Promote heart-healthy eating habits across the workforce",
        "Encourage balanced dietary patterns that support cardiovascular health",
        "Increase access to nutrition programmes focused on heart health",
    ],

    "nutrition_sodium": [
        "Promote lower sodium intake through workplace nutrition education",
        "Encourage healthier food choices that reduce excess salt consumption",
        "Support sodium reduction through awareness campaigns and healthy catering options",
    ],

    "nutrition_whole_food": [
        "Encourage balanced meals based on whole foods and appropriate portion sizes",
        "Promote eating patterns centred on minimally processed foods",
        "Support healthier food choices through whole-food nutrition programmes",
    ],

    "movement_programme": [
        "Encourage regular physical activity through structured movement programmes",
        "Promote active work routines with daily movement opportunities",
        "Support regular movement by incorporating active breaks into the workday",
    ],

    "weight_management": [
        "Provide structured weight management support for employees who may benefit",
        "Encourage healthy weight management through personalised lifestyle programmes",
        "Support long-term weight management with nutrition and physical activity guidance",
    ],

    "sleep_health": [
        "Promote healthy sleep habits to support recovery and overall wellbeing",
        "Encourage good sleep practices through workplace wellbeing initiatives",
        "Increase awareness of the importance of sleep for long-term health",
    ],

    "stress_management": [
        "Provide practical stress management resources and wellbeing support",
        "Encourage healthy stress management through workplace wellbeing programmes",
        "Support mental wellbeing by promoting stress reduction strategies",
    ],

    "recovery_programme": [
        "Develop recovery programmes focused on sleep, stress management, and healthy daily habits",
        "Support employee recovery through integrated wellbeing initiatives",
        "Promote recovery by combining healthy sleep, stress reduction, and restorative practices",
    ],

    "womens_health": [
        "Strengthen women's health programmes through education and preventive screening",
        "Improve access to confidential women's health support and clinical guidance",
        "Promote greater awareness of women's health through dedicated wellbeing initiatives",
    ],

    "smoking_cessation": [
        "Provide evidence-based smoking cessation support for employees",
        "Offer accessible programmes to help employees quit smoking",
        "Support tobacco cessation through counselling and workplace resources",
    ],

    "alcohol_moderation": [
        "Promote responsible alcohol consumption through health education",
        "Provide guidance on reducing alcohol-related health risks",
        "Encourage healthier drinking habits through workplace awareness programmes",
    ],

    "maintain_wellness": [
        "Continue existing preventive health initiatives and regular monitoring",
        "Maintain current wellbeing programmes that are supporting positive health outcomes",
        "Sustain healthy workplace practices through ongoing preventive care",
    ],

    "target_high_risk": [
        "Provide targeted health coaching for employees identified as higher risk",
        "Prioritise follow-up support for employees with elevated health risk",
        "Deliver focused preventive interventions for higher-risk groups",
    ],

    "scale_preventive_care": [
        "Expand preventive health programmes across screening, nutrition, and physical activity",
        "Strengthen organisation-wide preventive care through integrated wellness initiatives",
        "Broaden preventive services by combining clinical screening with lifestyle support",
    ],

    "clinical_review": [
        "Recommend clinical evaluation for employees with persistent abnormal findings",
        "Encourage medical review when health indicators require further assessment",
        "Support timely clinical follow-up for employees with concerning health markers",
    ],
}

# Synonymous, unquantified lead-ins for concern observations.
ELEVATED_SHARE_LEADS: List[str] = [
    "A considerable proportion of employees",
    "A notable proportion of employees",
    "A significant number of employees",
    "A meaningful proportion of employees",
    "A substantial share of employees",

]


# --- accessors (safe fallbacks; never raise on unknown ids) -------------------

def disease_severity_language(disease_id: str, band: str) -> Optional[str]:
    """Disease-specific severity phrase for a band, or None to fall back to the
    engine's generic language. Reasoning is unaffected — this is wording only."""
    return DISEASE_SEVERITY_LANGUAGE.get(disease_id, {}).get(band)


def disease_interpretation_clauses(disease_id: str) -> List[str]:
    return DISEASE_INTERPRETATION_PHRASES.get(disease_id, [])


def disease_positive_narratives(disease_id: str) -> List[str]:
    return DISEASE_POSITIVE_NARRATIVES.get(disease_id, [])


def disease_leadership_takeaways(disease_id: str) -> List[str]:
    return DISEASE_LEADERSHIP_TAKEAWAYS.get(disease_id, [])


def lever_action_variants(lever: str) -> List[str]:
    return LEVER_ACTION_VARIANTS.get(lever, [])
