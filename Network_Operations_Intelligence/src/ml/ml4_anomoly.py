import os
import joblib
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from pathlib import Path

# =====================================================
# FILE PATHS
# =====================================================

NP3_PATH = "./output/network_alerts.csv"
MODEL_PATH = Path(__file__).resolve().parent / "ml3_outputs" / "ml3_xgboost_model.joblib"

# =====================================================
# LOAD ML2 FEATURE DATA
# =====================================================

print("\nLoading feature dataset...")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "network_operations")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD","root")

JDBC_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)
FEATURE_TABLE = "network_feature_table"

engine = create_engine(JDBC_URL)

# =====================================================
# LOAD ML2 FEATURES
# =====================================================

query_features = f"""
SELECT
    grid_id,
    feature_timestamp,
    avg_activity,
    activity_growth,
    active_hours,
    peak_ratio,
    variability,
    internet_share
FROM {FEATURE_TABLE}
ORDER BY feature_timestamp, grid_id
"""

features_df = pd.read_sql(query_features, engine)

features_df["feature_timestamp"] = pd.to_datetime(
    features_df["feature_timestamp"]
)

features_df["grid_id"] = (
    features_df["grid_id"]
    .astype(str)
    .str.strip()
)

print("Feature Dataset Shape:", features_df.shape)


# =====================================================
# LOAD ACTUAL NETWORK ACTIVITY
# =====================================================

query_activity = """
SELECT
    g.grid_id,
    t.timestamp AS feature_timestamp,
    f.total_activity
FROM fact_network_activity f
JOIN dim_grid g
    ON f.grid_key = g.grid_key
JOIN dim_time t
    ON f.time_key = t.time_key
ORDER BY t.timestamp, g.grid_id
"""

activity_df = pd.read_sql(query_activity, engine)

activity_df["feature_timestamp"] = pd.to_datetime(
    activity_df["feature_timestamp"]
)

print("Activity Dataset Shape:", activity_df.shape)


# =====================================================
# JOIN ML2 FEATURES + ACTUAL ACTIVITY
# =====================================================

features_df = features_df.merge(
    activity_df,
    on=[
        "grid_id",
        "feature_timestamp"
    ],
    how="left"
)

print(
    "Combined Feature Dataset Shape:",
    features_df.shape
)# =====================================================
# LOAD ML3 MODEL
# =====================================================

print("\nLoading ML3 model...")

if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(
        f"ML3 model not found: {MODEL_PATH}"
    )

model = joblib.load(MODEL_PATH)

MODEL_VERSION = "XGBoost"

MODEL_FEATURES = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
    "hour_sin",
    "hour_cos"
]

print("ML3 Model Version:", MODEL_VERSION)
print("ML3 Model Features:", MODEL_FEATURES)


# =====================================================
# DERIVE ML3 TIME FEATURES
# =====================================================

features_df["hour_of_day"] = (
    features_df["feature_timestamp"].dt.hour
)

features_df["hour_sin"] = np.sin(
    2 * np.pi * features_df["hour_of_day"] / 24
)

features_df["hour_cos"] = np.cos(
    2 * np.pi * features_df["hour_of_day"] / 24
)


# =====================================================
# VERIFY REQUIRED ML3 FEATURES
# =====================================================

missing_features = [
    feature
    for feature in MODEL_FEATURES
    if feature not in features_df.columns
]

if missing_features:
    raise ValueError(
        f"Missing ML3 features in ML2 dataset: "
        f"{missing_features}"
    )


# =====================================================
# GENERATE ML3 PREDICTIONS
# =====================================================

print("\nGenerating ML3 predictions...")

ml3_input = features_df[
    MODEL_FEATURES
].copy()

features_df["ml3_prediction"] = (
    model.predict(ml3_input)
    .astype(int)
)


# =====================================================
# ML3 RISK SCORE
# =====================================================

if hasattr(model, "predict_proba"):
    features_df["ml3_risk_score"] = (
        model.predict_proba(ml3_input)[:, 1]
    )
else:
    features_df["ml3_risk_score"] = (
        features_df["ml3_prediction"]
    )

print("\nML3 Prediction Distribution:")
print(
    features_df["ml3_prediction"]
    .value_counts()
)

# =====================================================
# HOUR KEY
# =====================================================

features_df["hour_key"] = (
    features_df["feature_timestamp"]
    .dt.floor("h")
)

# =====================================================
# HOUR OF DAY
# =====================================================

features_df["hour_of_day"] = (
    features_df["feature_timestamp"]
    .dt.hour
)

# =====================================================
# HISTORICAL BASELINE
# MEDIAN ACTIVITY PER GRID + HOUR
# =====================================================

baseline_df = (
    features_df
    .groupby(
        [
            "grid_id",
            "hour_of_day"
        ]
    )["total_activity"]
    .median()
    .reset_index()
)

baseline_df = baseline_df.rename(
    columns={
        "total_activity":
        "baseline_activity"
    }
)

print(
    "\nBaseline Rows:",
    len(baseline_df)
)

# =====================================================
# JOIN BASELINE
# =====================================================

anomaly_df = features_df.merge(
    baseline_df,
    on=[
        "grid_id",
        "hour_of_day"
    ],
    how="left"
)

# =====================================================
# CURRENT ACTIVITY
# =====================================================

anomaly_df["current_activity"] = (
    anomaly_df["total_activity"]
)

# =====================================================
# DEVIATION
# =====================================================

anomaly_df["deviation"] = (
    anomaly_df["current_activity"]
    -
    anomaly_df["baseline_activity"]
)

# =====================================================
# ANOMALY SCORE
# =====================================================

anomaly_df["anomaly_score"] = np.where(
    anomaly_df["baseline_activity"] > 0,
    (
        anomaly_df["deviation"]
        /
        anomaly_df["baseline_activity"]
    ),
    0
)

anomaly_df["anomaly_score"] = (
    anomaly_df["anomaly_score"]
    .replace(
        [np.inf, -np.inf],
        0
    )
    .fillna(0)
)

# =====================================================
# ANOMALY THRESHOLD
# =====================================================

THRESHOLD = 0.50

# =====================================================
# ANOMALY FLAG
# =====================================================

anomaly_df["anomaly_flag"] = 0

anomaly_df.loc[
    anomaly_df["anomaly_score"] >= THRESHOLD,
    "anomaly_flag"
] = 1

anomaly_df.loc[
    anomaly_df["anomaly_score"] <= -THRESHOLD,
    "anomaly_flag"
] = 1

# =====================================================
# ANOMALY DIRECTION
# =====================================================

anomaly_df["anomaly_direction"] = "NORMAL"

anomaly_df.loc[
    anomaly_df["anomaly_score"] >= THRESHOLD,
    "anomaly_direction"
] = "HIGH"

anomaly_df.loc[
    anomaly_df["anomaly_score"] <= -THRESHOLD,
    "anomaly_direction"
] = "LOW"

# =====================================================
# HUMAN READABLE REASON
# =====================================================

def build_reason(row):

    if row["anomaly_direction"] == "HIGH":

        pct = round(
            row["anomaly_score"] * 100,
            2
        )

        return (
            f"HIGH_ANOMALY: current activity "
            f"{row['current_activity']:.2f} "
            f"is {pct}% above historical baseline "
            f"{row['baseline_activity']:.2f}"
        )

    if row["anomaly_direction"] == "LOW":

        pct = round(
            abs(row["anomaly_score"]) * 100,
            2
        )

        return (
            f"LOW_ANOMALY: current activity "
            f"{row['current_activity']:.2f} "
            f"is {pct}% below historical baseline "
            f"{row['baseline_activity']:.2f}"
        )

    return "NORMAL_ACTIVITY"


anomaly_df["anomaly_reason"] = (
    anomaly_df.apply(
        build_reason,
        axis=1
    )
)

# =====================================================
# NETWORK ANOMALY SCORES
# =====================================================

network_anomaly_scores = anomaly_df[
    [
        "grid_id",
        "feature_timestamp",
        "current_activity",
        "baseline_activity",
        "deviation",
        "anomaly_score",
        "anomaly_flag",
        "anomaly_direction",
        "anomaly_reason"
    ]
].copy()

network_anomaly_scores["hour_key"] = (
    network_anomaly_scores[
        "feature_timestamp"
    ].dt.floor("h")
)

network_anomaly_scores.to_csv(
    Path(__file__).resolve().parent / "network_anomaly_scores.csv",
    index=False
)

print(
    "\nnetwork_anomaly_scores.csv created"
)

# =====================================================
# LOAD NP3 ALERTS
# =====================================================

print("\nLoading NP3 alerts...")

np3_df = pd.read_csv(NP3_PATH)

np3_df = np3_df.rename(
    columns={
        "timestamp":
        "feature_timestamp"
    }
)

np3_df["feature_timestamp"] = (
    pd.to_datetime(
        np3_df["feature_timestamp"]
    )
)

np3_df["hour_key"] = (
    np3_df["feature_timestamp"]
    .dt.floor("h")
)

# Every NP3 record is an alert
np3_df["np3_alert"] = 1

print("\nNP3 Alert Types")

print(
    np3_df["alert_type"]
    .value_counts()
)

# =====================================================
# AGGREGATE NP3 BY GRID + HOUR
# =====================================================

np3_hourly = (
    np3_df
    .groupby(
        [
            "grid_id",
            "hour_key"
        ],
        as_index=False
    )
    .agg(
        np3_alert=(
            "np3_alert",
            "max"
        ),
        alert_type=(
            "alert_type",
            lambda x:
            ", ".join(
                sorted(
                    set(
                        x.astype(str)
                    )
                )
            )
        )
    )
)

# =====================================================
# PREPARE ML3 PREDICTIONS
# =====================================================

ml3_df = anomaly_df[
    [
        "grid_id",
        "feature_timestamp",
        "ml3_prediction",
        "ml3_risk_score"
    ]
].copy()

ml3_df = ml3_df.rename(
    columns={
        "ml3_prediction":
        "model_prediction"
    }
)

ml3_df["hour_key"] = (
    ml3_df["feature_timestamp"]
    .dt.floor("h")
)

# =====================================================
# KEEP ONE ML3 PREDICTION PER GRID + HOUR
# =====================================================

ml3_hourly = (
    ml3_df[
        [
            "grid_id",
            "hour_key",
            "model_prediction",
            "ml3_risk_score"
        ]
    ]
    .drop_duplicates(
        subset=[
            "grid_id",
            "hour_key"
        ]
    )
)

# =====================================================
# START FROM FULL ML4 TIMELINE
# =====================================================

comparison_df = (
    network_anomaly_scores.copy()
)

# =====================================================
# JOIN NP3
# =====================================================
# =====================================================
# ENSURE GRID ID TYPES MATCH
# =====================================================

comparison_df["grid_id"] = (
    comparison_df["grid_id"]
    .astype(str)
    .str.strip()
)

np3_hourly["grid_id"] = (
    np3_hourly["grid_id"]
    .astype(str)
    .str.strip()
)
comparison_df = comparison_df.merge(
    np3_hourly,
    on=[
        "grid_id",
        "hour_key"
    ],
    how="left"
)

comparison_df["np3_alert"] = (
    comparison_df["np3_alert"]
    .fillna(0)
    .astype(int)
)

comparison_df["alert_type"] = (
    comparison_df["alert_type"]
    .fillna("NO_ALERT")
)

# =====================================================
# JOIN ML3
# =====================================================

comparison_df = comparison_df.merge(
    ml3_hourly,
    on=[
        "grid_id",
        "hour_key"
    ],
    how="left"
)

comparison_df["model_prediction"] = (
    comparison_df["model_prediction"]
    .fillna(0)
    .astype(int)
)

comparison_df["ml3_risk_score"] = (
    comparison_df["ml3_risk_score"]
    .fillna(0)
)

# =====================================================
# AGREEMENT LOGIC
# =====================================================

def agreement_type(row):

    np3 = row["np3_alert"]
    ml3 = row["model_prediction"]
    ml4 = row["anomaly_flag"]

    if np3 and ml3 and ml4:
        return "ALL_AGREE"

    if np3 and ml3:
        return "NP3_ML3_ONLY"

    if np3 and ml4:
        return "NP3_ML4_ONLY"

    if ml3 and ml4:
        return "ML3_ML4_ONLY"

    if np3:
        return "NP3_ONLY"

    if ml3:
        return "ML3_ONLY"

    if ml4:
        return "ML4_ONLY"

    return "NO_SIGNAL"


comparison_df["agreement_type"] = (
    comparison_df.apply(
        agreement_type,
        axis=1
    )
)

# =====================================================
# SUMMARY
# =====================================================

print(
    "\nTHREE WAY COMPARISON"
)

print(
    comparison_df[
        "agreement_type"
    ].value_counts()
)

# =====================================================
# SAVE COMPARISON
# =====================================================

comparison_df.to_csv(
    Path(__file__).resolve().parent / "three_way_comparison.csv",
    index=False
)

print(
    "\nthree_way_comparison.csv created"
)

# =====================================================
# FINAL SUMMARY
# =====================================================

print("\nML4 COMPLETE")

print(
    "\nModel Version:",
    MODEL_VERSION
)

print(
    "Model Features:",
    MODEL_FEATURES
)

print(
    "Total Rows:",
    len(comparison_df)
)

print(
    "ML3 Positive Predictions:",
    comparison_df["model_prediction"].sum()
)

print(
    "ML4 Anomalies:",
    comparison_df["anomaly_flag"].sum()
)

print(
    "NP3 Alerts:",
    comparison_df["np3_alert"].sum()
)