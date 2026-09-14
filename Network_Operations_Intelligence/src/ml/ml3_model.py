"""
ML3 - Network Activity Risk Classification

Converted from the original Jupyter Notebook.

Purpose:
    1. Load ML2 features and network activity from MySQL.
    2. Create a next-hour prediction target.
    3. Add time-of-day features.
    4. Split the data chronologically.
    5. Create a grid-specific P75 high-activity label.
    6. Train and compare classification models.
    7. Evaluate the final XGBoost model.
    8. Test probability thresholds.
    9. Compare ML3 predictions with NP3 alerts.
    10. Save ML3 outputs and an evaluation report.

Required packages:
    pandas
    numpy
    sqlalchemy
    pymysql
    scikit-learn
    xgboost
"""

import os
import logging
import warnings
from datetime import timedelta

import numpy as np
import pandas as pd
import joblib
from pathlib import Path
from sqlalchemy import create_engine

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import (
    RandomForestClassifier,
    GradientBoostingClassifier
)
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report,
    roc_auc_score,
    average_precision_score
)

from xgboost import XGBClassifier

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("ML3")

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

features_df = features_df.sort_values(
    ["feature_timestamp", "grid_id"]
).reset_index(drop=True)

print("Rows:", len(features_df))
print("Grids:", features_df["grid_id"].nunique())
print(
    "Timestamp:",
    features_df["feature_timestamp"].min(),
    "->",
    features_df["feature_timestamp"].max()
)

features_df.head()

query_activity = """
SELECT
    g.grid_id,
    t.timestamp,
    f.total_activity
FROM fact_network_activity f
JOIN dim_time t
    ON f.time_key = t.time_key
JOIN dim_grid g
    ON f.grid_key = g.grid_key
ORDER BY t.timestamp, g.grid_id
"""

activity_df = pd.read_sql(query_activity, engine)

activity_df["timestamp"] = pd.to_datetime(
    activity_df["timestamp"]
)

activity_df = activity_df.sort_values(
    ["timestamp", "grid_id"]
).reset_index(drop=True)

print("Rows:", len(activity_df))
print("Grids:", activity_df["grid_id"].nunique())
print(
    "Timestamp:",
    activity_df["timestamp"].min(),
    "->",
    activity_df["timestamp"].max()
)

activity_df.head()

future_activity = activity_df.copy()

future_activity["feature_timestamp"] = (
    future_activity["timestamp"] - pd.Timedelta(hours=1)
)

future_activity = future_activity.rename(
    columns={
        "total_activity": "future_total_activity"
    }
)

future_activity = future_activity[
    [
        "grid_id",
        "feature_timestamp",
        "future_total_activity"
    ]
]

model_df = features_df.merge(
    future_activity,
    on=["grid_id", "feature_timestamp"],
    how="inner"
)

model_df = model_df.sort_values(
    ["feature_timestamp", "grid_id"]
).reset_index(drop=True)

print("ML2 rows:", len(features_df))
print("Rows with t+1 target:", len(model_df))

print(
    "Timestamp:",
    model_df["feature_timestamp"].min(),
    "->",
    model_df["feature_timestamp"].max()
)

model_df.head()

# Verify the t -> t+1 relationship

sample_grid = model_df["grid_id"].iloc[0]

check = model_df[
    model_df["grid_id"] == sample_grid
][[
    "grid_id",
    "feature_timestamp",
    "future_total_activity"
]].head(10)

check

row = model_df[
    model_df["grid_id"] == sample_grid
].iloc[0]

t = row["feature_timestamp"]
grid = row["grid_id"]

actual_next_hour = activity_df[
    (activity_df["grid_id"] == grid) &
    (activity_df["timestamp"] == t + pd.Timedelta(hours=1))
]["total_activity"]

print("Grid:", grid)
print("t:", t)
print("Expected future time:", t + pd.Timedelta(hours=1))
print("future_total_activity:", row["future_total_activity"])

if len(actual_next_hour) > 0:
    print("Actual activity:", actual_next_hour.iloc[0])

# NEW FEATURES hour_sin, hour_cos
import numpy as np

# ============================================================
# ADD TIME-OF-DAY FEATURES
# ============================================================

model_df["hour"] = model_df["feature_timestamp"].dt.hour

model_df["hour_sin"] = np.sin(
    2 * np.pi * model_df["hour"] / 24
)

model_df["hour_cos"] = np.cos(
    2 * np.pi * model_df["hour"] / 24
)

print("Time features added successfully.")
print(
    model_df[
        ["feature_timestamp", "hour", "hour_sin", "hour_cos"]
    ].head()
)


# NEW FEATURES hour_sin, hour_cos
ML3_FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
    "hour_sin",
    "hour_cos"
]

print("ML3 features:")
for feature in ML3_FEATURE_COLUMNS:
    print("-", feature)

# NEW FEATURES hour_sin, hour_cos
# ============================================================
# CHRONOLOGICAL TRAIN / TEST SPLIT
# ============================================================

unique_timestamps = sorted(
    model_df["feature_timestamp"].unique()
)

split_index = int(len(unique_timestamps) * 0.8)

train_timestamps = unique_timestamps[:split_index]
test_timestamps = unique_timestamps[split_index:]

train_new = model_df[
    model_df["feature_timestamp"].isin(train_timestamps)
].copy()

test_new = model_df[
    model_df["feature_timestamp"].isin(test_timestamps)
].copy()

print("Train rows:", len(train_new))
print("Test rows:", len(test_new))

print(
    "Train period:",
    train_new["feature_timestamp"].min(),
    "to",
    train_new["feature_timestamp"].max()
)

print(
    "Test period:",
    test_new["feature_timestamp"].min(),
    "to",
    test_new["feature_timestamp"].max()
)

# ============================================================
# GRID-SPECIFIC P75 TARGET
# ============================================================

grid_thresholds = (
    train_new
    .groupby("grid_id")["future_total_activity"]
    .quantile(0.75)
    .reset_index()
    .rename(
        columns={
            "future_total_activity": "grid_threshold"
        }
    )
)

train_new = train_new.merge(
    grid_thresholds,
    on="grid_id",
    how="left"
)

test_new = test_new.merge(
    grid_thresholds,
    on="grid_id",
    how="left"
)

train_new["risk_label_grid"] = (
    train_new["future_total_activity"]
    > train_new["grid_threshold"]
).astype(int)

test_new["risk_label_grid"] = (
    test_new["future_total_activity"]
    > test_new["grid_threshold"]
).astype(int)

print("Grid-specific P75 target created.")

print("\nTrain positive rate:",
      train_new["risk_label_grid"].mean())

print("Test positive rate:",
      test_new["risk_label_grid"].mean())

# ============================================================
# PREPARE FEATURES AND TARGET
# ============================================================

X_train_new = train_new[ML3_FEATURE_COLUMNS]
y_train_new = train_new["risk_label_grid"]

X_test_new = test_new[ML3_FEATURE_COLUMNS]
y_test_new = test_new["risk_label_grid"]

print("X_train shape:", X_train_new.shape)
print("X_test shape :", X_test_new.shape)

print("\nTraining positive rate:", y_train_new.mean())
print("Testing positive rate :", y_test_new.mean())

models_new = {
    "Logistic Regression": Pipeline([
        ("scaler", StandardScaler()),
        ("model", LogisticRegression(
            max_iter=1000,
            random_state=42
        ))
    ]),

    "Decision Tree": DecisionTreeClassifier(
        max_depth=8,
        random_state=42
    ),

    "Random Forest": RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    ),

    "Gradient Boosting": GradientBoostingClassifier(
        random_state=42
    ),

    "XGBoost": XGBClassifier(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        eval_metric="logloss",
        n_jobs=-1
    )
}

print("Models created:")
for name in models_new:
    print("-", name)

# ============================================================
# MODEL TRAINING
# ============================================================

trained_models_new = {}

for name, model in models_new.items():

    print(f"\nTraining {name}...")

    model.fit(
        X_train_new,
        y_train_new
    )

    trained_models_new[name] = model

    print(f"{name} training completed.")

# ============================================================
# PREDICTIONS
# ============================================================

predictions_new = {}

for name, model in trained_models_new.items():

    y_pred = model.predict(X_test_new)
    y_prob = model.predict_proba(X_test_new)[:, 1]

    predictions_new[name] = {
        "model": model,
        "y_pred": y_pred,
        "y_prob": y_prob
    }

    print(f"{name}: predictions generated.")

new_metrics = []

for name, result in predictions_new.items():

    y_pred = result["y_pred"]
    y_prob = result["y_prob"]

    new_metrics.append({
        "Model": name,
        "Accuracy": accuracy_score(y_test_new, y_pred),
        "Precision": precision_score(
            y_test_new,
            y_pred,
            zero_division=0
        ),
        "Recall": recall_score(
            y_test_new,
            y_pred,
            zero_division=0
        ),
        "F1": f1_score(
            y_test_new,
            y_pred,
            zero_division=0
        ),
        "ROC_AUC": roc_auc_score(
            y_test_new,
            y_prob
        ),
        "Average_Precision": average_precision_score(
            y_test_new,
            y_prob
        )
    })

new_metrics_df = pd.DataFrame(new_metrics)

print("\nMetrics with time features:")
print(new_metrics_df.to_string(index=False))

xgb_prob = predictions_new["XGBoost"]["y_prob"]

threshold_results = []

for threshold in [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60]:

    y_pred_threshold = (
        xgb_prob >= threshold
    ).astype(int)

    threshold_results.append({
        "threshold": threshold,
        "precision": precision_score(
            y_test_new,
            y_pred_threshold,
            zero_division=0
        ),
        "recall": recall_score(
            y_test_new,
            y_pred_threshold,
            zero_division=0
        ),
        "f1": f1_score(
            y_test_new,
            y_pred_threshold,
            zero_division=0
        ),
        "accuracy": accuracy_score(
            y_test_new,
            y_pred_threshold
        )
    })

threshold_df = pd.DataFrame(threshold_results)

print(threshold_df.to_string(index=False))

best_model = trained_models_new["XGBoost"]

importance_df = pd.DataFrame({
    "feature": ML3_FEATURE_COLUMNS,
    "importance": best_model.feature_importances_
}).sort_values(
    "importance",
    ascending=False
)

print(importance_df.to_string(index=False))

# ============================================================
# FINAL XGBOOST PREDICTIONS
# ============================================================

FINAL_THRESHOLD = 0.30

xgb_model = trained_models_new["XGBoost"]

xgb_prob = xgb_model.predict_proba(X_test_new)[:, 1]

xgb_final_pred = (
    xgb_prob >= FINAL_THRESHOLD
).astype(int)

print("Final model: XGBoost")
print(f"Prediction threshold: {FINAL_THRESHOLD}")

# ============================================================
# FINAL XGBOOST METRICS
# ============================================================

final_accuracy = accuracy_score(
    y_test_new,
    xgb_final_pred
)

final_precision = precision_score(
    y_test_new,
    xgb_final_pred,
    zero_division=0
)

final_recall = recall_score(
    y_test_new,
    xgb_final_pred,
    zero_division=0
)

final_f1 = f1_score(
    y_test_new,
    xgb_final_pred,
    zero_division=0
)

final_roc_auc = roc_auc_score(
    y_test_new,
    xgb_prob
)

final_average_precision = average_precision_score(
    y_test_new,
    xgb_prob
)

final_metrics = pd.DataFrame([{
    "Model": "XGBoost",
    "Threshold": FINAL_THRESHOLD,
    "Accuracy": final_accuracy,
    "Precision": final_precision,
    "Recall": final_recall,
    "F1": final_f1,
    "ROC_AUC": final_roc_auc,
    "Average_Precision": final_average_precision
}])

print(final_metrics.to_string(index=False))

# ============================================================
# CONFUSION MATRIX
# ============================================================

cm = confusion_matrix(
    y_test_new,
    xgb_final_pred
)

print("Confusion Matrix:")
print(cm)

# ============================================================
# CLASSIFICATION REPORT
# ============================================================

print(
    classification_report(
        y_test_new,
        xgb_final_pred,
        target_names=["Normal", "High Activity"],
        zero_division=0
    )
)

# ============================================================
# ML3 TEST PREDICTIONS
# ============================================================

ml3_results = test_new[
    [
        "grid_id",
        "feature_timestamp"
    ]
].copy()

ml3_results["predicted_probability"] = xgb_prob

ml3_results["risk_prediction"] = xgb_final_pred

ml3_results["risk_level"] = np.where(
    xgb_final_pred == 1,
    "High Activity",
    "Normal"
)


# ============================================================
# LOAD NP3 ALERTS
# ============================================================

NP3_OUTPUT_PATH = Path(
    "output/network_alerts.csv"
)

np3_results = pd.read_csv(
    NP3_OUTPUT_PATH
)

print("NP3 rows:", len(np3_results))
print("NP3 columns:")
print(np3_results.columns.tolist())


# ============================================================
# PREPARE ML3 FOR NP3 COMPARISON
# ============================================================

ml3_comparison = ml3_results.copy()

# ML3 predicts the next hour
ml3_comparison["timestamp"] = (
    ml3_comparison["feature_timestamp"]
    + pd.Timedelta(hours=1)
)

ml3_comparison = ml3_comparison[
    [
        "grid_id",
        "feature_timestamp",
        "timestamp",
        "predicted_probability",
        "risk_prediction",
        "risk_level"
    ]
].copy()

print("ML3 comparison rows:", len(ml3_comparison))
print(
    "ML3 prediction period:",
    ml3_comparison["timestamp"].min(),
    "to",
    ml3_comparison["timestamp"].max()
)


# ============================================================
# PREPARE NP3 FOR COMPARISON
# ============================================================

np3_comparison = np3_results.copy()

np3_comparison["timestamp"] = pd.to_datetime(
    np3_comparison["timestamp"]
)

np3_comparison["np3_alert"] = 1

# One row per grid + timestamp
np3_comparison = (
    np3_comparison
    .groupby(
        ["grid_id", "timestamp"],
        as_index=False
    )
    .agg(
        np3_alert=("np3_alert", "max")
    )
)

print("Unique NP3 alert grid-hours:", len(np3_comparison))

print(
    "NP3 alert period:",
    np3_comparison["timestamp"].min(),
    "to",
    np3_comparison["timestamp"].max()
)


# ============================================================
# MERGE ML3 AND NP3
# ============================================================
ml3_comparison["grid_id"] = ml3_comparison["grid_id"].astype(str).str.strip()
np3_comparison["grid_id"] = np3_comparison["grid_id"].astype(str).str.strip()

ml3_comparison["timestamp"] = pd.to_datetime(ml3_comparison["timestamp"])
np3_comparison["timestamp"] = pd.to_datetime(np3_comparison["timestamp"])

comparison_df = ml3_comparison.merge(
    np3_comparison,
    on=["grid_id", "timestamp"],
    how="left"
)

# If NP3 has no alert for that grid-hour,
# it means NP3 did not generate an alert.
comparison_df["np3_alert"] = (
    comparison_df["np3_alert"]
    .fillna(0)
    .astype(int)
)

comparison_df["ml3_alert"] = (
    comparison_df["risk_prediction"]
    .astype(int)
)

print("Comparison rows:", len(comparison_df))


# ============================================================
# ML3 vs NP3 COMPARISON CATEGORIES
# ============================================================

comparison_df["comparison"] = np.select(
    [
        (comparison_df["ml3_alert"] == 1) &
        (comparison_df["np3_alert"] == 1),

        (comparison_df["ml3_alert"] == 1) &
        (comparison_df["np3_alert"] == 0),

        (comparison_df["ml3_alert"] == 0) &
        (comparison_df["np3_alert"] == 1),

        (comparison_df["ml3_alert"] == 0) &
        (comparison_df["np3_alert"] == 0)
    ],
    [
        "Both Alert",
        "ML3 Only",
        "NP3 Only",
        "Neither"
    ],
    default="Unknown"
)

comparison_summary = (
    comparison_df["comparison"]
    .value_counts()
    .rename_axis("Outcome")
    .reset_index(name="Count")
)

comparison_summary["Percentage"] = (
    comparison_summary["Count"]
    / len(comparison_df)
    * 100
)

print(comparison_summary.to_string(index=False))

# ============================================================
# CHECK NP3 ALERTS IN ML3 TEST PERIOD
# ============================================================

print("ML3 comparison period:")
print(
    ml3_comparison["timestamp"].min(),
    "to",
    ml3_comparison["timestamp"].max()
)

print("\nNP3 period:")
print(
    np3_comparison["timestamp"].min(),
    "to",
    np3_comparison["timestamp"].max()
)

print("\nNP3 alerts inside ML3 period:")

np3_in_ml3_period = np3_comparison[
    np3_comparison["timestamp"].isin(
        ml3_comparison["timestamp"].unique()
    )
]

print(len(np3_in_ml3_period))


# ============================================================
# ML3 / NP3 AGREEMENT
# ============================================================

agreement = (
    comparison_df["ml3_alert"]
    == comparison_df["np3_alert"]
).mean()

print(f"ML3-NP3 agreement: {agreement:.2%}")

# ============================================================
# ALERT COUNTS
# ============================================================

ml3_alert_count = comparison_df["ml3_alert"].sum()
np3_alert_count = comparison_df["np3_alert"].sum()

print(f"ML3 alerts: {ml3_alert_count:,}")
print(f"NP3 alerts: {np3_alert_count:,}")

print(
    f"ML3 alert rate: "
    f"{ml3_alert_count / len(comparison_df):.2%}"
)

print(
    f"NP3 alert rate: "
    f"{np3_alert_count / len(comparison_df):.2%}"
)

# ============================================================
# ML3-ONLY ALERTS
# ============================================================

ml3_only = comparison_df[
    comparison_df["comparison"] == "ML3 Only"
].copy()

print("ML3-only alerts:", len(ml3_only))


# ============================================================
# NP3-ONLY ALERTS
# ============================================================

np3_only = comparison_df[
    comparison_df["comparison"] == "NP3 Only"
].copy()

print("NP3-only alerts:", len(np3_only))


# ============================================================
# NP3 HIGH-ACTIVITY ALERTS ONLY
# ============================================================

np3_high_activity = np3_results[
    np3_results["alert_type"].isin(
        ["HIGH_ACTIVITY", "ACTIVITY_SPIKE"]
    )
].copy()

np3_high_activity["timestamp"] = pd.to_datetime(
    np3_high_activity["timestamp"]
)

np3_high_activity["np3_high_alert"] = 1

np3_high_activity = (
    np3_high_activity
    .groupby(
        ["grid_id", "timestamp"],
        as_index=False
    )
    .agg(
        np3_high_alert=("np3_high_alert", "max")
    )
)

print(
    "Unique NP3 high-activity alert grid-hours:",
    len(np3_high_activity)
)

# ============================================================
# ML3 vs NP3 HIGH-ACTIVITY ALERTS
# ============================================================
ml3_comparison["grid_id"] = ml3_comparison["grid_id"].astype(str).str.strip()
np3_high_activity["grid_id"] = np3_high_activity["grid_id"].astype(str).str.strip()

ml3_comparison["timestamp"] = pd.to_datetime(ml3_comparison["timestamp"])
np3_high_activity["timestamp"] = pd.to_datetime(np3_high_activity["timestamp"])

comparison_high_df = ml3_comparison.merge(
    np3_high_activity,
    on=["grid_id", "timestamp"],
    how="left"
)

comparison_high_df["np3_high_alert"] = (
    comparison_high_df["np3_high_alert"]
    .fillna(0)
    .astype(int)
)

comparison_high_df["ml3_alert"] = (
    comparison_high_df["risk_prediction"]
    .astype(int)
)

comparison_high_df["comparison"] = np.select(
    [
        (comparison_high_df["ml3_alert"] == 1) &
        (comparison_high_df["np3_high_alert"] == 1),

        (comparison_high_df["ml3_alert"] == 1) &
        (comparison_high_df["np3_high_alert"] == 0),

        (comparison_high_df["ml3_alert"] == 0) &
        (comparison_high_df["np3_high_alert"] == 1),

        (comparison_high_df["ml3_alert"] == 0) &
        (comparison_high_df["np3_high_alert"] == 0)
    ],
    [
        "Both Alert",
        "ML3 Only",
        "NP3 Only",
        "Neither"
    ],
    default="Unknown"
)

comparison_high_summary = (
    comparison_high_df["comparison"]
    .value_counts()
    .rename_axis("Outcome")
    .reset_index(name="Count")
)

comparison_high_summary["Percentage"] = (
    comparison_high_summary["Count"]
    / len(comparison_high_df)
    * 100
)

print(comparison_high_summary.to_string(index=False))

import os


# ============================================================
# ML3 OUTPUT DIRECTORY
# ============================================================

ML3_OUTPUT_DIR = Path(__file__).resolve().parent / "ml3_outputs"

os.makedirs(
    ML3_OUTPUT_DIR,
    exist_ok=True
)

print(f"ML3 output directory: {ML3_OUTPUT_DIR}")

# ============================================================
# FINAL ML3 PREDICTIONS
# ============================================================

final_ml3_predictions = ml3_results.copy()

final_ml3_predictions["prediction_timestamp"] = (
    final_ml3_predictions["feature_timestamp"]
    + pd.Timedelta(hours=1)
)

final_ml3_predictions = final_ml3_predictions[
    [
        "grid_id",
        "feature_timestamp",
        "prediction_timestamp",
        "predicted_probability",
        "risk_prediction",
        "risk_level"
    ]
]

ml3_predictions_path = os.path.join(
    ML3_OUTPUT_DIR,
    "ml3_xgboost_predictions.csv"
)

final_ml3_predictions.to_csv(
    ml3_predictions_path,
    index=False
)

print(
    f"Saved ML3 predictions to:\n"
    f"{ml3_predictions_path}"
)

# ============================================================
# FINAL ML3 METRICS
# ============================================================

final_metrics = pd.DataFrame([{
    "model": "XGBoost",
    "threshold": FINAL_THRESHOLD,
    "accuracy": accuracy_score(
        y_test_new,
        xgb_final_pred
    ),
    "precision": precision_score(
        y_test_new,
        xgb_final_pred,
        zero_division=0
    ),
    "recall": recall_score(
        y_test_new,
        xgb_final_pred,
        zero_division=0
    ),
    "f1": f1_score(
        y_test_new,
        xgb_final_pred,
        zero_division=0
    ),
    "roc_auc": roc_auc_score(
        y_test_new,
        xgb_prob
    ),
    "average_precision": average_precision_score(
        y_test_new,
        xgb_prob
    )
}])

metrics_path = os.path.join(
    ML3_OUTPUT_DIR,
    "ml3_model_metrics.csv"
)

final_metrics.to_csv(
    metrics_path,
    index=False
)

print(final_metrics.to_string(index=False))

print(f"Saved metrics to:\n{metrics_path}")

# ============================================================
# SAVE CONFUSION MATRIX
# ============================================================

cm = confusion_matrix(
    y_test_new,
    xgb_final_pred
)

confusion_df = pd.DataFrame(
    cm,
    index=["Actual Normal", "Actual High Activity"],
    columns=["Predicted Normal", "Predicted High Activity"]
)

confusion_path = os.path.join(
    ML3_OUTPUT_DIR,
    "ml3_confusion_matrix.csv"
)

confusion_df.to_csv(
    confusion_path
)

print(confusion_df.to_string(index=False))

print(
    f"Saved confusion matrix to:\n"
    f"{confusion_path}"
)

# ============================================================
# SAVE FEATURE IMPORTANCE
# ============================================================

importance_df = pd.DataFrame({
    "feature": ML3_FEATURE_COLUMNS,
    "importance": xgb_model.feature_importances_
}).sort_values(
    "importance",
    ascending=False
)

importance_path = os.path.join(
    ML3_OUTPUT_DIR,
    "ml3_feature_importance.csv"
)

importance_df.to_csv(
    importance_path,
    index=False
)

print(importance_df.to_string(index=False))

print(
    f"Saved feature importance to:\n"
    f"{importance_path}"
)

# ============================================================
# SAVE ML3 vs NP3 COMPARISON
# ============================================================

comparison_path = os.path.join(
    ML3_OUTPUT_DIR,
    "ml3_np3_comparison.csv"
)

comparison_df.to_csv(
    comparison_path,
    index=False
)

print(
    f"Saved ML3-NP3 comparison to:\n"
    f"{comparison_path}"
)

# ============================================================
# SAVE ML3 vs NP3 SUMMARY
# ============================================================

comparison_summary_path = os.path.join(
    ML3_OUTPUT_DIR,
    "ml3_np3_comparison_summary.csv"
)

comparison_summary.to_csv(
    comparison_summary_path,
    index=False
)

print(comparison_summary.to_string(index=False))

print(
    f"Saved comparison summary to:\n"
    f"{comparison_summary_path}"
)

# ============================================================
# SAVE HIGH-ACTIVITY COMPARISON
# ============================================================

comparison_high_path = os.path.join(
    ML3_OUTPUT_DIR,
    "ml3_np3_high_activity_comparison.csv"
)

comparison_high_df.to_csv(
    comparison_high_path,
    index=False
)

comparison_high_summary_path = os.path.join(
    ML3_OUTPUT_DIR,
    "ml3_np3_high_activity_summary.csv"
)

comparison_high_summary.to_csv(
    comparison_high_summary_path,
    index=False
)

print(
    f"Saved high-activity comparison to:\n"
    f"{comparison_high_path}"
)

print(
    f"Saved high-activity summary to:\n"
    f"{comparison_high_summary_path}"
)

# ============================================================
# SAVE FINAL XGBOOST MODEL
# ============================================================

model_path = os.path.join(
    ML3_OUTPUT_DIR,
    "ml3_xgboost_model.joblib"
)

joblib.dump(
    xgb_model,
    model_path
)

print(
    f"Saved XGBoost model to:\n"
    f"{model_path}"
)

# ============================================================
# SAVE MODEL CONFIGURATION
# ============================================================

model_config = {
    "model": "XGBoost",
    "threshold": float(FINAL_THRESHOLD),
    "features": ML3_FEATURE_COLUMNS,
    "target": "risk_label_grid",
    "target_definition": (
        "1 if next-hour activity is above the "
        "grid-specific training-period 75th percentile"
    ),
    "prediction_horizon": "1 hour",
    "split_method": "chronological train-test split",
    "test_period": (
        f"{test_new['feature_timestamp'].min()} "
        f"to "
        f"{test_new['feature_timestamp'].max()}"
    )
}

config_path = os.path.join(
    ML3_OUTPUT_DIR,
    "ml3_model_config.joblib"
)

joblib.dump(
    model_config,
    config_path
)

print(
    f"Saved model configuration to:\n"
    f"{config_path}"
)

# ============================================================
# SAVE CLASSIFICATION REPORT
# ============================================================

report_dict = classification_report(
    y_test_new,
    xgb_final_pred,
    target_names=[
        "Normal",
        "High Activity"
    ],
    output_dict=True,
    zero_division=0
)

classification_df = pd.DataFrame(
    report_dict
).transpose()

classification_path = os.path.join(
    ML3_OUTPUT_DIR,
    "ml3_classification_report.csv"
)

classification_df.to_csv(
    classification_path
)

print(classification_df.to_string(index=False))

print(
    f"Saved classification report to:\n"
    f"{classification_path}"
)

# ============================================================
# SAVE ML3 SUMMARY
# ============================================================

ml3_summary = pd.DataFrame([
    {
        "item": "Final Model",
        "value": "XGBoost"
    },
    {
        "item": "Prediction Horizon",
        "value": "Next hour"
    },
    {
        "item": "Target",
        "value": "Grid-specific P75 high activity"
    },
    {
        "item": "Threshold",
        "value": FINAL_THRESHOLD
    },
    {
        "item": "Accuracy",
        "value": final_metrics.loc[0, "accuracy"]
    },
    {
        "item": "Precision",
        "value": final_metrics.loc[0, "precision"]
    },
    {
        "item": "Recall",
        "value": final_metrics.loc[0, "recall"]
    },
    {
        "item": "F1",
        "value": final_metrics.loc[0, "f1"]
    },
    {
        "item": "ROC-AUC",
        "value": final_metrics.loc[0, "roc_auc"]
    },
    {
        "item": "Average Precision",
        "value": final_metrics.loc[0, "average_precision"]
    },
    {
        "item": "ML3-NP3 Agreement",
        "value": agreement
    },
    {
        "item": "ML3 Alerts",
        "value": int(ml3_alert_count)
    },
    {
        "item": "NP3 Alerts in ML3 Test Period",
        "value": int(np3_alert_count)
    }
])

summary_path = os.path.join(
    ML3_OUTPUT_DIR,
    "ml3_summary.csv"
)

ml3_summary.to_csv(
    summary_path,
    index=False
)

print(ml3_summary.to_string(index=False))

print(
    f"Saved ML3 summary to:\n{summary_path}"
)

base_rate_df = pd.DataFrame([
    {
        "dataset": "train",
        "positive_class": 1,
        "positive_count": int(y_train_new.sum()),
        "total_count": int(len(y_train_new)),
        "positive_base_rate": float(y_train_new.mean())
    },
    {
        "dataset": "test",
        "positive_class": 1,
        "positive_count": int(y_test_new.sum()),
        "total_count": int(len(y_test_new)),
        "positive_base_rate": float(y_test_new.mean())
    }
])

base_rate_df.to_csv(
    os.path.join(
        ML3_OUTPUT_DIR,
        "ml3_positive_base_rate.csv"
    ),
    index=False
)

import json

evaluation_report = {
    "model": "XGBoost",
    "target": "risk_label_grid",
    "target_definition": (
        "1 if next-hour activity is above the "
        "grid-specific training-period 75th percentile"
    ),
    "prediction_horizon": "1 hour",

    "split": {
        "method": "chronological train-test split",
        "train_rows": int(len(y_train_new)),
        "test_rows": int(len(y_test_new)),
        "train_period": {
            "start": str(test_new["feature_timestamp"].min())
            if False else str(train_new["feature_timestamp"].min()),
            "end": str(train_new["feature_timestamp"].max())
        },
        "test_period": {
            "start": str(test_new["feature_timestamp"].min()),
            "end": str(test_new["feature_timestamp"].max())
        }
    },

    "class_distribution": {
        "train": {
            "positive_count": int(y_train_new.sum()),
            "total_count": int(len(y_train_new)),
            "positive_base_rate": float(y_train_new.mean())
        },
        "test": {
            "positive_count": int(y_test_new.sum()),
            "total_count": int(len(y_test_new)),
            "positive_base_rate": float(y_test_new.mean())
        }
    },

    "decision_threshold": float(FINAL_THRESHOLD),

    "metrics": {
        "accuracy": float(accuracy_score(y_test_new, xgb_final_pred)),
        "precision": float(precision_score(
            y_test_new,
            xgb_final_pred,
            zero_division=0
        )),
        "recall": float(recall_score(
            y_test_new,
            xgb_final_pred,
            zero_division=0
        )),
        "f1": float(f1_score(
            y_test_new,
            xgb_final_pred,
            zero_division=0
        )),
        "roc_auc": float(roc_auc_score(
            y_test_new,
            xgb_prob
        )),
        "average_precision": float(average_precision_score(
            y_test_new,
            xgb_prob
        ))
    },

    "confusion_matrix": {
        "true_negative": int(cm[0, 0]),
        "false_positive": int(cm[0, 1]),
        "false_negative": int(cm[1, 0]),
        "true_positive": int(cm[1, 1])
    },

    "prediction_summary": {
        "predicted_positive_count": int(xgb_final_pred.sum()),
        "predicted_positive_rate": float(xgb_final_pred.mean()),
        "actual_positive_count": int(y_test_new.sum()),
        "actual_positive_rate": float(y_test_new.mean())
    },

    "model_configuration": {
        "n_estimators": 100,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42
    }
}

with open(
    os.path.join(
        ML3_OUTPUT_DIR,
        "ml3_evaluation_report.json"
    ),
    "w",
    encoding="utf-8"
) as f:
    json.dump(
        evaluation_report,
        f,
        indent=4
    )