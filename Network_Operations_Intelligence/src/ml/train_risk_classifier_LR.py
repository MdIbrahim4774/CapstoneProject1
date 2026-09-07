"""
ML3 — Train a Simple Risk Classifier

Purpose:
    Train an interpretable Logistic Regression classifier using the
    engineered network feature table and an NP3-derived proxy risk label.

Data:
    network_feature_table
    network_alert

Important:
    - Chronological train/test split only.
    - No random splitting.
    - Features are taken only from network_feature_table.
    - NP3 alerts are converted into a binary proxy risk label.
    - This predicts the NP3 proxy label; it does NOT predict real congestion.
"""

import json
import logging
from pathlib import Path

import joblib
import mysql.connector
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    confusion_matrix,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


# ============================================================
# CONFIGURATION
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "network_operations",
}

START_DATE = "2013-11-01"
END_DATE = "2013-11-07"

TEST_RATIO = 0.20

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]

TARGET_COLUMN = "risk_label"

OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
MODEL_PATH = OUTPUT_DIR / "risk_classifier.joblib"
REPORT_PATH = OUTPUT_DIR / "ml3_evaluation_report.json"
PREDICTIONS_PATH = OUTPUT_DIR / "ml3_predictions.csv"
COMPARISON_PATH = OUTPUT_DIR / "ml3_np3_comparison.csv"


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# DATABASE
# ============================================================

def get_connection():
    """Create a MySQL connection."""
    return mysql.connector.connect(**DB_CONFIG)


# ============================================================
# LOAD FEATURES
# ============================================================

def load_feature_data():
    """
    Load engineered features from network_feature_table.

    Only the six ML2 engineered features are used.
    """

    query = """
        SELECT
            grid_id,
            feature_timestamp,
            avg_activity,
            activity_growth,
            active_hours,
            peak_ratio,
            variability,
            internet_share
        FROM network_feature_table
        ORDER BY feature_timestamp, grid_id
    """

    conn = get_connection()

    try:
        df = pd.read_sql(
            query,
            conn,
        )
    finally:
        conn.close()

    if df.empty:
        raise ValueError(
            "network_feature_table returned no rows for the requested period."
        )

    df["feature_timestamp"] = pd.to_datetime(
        df["feature_timestamp"]
    )

    logger.info("Feature rows loaded: %d", len(df))

    return df


# ============================================================
# LOAD NP3 ALERTS
# ============================================================

def load_np3_alerts():
    """
    Load NP3 alerts and convert them into a binary proxy label.

    Any NP3 alert for a grid/hour means:

        risk_label = 1

    No alert means:

        risk_label = 0

    Multiple NP3 rules firing for the same grid/hour are
    deduplicated.
    """

    query = """
        SELECT
            grid_id,
            alert_timestamp,
            alert_type
        FROM network_alert
        WHERE alert_type IN (
              'HIGH_ACTIVITY',
              'ACTIVITY_SPIKE',
              'ACTIVITY_DROP'
          )
        ORDER BY alert_timestamp, grid_id
    """

    conn = get_connection()

    try:
        alerts = pd.read_sql(
            query,
            conn,
        )
    finally:
        conn.close()

    alerts["alert_timestamp"] = pd.to_datetime(
        alerts["alert_timestamp"]
    )

    logger.info("Raw NP3 alert rows loaded: %d", len(alerts))

    # Deduplicate multiple rules firing at the same grid/hour.
    alerts = (
        alerts[
            [
                "grid_id",
                "alert_timestamp",
            ]
        ]
        .drop_duplicates()
        .copy()
    )

    alerts[TARGET_COLUMN] = 1

    logger.info(
        "Unique grid/hour NP3 risk events: %d",
        len(alerts),
    )

    return alerts


# ============================================================
# CREATE TRAINING DATASET
# ============================================================

def build_training_dataset(features, alerts):
    """
    Left join NP3 proxy labels onto the feature table.

    Every feature row becomes either:

        1 -> NP3 alert exists
        0 -> no NP3 alert
    """

    df = features.merge(
        alerts[
            [
                "grid_id",
                "alert_timestamp",
                TARGET_COLUMN,
            ]
        ],
        left_on=["grid_id", "feature_timestamp"],
        right_on=["grid_id", "alert_timestamp"],
        how="left",
    )

    df[TARGET_COLUMN] = (
        df[TARGET_COLUMN]
        .fillna(0)
        .astype(int)
    )

    df.drop(
        columns=["alert_timestamp"],
        inplace=True,
    )

    # Sort again after joining.
    df.sort_values(
        by=["feature_timestamp", "grid_id"],
        inplace=True,
    )

    df.reset_index(drop=True, inplace=True)

    return df


# ============================================================
# CLEAN FEATURES
# ============================================================

def prepare_features(df):
    """
    Handle missing feature values.

    Median imputation is performed separately inside each
    train/test split to avoid leakage.
    """

    for column in FEATURE_COLUMNS:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    before = len(df)

    # Remove rows where all ML features are missing.
    df = df.dropna(
        subset=FEATURE_COLUMNS,
        how="all",
    ).copy()

    removed = before - len(df)

    if removed:
        logger.warning(
            "Removed %d rows with all features missing.",
            removed,
        )

    return df


# ============================================================
# CHRONOLOGICAL SPLIT
# ============================================================

def chronological_split(df):
    """
    Split data chronologically.

    Earlier observations -> training
    Later observations   -> testing

    No randomization.
    """

    df = df.sort_values(
        by=["feature_timestamp", "grid_id"]
    ).reset_index(drop=True)

    split_index = int(
        len(df) * (1 - TEST_RATIO)
    )

    train_df = df.iloc[:split_index].copy()
    test_df = df.iloc[split_index:].copy()

    if train_df.empty or test_df.empty:
        raise ValueError(
            "Train or test set is empty."
        )

    logger.info(
        "Train rows: %d | Test rows: %d",
        len(train_df),
        len(test_df),
    )

    return train_df, test_df


# ============================================================
# MODEL
# ============================================================

def train_model(train_df):
    """
    Train interpretable Logistic Regression.

    StandardScaler is used because the engineered features
    have different scales.
    """

    X_train = train_df[FEATURE_COLUMNS]
    y_train = train_df[TARGET_COLUMN]

    if y_train.nunique() < 2:
        raise ValueError(
            "Training data contains only one class. "
            "Logistic Regression requires both classes."
        )

    model = Pipeline(
        steps=[
            (
                "scaler",
                StandardScaler(),
            ),
            (
                "classifier",
                LogisticRegression(
                    max_iter=1000,
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )

    model.fit(X_train, y_train)

    return model


# ============================================================
# EVALUATION
# ============================================================

def evaluate_model(model, train_df, test_df):
    """Calculate ML3 evaluation metrics."""

    X_train = train_df[FEATURE_COLUMNS]
    y_train = train_df[TARGET_COLUMN]

    X_test = test_df[FEATURE_COLUMNS]
    y_test = test_df[TARGET_COLUMN]

    # Median imputation using training medians only.
    train_medians = X_train.median()

    X_train = X_train.fillna(train_medians)
    X_test = X_test.fillna(train_medians)

    # Retrain using cleaned values.
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    accuracy = accuracy_score(
        y_test,
        predictions,
    )

    precision = precision_score(
        y_test,
        predictions,
        zero_division=0,
    )

    recall = recall_score(
        y_test,
        predictions,
        zero_division=0,
    )

    base_rate = y_test.mean()

    cm = confusion_matrix(
        y_test,
        predictions,
        labels=[0, 1],
    )

    tn, fp, fn, tp = cm.ravel()

    report = {
        "model": "Logistic Regression",
        "period": {
            "requested_start": START_DATE,
            "requested_end": END_DATE,
        },
        "train": {
            "rows": int(len(train_df)),
            "earliest_timestamp": str(
                train_df["feature_timestamp"].min()
            ),
            "latest_timestamp": str(
                train_df["feature_timestamp"].max()
            ),
            "positive_count": int(
                train_df[TARGET_COLUMN].sum()
            ),
            "negative_count": int(
                (train_df[TARGET_COLUMN] == 0).sum()
            ),
            "positive_rate": float(
                train_df[TARGET_COLUMN].mean()
            ),
        },
        "test": {
            "rows": int(len(test_df)),
            "earliest_timestamp": str(
                test_df["feature_timestamp"].min()
            ),
            "latest_timestamp": str(
                test_df["feature_timestamp"].max()
            ),
            "positive_count": int(
                test_df[TARGET_COLUMN].sum()
            ),
            "negative_count": int(
                (test_df[TARGET_COLUMN] == 0).sum()
            ),
            "positive_rate": float(base_rate),
        },
        "metrics": {
            "accuracy": float(accuracy),
            "precision": float(precision),
            "recall": float(recall),
            "base_rate": float(base_rate),
        },
        "confusion_matrix": {
            "true_negative": int(tn),
            "false_positive": int(fp),
            "false_negative": int(fn),
            "true_positive": int(tp),
        },
    }

    return model, report, predictions


# ============================================================
# COEFFICIENTS
# ============================================================

def extract_coefficients(model):
    """
    Extract Logistic Regression coefficients.

    Because features are standardized, coefficients are
    directly comparable in relative magnitude.
    """

    classifier = model.named_steps["classifier"]

    coefficients = classifier.coef_[0]

    result = []

    for feature, coefficient in zip(
        FEATURE_COLUMNS,
        coefficients,
    ):
        result.append(
            {
                "feature": feature,
                "coefficient": float(coefficient),
                "direction": (
                    "positive"
                    if coefficient > 0
                    else "negative"
                ),
            }
        )

    result.sort(
        key=lambda x: abs(x["coefficient"]),
        reverse=True,
    )

    return result


# ============================================================
# NP3 COMPARISON
# ============================================================

def compare_against_np3(
    test_df,
    predictions,
):
    """
    Compare Logistic Regression predictions against
    the NP3-derived proxy label.
    """

    comparison = test_df[
        [
            "grid_id",
            "feature_timestamp",
            TARGET_COLUMN,
        ]
    ].copy()

    comparison["ml_prediction"] = predictions

    comparison["comparison"] = "AGREEMENT"

    comparison.loc[
        (comparison[TARGET_COLUMN] == 1)
        & (comparison["ml_prediction"] == 0),
        "comparison",
    ] = "NP3_ALERT_ML_NORMAL"

    comparison.loc[
        (comparison[TARGET_COLUMN] == 0)
        & (comparison["ml_prediction"] == 1),
        "comparison",
    ] = "NP3_NORMAL_ML_ALERT"

    summary = (
        comparison["comparison"]
        .value_counts()
        .to_dict()
    )

    return comparison, summary


# ============================================================
# SAVE OUTPUTS
# ============================================================

def save_outputs(
    model,
    report,
    coefficients,
    comparison,
):
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Add coefficients to report.
    report["feature_coefficients"] = coefficients

    with open(
        REPORT_PATH,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            report,
            f,
            indent=4,
        )

    joblib.dump(
        model,
        MODEL_PATH,
    )

    comparison.to_csv(
        COMPARISON_PATH,
        index=False,
    )

    comparison[
        [
            "grid_id",
            "feature_timestamp",
            TARGET_COLUMN,
            "ml_prediction",
            "comparison",
        ]
    ].to_csv(
        PREDICTIONS_PATH,
        index=False,
    )

    logger.info(
        "Model saved: %s",
        MODEL_PATH,
    )

    logger.info(
        "Evaluation report saved: %s",
        REPORT_PATH,
    )

    logger.info(
        "Predictions saved: %s",
        PREDICTIONS_PATH,
    )

    logger.info(
        "NP3 comparison saved: %s",
        COMPARISON_PATH,
    )


# ============================================================
# MAIN
# ============================================================

def main():

    logger.info("========== ML3 START ==========")

    # 1. Load ML2 features.
    features = load_feature_data()

    # 2. Load NP3 alerts.
    alerts = load_np3_alerts()

    # 3. Create proxy risk label.
    df = build_training_dataset(
        features,
        alerts,
    )

    # 4. Clean feature values.
    df = prepare_features(df)

    # 5. Chronological split.
    train_df, test_df = chronological_split(df)

    # 6. Train model.
    model = train_model(train_df)

    # 7. Evaluate.
    model, report, predictions = evaluate_model(
        model,
        train_df,
        test_df,
    )

    # 8. Coefficients.
    coefficients = extract_coefficients(
        model
    )

    # 9. Compare against NP3.
    comparison, comparison_summary = (
        compare_against_np3(
            test_df,
            predictions,
        )
    )

    report["np3_comparison"] = {
        "summary": {
            key: int(value)
            for key, value in comparison_summary.items()
        }
    }

    # 10. Save.
    save_outputs(
        model,
        report,
        coefficients,
        comparison,
    )

    # 11. Console summary.
    print("\n========== ML3 RESULTS ==========")

    print("\nTRAIN")
    print(
        f"Earliest: "
        f"{report['train']['earliest_timestamp']}"
    )
    print(
        f"Latest:   "
        f"{report['train']['latest_timestamp']}"
    )
    print(
        f"Rows:     "
        f"{report['train']['rows']}"
    )
    print(
        f"Base rate:"
        f" {report['train']['positive_rate']:.4f}"
    )

    print("\nTEST")
    print(
        f"Earliest: "
        f"{report['test']['earliest_timestamp']}"
    )
    print(
        f"Latest:   "
        f"{report['test']['latest_timestamp']}"
    )
    print(
        f"Rows:     "
        f"{report['test']['rows']}"
    )
    print(
        f"Base rate:"
        f" {report['test']['positive_rate']:.4f}"
    )

    print("\nMETRICS")

    metrics = report["metrics"]

    print(
        f"Accuracy:  {metrics['accuracy']:.4f}"
    )
    print(
        f"Precision: {metrics['precision']:.4f}"
    )
    print(
        f"Recall:    {metrics['recall']:.4f}"
    )
    print(
        f"Base rate: {metrics['base_rate']:.4f}"
    )

    print("\nFEATURE COEFFICIENTS")

    for item in coefficients:
        print(
            f"{item['feature']:20s}"
            f" {item['coefficient']:+.4f}"
            f" ({item['direction']})"
        )

    print("\nNP3 VS ML")

    for key, value in comparison_summary.items():
        print(
            f"{key:25s}: {value}"
        )

    print("\n========== ML3 COMPLETE ==========")


if __name__ == "__main__":
    main()