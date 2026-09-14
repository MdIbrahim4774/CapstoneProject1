"""
ML3 — Train a Simple Risk Classifier using Decision Tree

Purpose:
    Train an interpretable Decision Tree classifier using the
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

from sklearn.tree import DecisionTreeClassifier


from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    confusion_matrix,
)


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

# Dataset period
START_DATE = "2013-11-01"
END_DATE = "2013-11-07"

# Chronological split
TEST_RATIO = 0.20

# ML2 engineered features
FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]

TARGET_COLUMN = "risk_label"


# ============================================================
# OUTPUT PATHS
# ============================================================

OUTPUT_DIR = Path(__file__).resolve().parent / "dt_outputs"

MODEL_PATH = (
    OUTPUT_DIR / "risk_classifier.joblib"
)

REPORT_PATH = (
    OUTPUT_DIR / "ml3_evaluation_report.json"
)

PREDICTIONS_PATH = (
    OUTPUT_DIR / "ml3_predictions.csv"
)

COMPARISON_PATH = (
    OUTPUT_DIR / "ml3_np3_comparison.csv"
)


# ============================================================
# DECISION TREE CONFIGURATION
# ============================================================

TREE_MAX_DEPTH = 5

TREE_MIN_SAMPLES_LEAF = 50

TREE_RANDOM_STATE = 42


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():
    """
    Create a MySQL connection.
    """

    return mysql.connector.connect(
        **DB_CONFIG
    )


# ============================================================
# LOAD ML2 FEATURES
# ============================================================

def load_feature_data():
    """
    Load engineered features from network_feature_table.

    These are the only features used by the ML model.
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
        ORDER BY
            feature_timestamp,
            grid_id
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
            "network_feature_table returned "
            "no rows for the requested period."
        )

    df["feature_timestamp"] = pd.to_datetime(
        df["feature_timestamp"]
    )

    logger.info(
        "Feature rows loaded: %d",
        len(df),
    )

    return df


# ============================================================
# LOAD NP3 ALERTS
# ============================================================

def load_np3_alerts():
    """
    Load NP3 alerts.

    Any NP3 alert for a grid/hour becomes:

        risk_label = 1

    No NP3 alert becomes:

        risk_label = 0

    Multiple rules firing for the same grid/hour
    are deduplicated.
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
        ORDER BY
            alert_timestamp,
            grid_id
    """

    conn = get_connection()

    try:

        alerts = pd.read_sql(
            query,
            conn,
        )

    finally:

        conn.close()

    if alerts.empty:

        raise ValueError(
            "network_alert returned no NP3 alerts."
        )

    alerts["alert_timestamp"] = pd.to_datetime(
        alerts["alert_timestamp"]
    )

    logger.info(
        "Raw NP3 alert rows loaded: %d",
        len(alerts),
    )

    # --------------------------------------------------------
    # Deduplicate multiple rules for the same grid/hour
    # --------------------------------------------------------

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
        "Unique NP3 risk events: %d",
        len(alerts),
    )

    return alerts


# ============================================================
# BUILD ML DATASET
# ============================================================

def build_training_dataset(
    features,
    alerts,
):
    """
    Join NP3 proxy labels onto the ML2 feature table.

    Feature row + NP3 alert
        -> risk_label = 1

    Feature row + no NP3 alert
        -> risk_label = 0
    """

    df = features.merge(
        alerts[
            [
                "grid_id",
                "alert_timestamp",
                TARGET_COLUMN,
            ]
        ],
        left_on=[
            "grid_id",
            "feature_timestamp",
        ],
        right_on=[
            "grid_id",
            "alert_timestamp",
        ],
        how="left",
    )

    # No alert = normal
    df[TARGET_COLUMN] = (
        df[TARGET_COLUMN]
        .fillna(0)
        .astype(int)
    )

    # alert_timestamp is no longer required
    df.drop(
        columns=[
            "alert_timestamp"
        ],
        inplace=True,
    )

    # Chronological ordering
    df.sort_values(
        by=[
            "feature_timestamp",
            "grid_id",
        ],
        inplace=True,
    )

    df.reset_index(
        drop=True,
        inplace=True,
    )

    logger.info(
        "Final ML dataset rows: %d",
        len(df),
    )

    logger.info(
        "Positive labels: %d",
        df[TARGET_COLUMN].sum(),
    )

    logger.info(
        "Negative labels: %d",
        (
            df[TARGET_COLUMN] == 0
        ).sum(),
    )

    logger.info(
        "Overall base rate: %.4f",
        df[TARGET_COLUMN].mean(),
    )

    return df


# ============================================================
# PREPARE FEATURES
# ============================================================

def prepare_features(df):
    """
    Convert ML features to numeric values.

    Decision Trees cannot directly process NaN values in
    this implementation, so missing values are handled
    using training-set medians later.
    """

    df = df.copy()

    for column in FEATURE_COLUMNS:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    # Remove rows where ALL ML features are missing.
    before = len(df)

    df.dropna(
        subset=FEATURE_COLUMNS,
        how="all",
        inplace=True,
    )

    removed = (
        before - len(df)
    )

    if removed > 0:

        logger.warning(
            "Removed %d rows where all "
            "features were missing.",
            removed,
        )

    return df


# ============================================================
# CHRONOLOGICAL TRAIN / TEST SPLIT
# ============================================================

def chronological_split(df):
    """
    Perform a chronological train/test split.

    Earlier observations:
        TRAIN

    Later observations:
        TEST

    No randomization.
    """

    df = df.sort_values(
        by=[
            "feature_timestamp",
            "grid_id",
        ]
    ).reset_index(
        drop=True
    )

    split_index = int(
        len(df) * (
            1 - TEST_RATIO
        )
    )

    train_df = df.iloc[
        :split_index
    ].copy()

    test_df = df.iloc[
        split_index:
    ].copy()

    if train_df.empty:

        raise ValueError(
            "Training set is empty."
        )

    if test_df.empty:

        raise ValueError(
            "Test set is empty."
        )

    logger.info(
        "Training rows: %d",
        len(train_df),
    )

    logger.info(
        "Testing rows: %d",
        len(test_df),
    )

    logger.info(
        "Train earliest: %s",
        train_df[
            "feature_timestamp"
        ].min(),
    )

    logger.info(
        "Train latest: %s",
        train_df[
            "feature_timestamp"
        ].max(),
    )

    logger.info(
        "Test earliest: %s",
        test_df[
            "feature_timestamp"
        ].min(),
    )

    logger.info(
        "Test latest: %s",
        test_df[
            "feature_timestamp"
        ].max(),
    )

    return (
        train_df,
        test_df,
    )


# ============================================================
# IMPUTE MISSING VALUES
# ============================================================

def prepare_train_test_features(
    train_df,
    test_df,
):
    """
    Fill missing values using medians calculated
    ONLY from the training set.

    This prevents test-set information from leaking
    into training.
    """

    X_train = train_df[
        FEATURE_COLUMNS
    ].copy()

    X_test = test_df[
        FEATURE_COLUMNS
    ].copy()

    # Calculate medians from TRAIN ONLY
    train_medians = X_train.median()

    X_train = X_train.fillna(
        train_medians
    )

    X_test = X_test.fillna(
        train_medians
    )

    return (
        X_train,
        X_test,
    )


# ============================================================
# TRAIN DECISION TREE
# ============================================================

def train_model(
    X_train,
    y_train,
):
    """
    Train an interpretable Decision Tree.

    max_depth prevents the tree from becoming unnecessarily
    complex and easier to explain operationally.
    """

    if y_train.nunique() < 2:

        raise ValueError(
            "Training data contains only one class. "
            "Decision Tree requires both classes."
        )

    model = DecisionTreeClassifier(
        max_depth=TREE_MAX_DEPTH,
        min_samples_leaf=TREE_MIN_SAMPLES_LEAF,
        random_state=TREE_RANDOM_STATE,
        class_weight="balanced",
    )

    model.fit(
        X_train,
        y_train,
    )

    logger.info(
        "Decision Tree trained."
    )

    logger.info(
        "Tree depth: %d",
        model.get_depth(),
    )

    logger.info(
        "Number of leaves: %d",
        model.get_n_leaves(),
    )

    return model


# ============================================================
# EVALUATE MODEL
# ============================================================

def evaluate_model(
    model,
    X_test,
    y_test,
    train_df,
    test_df,
):
    """
    Calculate ML3 evaluation metrics.
    """

    predictions = model.predict(
        X_test
    )

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

    # Positive-label proportion
    base_rate = y_test.mean()

    # Confusion matrix
    cm = confusion_matrix(
        y_test,
        predictions,
        labels=[0, 1],
    )

    tn, fp, fn, tp = cm.ravel()

    report = {

        "model": "Decision Tree",

        "model_parameters": {
            "max_depth": TREE_MAX_DEPTH,
            "min_samples_leaf": (
                TREE_MIN_SAMPLES_LEAF
            ),
            "class_weight": "balanced",
            "random_state": (
                TREE_RANDOM_STATE
            ),
        },

        "period": {
            "requested_start": START_DATE,
            "requested_end": END_DATE,
        },

        "train": {

            "rows": int(
                len(train_df)
            ),

            "earliest_timestamp": str(
                train_df[
                    "feature_timestamp"
                ].min()
            ),

            "latest_timestamp": str(
                train_df[
                    "feature_timestamp"
                ].max()
            ),

            "positive_count": int(
                train_df[
                    TARGET_COLUMN
                ].sum()
            ),

            "negative_count": int(
                (
                    train_df[
                        TARGET_COLUMN
                    ] == 0
                ).sum()
            ),

            "positive_rate": float(
                train_df[
                    TARGET_COLUMN
                ].mean()
            ),
        },

        "test": {

            "rows": int(
                len(test_df)
            ),

            "earliest_timestamp": str(
                test_df[
                    "feature_timestamp"
                ].min()
            ),

            "latest_timestamp": str(
                test_df[
                    "feature_timestamp"
                ].max()
            ),

            "positive_count": int(
                test_df[
                    TARGET_COLUMN
                ].sum()
            ),

            "negative_count": int(
                (
                    test_df[
                        TARGET_COLUMN
                    ] == 0
                ).sum()
            ),

            "positive_rate": float(
                base_rate
            ),
        },

        "metrics": {

            "accuracy": float(
                accuracy
            ),

            "precision": float(
                precision
            ),

            "recall": float(
                recall
            ),

            "base_rate": float(
                base_rate
            ),
        },

        "confusion_matrix": {

            "true_negative": int(
                tn
            ),

            "false_positive": int(
                fp
            ),

            "false_negative": int(
                fn
            ),

            "true_positive": int(
                tp
            ),
        },
    }

    return (
        report,
        predictions,
    )


# ============================================================
# FEATURE IMPORTANCE
# ============================================================

def extract_feature_importances(
    model,
):
    """
    Extract Decision Tree feature importances.

    Higher value means the feature contributed more
    to the tree's decision process.
    """

    importances = (
        model.feature_importances_
    )

    result = []

    for feature, importance in zip(
        FEATURE_COLUMNS,
        importances,
    ):

        result.append(
            {
                "feature": feature,
                "importance": float(
                    importance
                ),
            }
        )

    result.sort(
        key=lambda x: x[
            "importance"
        ],
        reverse=True,
    )

    return result


# ============================================================
# NP3 VS ML COMPARISON
# ============================================================

def compare_against_np3(
    test_df,
    predictions,
):
    """
    Compare Decision Tree predictions against
    the NP3-derived proxy label.
    """

    comparison = test_df[
        [
            "grid_id",
            "feature_timestamp",
            TARGET_COLUMN,
        ]
    ].copy()

    comparison[
        "ml_prediction"
    ] = predictions

    comparison[
        "comparison"
    ] = "AGREEMENT"

    # NP3 alert but ML predicts normal
    comparison.loc[
        (
            comparison[
                TARGET_COLUMN
            ] == 1
        )
        &
        (
            comparison[
                "ml_prediction"
            ] == 0
        ),
        "comparison",
    ] = "NP3_ALERT_ML_NORMAL"

    # NP3 normal but ML predicts alert
    comparison.loc[
        (
            comparison[
                TARGET_COLUMN
            ] == 0
        )
        &
        (
            comparison[
                "ml_prediction"
            ] == 1
        ),
        "comparison",
    ] = "NP3_NORMAL_ML_ALERT"

    summary = (
        comparison[
            "comparison"
        ]
        .value_counts()
        .to_dict()
    )

    return (
        comparison,
        summary,
    )


# ============================================================
# SAVE OUTPUTS
# ============================================================

def save_outputs(
    model,
    report,
    feature_importances,
    comparison,
):
    """
    Save model, evaluation report and predictions.
    """

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Add feature importance to report
    report[
        "feature_importances"
    ] = feature_importances

    # --------------------------------------------------------
    # Save model
    # --------------------------------------------------------

    joblib.dump(
        model,
        MODEL_PATH,
    )

    # --------------------------------------------------------
    # Save JSON report
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Save comparison
    # --------------------------------------------------------

    comparison.to_csv(
        COMPARISON_PATH,
        index=False,
    )

    # --------------------------------------------------------
    # Save predictions
    # --------------------------------------------------------

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

    logger.info(
        "========== ML3 START =========="
    )

    # --------------------------------------------------------
    # 1. Load ML2 features
    # --------------------------------------------------------

    features = load_feature_data()

    # --------------------------------------------------------
    # 2. Load NP3 alerts
    # --------------------------------------------------------

    alerts = load_np3_alerts()

    # --------------------------------------------------------
    # 3. Build proxy risk label
    # --------------------------------------------------------

    df = build_training_dataset(
        features,
        alerts,
    )

    # --------------------------------------------------------
    # 4. Prepare features
    # --------------------------------------------------------

    df = prepare_features(
        df
    )

    # --------------------------------------------------------
    # 5. Chronological split
    # --------------------------------------------------------

    (
        train_df,
        test_df,
    ) = chronological_split(
        df
    )

    # --------------------------------------------------------
    # 6. Prepare train/test feature matrices
    # --------------------------------------------------------

    (
        X_train,
        X_test,
    ) = prepare_train_test_features(
        train_df,
        test_df,
    )

    y_train = train_df[
        TARGET_COLUMN
    ]

    y_test = test_df[
        TARGET_COLUMN
    ]

    # --------------------------------------------------------
    # 7. Train Decision Tree
    # --------------------------------------------------------

    model = train_model(
        X_train,
        y_train,
    )

    # --------------------------------------------------------
    # 8. Evaluate
    # --------------------------------------------------------

    (
        report,
        predictions,
    ) = evaluate_model(
        model,
        X_test,
        y_test,
        train_df,
        test_df,
    )

    # --------------------------------------------------------
    # 9. Feature importance
    # --------------------------------------------------------

    feature_importances = (
        extract_feature_importances(
            model
        )
    )

    # --------------------------------------------------------
    # 10. Compare with NP3
    # --------------------------------------------------------

    (
        comparison,
        comparison_summary,
    ) = compare_against_np3(
        test_df,
        predictions,
    )

    report[
        "np3_comparison"
    ] = {

        "summary": {
            key: int(value)
            for key, value
            in comparison_summary.items()
        }
    }

    # --------------------------------------------------------
    # 11. Save everything
    # --------------------------------------------------------

    save_outputs(
        model,
        report,
        feature_importances,
        comparison,
    )

    # --------------------------------------------------------
    # 12. Console report
    # --------------------------------------------------------

    print(
        "\n========== ML3 RESULTS =========="
    )

    # --------------------------------------------------------
    # TRAIN
    # --------------------------------------------------------

    print("\nTRAIN")

    print(
        "Earliest:",
        report[
            "train"
        ][
            "earliest_timestamp"
        ],
    )

    print(
        "Latest:  ",
        report[
            "train"
        ][
            "latest_timestamp"
        ],
    )

    print(
        "Rows:    ",
        report[
            "train"
        ][
            "rows"
        ],
    )

    print(
        "Positive:",
        report[
            "train"
        ][
            "positive_count"
        ],
    )

    print(
        "Negative:",
        report[
            "train"
        ][
            "negative_count"
        ],
    )

    print(
        "Base rate:",
        f"{report['train']['positive_rate']:.4f}",
    )

    # --------------------------------------------------------
    # TEST
    # --------------------------------------------------------

    print("\nTEST")

    print(
        "Earliest:",
        report[
            "test"
        ][
            "earliest_timestamp"
        ],
    )

    print(
        "Latest:  ",
        report[
            "test"
        ][
            "latest_timestamp"
        ],
    )

    print(
        "Rows:    ",
        report[
            "test"
        ][
            "rows"
        ],
    )

    print(
        "Positive:",
        report[
            "test"
        ][
            "positive_count"
        ],
    )

    print(
        "Negative:",
        report[
            "test"
        ][
            "negative_count"
        ],
    )

    print(
        "Base rate:",
        f"{report['test']['positive_rate']:.4f}",
    )

    # --------------------------------------------------------
    # METRICS
    # --------------------------------------------------------

    print("\nMETRICS")

    metrics = report[
        "metrics"
    ]

    print(
        "Accuracy: ",
        f"{metrics['accuracy']:.4f}",
    )

    print(
        "Precision:",
        f"{metrics['precision']:.4f}",
    )

    print(
        "Recall:   ",
        f"{metrics['recall']:.4f}",
    )

    print(
        "Base rate:",
        f"{metrics['base_rate']:.4f}",
    )

    # --------------------------------------------------------
    # CONFUSION MATRIX
    # --------------------------------------------------------

    print(
        "\nCONFUSION MATRIX"
    )

    cm = report[
        "confusion_matrix"
    ]

    print(
        "True Negatives :",
        cm["true_negative"],
    )

    print(
        "False Positives:",
        cm["false_positive"],
    )

    print(
        "False Negatives:",
        cm["false_negative"],
    )

    print(
        "True Positives :",
        cm["true_positive"],
    )

    # --------------------------------------------------------
    # TREE
    # --------------------------------------------------------

    print(
        "\nTREE"
    )

    print(
        "Depth:",
        model.get_depth(),
    )

    print(
        "Leaves:",
        model.get_n_leaves(),
    )

    # --------------------------------------------------------
    # FEATURE IMPORTANCE
    # --------------------------------------------------------

    print(
        "\nFEATURE IMPORTANCE"
    )

    for item in feature_importances:

        print(
            f"{item['feature']:20s}"
            f" {item['importance']:.4f}"
        )

    # --------------------------------------------------------
    # NP3 COMPARISON
    # --------------------------------------------------------

    print(
        "\nNP3 VS ML"
    )

    for key, value in (
        comparison_summary.items()
    ):

        print(
            f"{key:25s}: {value}"
        )

    print(
        "\n========== ML3 COMPLETE =========="
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()