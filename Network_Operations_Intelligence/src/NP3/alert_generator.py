import logging
from pathlib import Path

import pandas as pd
import mysql.connector
from mysql.connector import Error


# ---------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Rule thresholds
# ---------------------------------------------------------
HIGH_ACTIVITY_MULTIPLIER = 2.0
SPIKE_MULTIPLIER = 1.5
DROP_MULTIPLIER = 0.5


# ---------------------------------------------------------
# Input / Output
# ---------------------------------------------------------
INPUT_FILE = Path(
    "output/grid_hour_summary.csv"
)

OUTPUT_FILE = Path(
    "output/network_alerts.csv"
)


# ---------------------------------------------------------
# MySQL configuration
# ---------------------------------------------------------
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "network_operations",
    "user": "root",
    "password": "root",
}


# ---------------------------------------------------------
# MySQL table
# ---------------------------------------------------------
CREATE_ALERT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS network_alert (
    alert_id BIGINT NOT NULL AUTO_INCREMENT,
    grid_id VARCHAR(50) NOT NULL,
    alert_timestamp DATETIME NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    current_activity DOUBLE NOT NULL,
    baseline_activity DOUBLE NULL,
    reason VARCHAR(500) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (alert_id),

    UNIQUE KEY uq_network_alert (
        grid_id,
        alert_timestamp,
        alert_type
    ),

    INDEX idx_network_alert_grid (
        grid_id
    ),

    INDEX idx_network_alert_timestamp (
        alert_timestamp
    ),

    INDEX idx_network_alert_type (
        alert_type
    ),

    INDEX idx_network_alert_grid_timestamp (
        grid_id,
        alert_timestamp
    )
)
"""


# ---------------------------------------------------------
# Insert / Upsert SQL
# ---------------------------------------------------------
INSERT_ALERT_SQL = """
INSERT INTO network_alert (
    grid_id,
    alert_timestamp,
    alert_type,
    current_activity,
    baseline_activity,
    reason
)
VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
)
ON DUPLICATE KEY UPDATE
    current_activity = VALUES(current_activity),
    baseline_activity = VALUES(baseline_activity),
    reason = VALUES(reason)
"""


# ---------------------------------------------------------
# Load NP2 output
# ---------------------------------------------------------
def load_np2_output(filepath):
    """Load hourly analytics produced by NP2."""

    logger.info(
        "Loading NP2 output: %s",
        filepath
    )

    df = pd.read_csv(filepath)

    required_columns = {
        "grid_id",
        "hour_timestamp",
        "total_activity",
    }

    missing_columns = (
        required_columns - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Missing required columns: "
            f"{sorted(missing_columns)}"
        )

    df["hour_timestamp"] = pd.to_datetime(
        df["hour_timestamp"]
    )

    df = df.sort_values(
        ["grid_id", "hour_timestamp"]
    ).reset_index(drop=True)

    return df


# ---------------------------------------------------------
# Activity floor
# ---------------------------------------------------------
def calculate_activity_floor(df):
    """
    Use the 25th percentile of grid-level daily
    activity totals as the activity floor.
    """

    daily_grid_totals = (
        df.groupby("grid_id")["total_activity"]
        .sum()
    )

    activity_floor = (
        daily_grid_totals.quantile(0.25)
    )

    logger.info(
        "Activity floor: %.2f",
        activity_floor
    )

    return activity_floor


# ---------------------------------------------------------
# Within-day baseline
# ---------------------------------------------------------
def calculate_baseline(group):
    """
    Calculate the leave-one-hour-out median.

    The current hour is excluded from its own baseline.
    """

    group = group.copy()

    activities = (
        group["total_activity"]
        .to_numpy()
    )

    baselines = []

    for index in range(len(activities)):

        other_hours = [
            value
            for i, value in enumerate(activities)
            if i != index
        ]

        if other_hours:
            baseline = pd.Series(
                other_hours
            ).median()
        else:
            baseline = None

        baselines.append(baseline)

    group["baseline_activity"] = baselines

    return group


def add_baselines(df):
    """
    Add leave-one-hour-out baseline
    for each grid and calendar day.

    The current hour is excluded from
    its own baseline.
    """

    logger.info(
        "Calculating within-day baselines"
    )

    df = df.copy()

    # Ensure timestamp is datetime
    df["hour_timestamp"] = pd.to_datetime(
        df["hour_timestamp"]
    )

    # Temporary date used only for grouping
    df["_baseline_date"] = (
        df["hour_timestamp"].dt.date
    )

    results = []

    for (grid_id, baseline_date), group in df.groupby(
        ["grid_id", "_baseline_date"],
        sort=False
    ):

        logger.debug(
            "Calculating baseline for grid=%s date=%s",
            grid_id,
            baseline_date
        )

        group = (
            group
            .sort_values("hour_timestamp")
            .reset_index(drop=True)
        )

        group = calculate_baseline(group)

        results.append(group)

    if results:

        result = pd.concat(
            results,
            ignore_index=True
        )

    else:

        result = df.copy()

        result["baseline_activity"] = None

    # Remove temporary column safely
    result = result.drop(
        columns=["_baseline_date"],
        errors="ignore"
    )

    # Restore consistent ordering
    result = result.sort_values(
        ["grid_id", "hour_timestamp"]
    ).reset_index(drop=True)

    logger.info(
        "Columns after baseline calculation: %s",
        result.columns.tolist()
    )

    return result


# ---------------------------------------------------------
# Generate alerts
# ---------------------------------------------------------
def generate_alerts(df, activity_floor):
    """
    Apply the three NP3 rules.

    HIGH_ACTIVITY:
        current vs baseline

    ACTIVITY_SPIKE:
        current vs previous hour

    ACTIVITY_DROP:
        current vs baseline
    """

    logger.info(
        "Generating network alerts: %s", df.columns.tolist()
    )

    alerts = []

    for grid_id, group in df.groupby("grid_id"):

        group = (
            group
            .sort_values("hour_timestamp")
            .reset_index(drop=True)
        )

        # ---------------------------------------------
        # Activity floor
        # ---------------------------------------------
        daily_total = (
            group["total_activity"].sum()
        )

        if daily_total < activity_floor:
            continue

        # ---------------------------------------------
        # Evaluate every hour
        # ---------------------------------------------
        for index, row in group.iterrows():

            current_activity = (
                row["total_activity"]
            )

            baseline_activity = (
                row["baseline_activity"]
            )

            if pd.isna(baseline_activity):
                continue

            # -----------------------------------------
            # Previous hour
            # -----------------------------------------
            previous_activity = None

            if index > 0:
                previous_activity = (
                    group.loc[
                        index - 1,
                        "total_activity"
                    ]
                )

            # -----------------------------------------
            # HIGH_ACTIVITY
            #
            # Current vs baseline
            # -----------------------------------------
            if (
                baseline_activity > 0
                and current_activity
                >= baseline_activity
                * HIGH_ACTIVITY_MULTIPLIER
            ):

                alerts.append({
                    "grid_id": grid_id,
                    "timestamp": row["hour_timestamp"],
                    "alert_type": "HIGH_ACTIVITY",
                    "current_activity": current_activity,
                    "baseline_activity": baseline_activity,
                    "reason": (
                        "Current activity is at least "
                        "2x the within-day baseline."
                    ),
                })

            # -----------------------------------------
            # ACTIVITY_SPIKE
            #
            # Current vs previous hour ONLY
            # -----------------------------------------
            if (
                previous_activity is not None
                and previous_activity > 0
                and current_activity
                >= previous_activity
                * SPIKE_MULTIPLIER
            ):

                alerts.append({
                    "grid_id": grid_id,
                    "timestamp": row["hour_timestamp"],
                    "alert_type": "ACTIVITY_SPIKE",
                    "current_activity": current_activity,
                    "baseline_activity": baseline_activity,
                    "reason": (
                        "Current activity increased by "
                        "at least 50% from the "
                        "previous hour."
                    ),
                })

            # -----------------------------------------
            # ACTIVITY_DROP
            #
            # Current vs baseline
            # -----------------------------------------
            if (
                baseline_activity > 0
                and current_activity
                <= baseline_activity
                * DROP_MULTIPLIER
            ):

                alerts.append({
                    "grid_id": grid_id,
                    "timestamp": row["hour_timestamp"],
                    "alert_type": "ACTIVITY_DROP",
                    "current_activity": current_activity,
                    "baseline_activity": baseline_activity,
                    "reason": (
                        "Current activity is at or below "
                        "50% of the within-day baseline."
                    ),
                })

    return pd.DataFrame(alerts)


# ---------------------------------------------------------
# Create MySQL connection
# ---------------------------------------------------------
def get_mysql_connection():
    """Create and return a MySQL database connection."""

    logger.info(
        "Connecting to MySQL database: %s",
        MYSQL_CONFIG["database"]
    )

    try:
        connection = mysql.connector.connect(
            host=MYSQL_CONFIG["host"],
            port=MYSQL_CONFIG["port"],
            database=MYSQL_CONFIG["database"],
            user=MYSQL_CONFIG["user"],
            password=MYSQL_CONFIG["password"],
        )

        if connection.is_connected():
            logger.info(
                "Successfully connected to MySQL"
            )

        return connection

    except Error as exc:
        logger.error(
            "MySQL connection failed: %s",
            exc
        )
        raise


# ---------------------------------------------------------
# Create network_alert table
# ---------------------------------------------------------
def create_alert_table(connection):
    """Create network_alert table if it does not exist."""

    logger.info(
        "Checking network_alert table"
    )

    cursor = connection.cursor()

    try:
        cursor.execute(
            CREATE_ALERT_TABLE_SQL
        )

        connection.commit()

        logger.info(
            "network_alert table is ready"
        )

    finally:
        cursor.close()


# ---------------------------------------------------------
# Load alerts into MySQL
# ---------------------------------------------------------
def load_alerts_to_mysql(alerts):
    """
    Insert generated alerts into MySQL.

    Duplicate grid_id + timestamp + alert_type
    combinations are updated instead of inserted again.
    """

    if alerts.empty:
        logger.info(
            "No alerts to load into MySQL"
        )
        return

    connection = None
    cursor = None

    try:

        connection = get_mysql_connection()

        create_alert_table(connection)

        cursor = connection.cursor()

        records = []

        for _, row in alerts.iterrows():

            alert_timestamp = row["timestamp"]

            # Convert pandas Timestamp to Python datetime
            if pd.notna(alert_timestamp):
                alert_timestamp = (
                    alert_timestamp.to_pydatetime()
                )

            baseline_activity = (
                None
                if pd.isna(row["baseline_activity"])
                else float(row["baseline_activity"])
            )

            records.append((
                str(row["grid_id"]),
                alert_timestamp,
                str(row["alert_type"]),
                float(row["current_activity"]),
                baseline_activity,
                str(row["reason"]),
            ))

        cursor.executemany(
            INSERT_ALERT_SQL,
            records
        )

        connection.commit()

        logger.info(
            "Loaded %d alerts into network_alert",
            len(records)
        )

    except Error as exc:

        if connection is not None:
            connection.rollback()

        logger.error(
            "Failed to load alerts into MySQL: %s",
            exc
        )

        raise

    finally:

        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()

            logger.info(
                "MySQL connection closed"
            )


# ---------------------------------------------------------
# Operational summary
# ---------------------------------------------------------
def print_operational_summary(
    alerts,
    total_grid_hours
):
    """Print the required NP3 operational summary."""

    print(
        "\n========== NETWORK ALERT SUMMARY ==========\n"
    )

    if alerts.empty:

        print("No alerts generated.")

        return

    # ---------------------------------------------
    # Alerts by type
    # ---------------------------------------------
    print("Alerts by type:")

    print(
        alerts["alert_type"]
        .value_counts()
        .to_string()
    )

    # ---------------------------------------------
    # Top 10 grids
    # ---------------------------------------------
    print("\nTop 10 grids by alert count:")

    print(
        alerts["grid_id"]
        .value_counts()
        .head(10)
        .to_string()
    )

    # ---------------------------------------------
    # Unique grid/hour combinations
    # ---------------------------------------------
    alerting_grid_hours = (
        alerts[
            ["grid_id", "timestamp"]
        ]
        .drop_duplicates()
        .shape[0]
    )

    # ---------------------------------------------
    # Proportion
    # ---------------------------------------------
    if total_grid_hours > 0:

        proportion = (
            alerting_grid_hours
            / total_grid_hours
        )

        print(
            f"\nProportion of all grid/hours "
            f"that alerted: {proportion:.2%}"
        )

    print(
        "\n===========================================\n"
    )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
def main():

    try:

        # -----------------------------------------
        # 1. Load NP2 hourly output
        # -----------------------------------------
        df = load_np2_output(
            INPUT_FILE
        )

        logger.info(
            "Loaded %d grid/hour records",
            len(df)
        )

        # -----------------------------------------
        # 2. Calculate data-driven activity floor
        # -----------------------------------------
        activity_floor = (
            calculate_activity_floor(df)
        )

        # -----------------------------------------
        # 3. Calculate leave-one-hour-out baseline
        # -----------------------------------------
        df = add_baselines(df)

        # -----------------------------------------
        # 4. Generate alerts
        # -----------------------------------------
        alerts = generate_alerts(
            df,
            activity_floor
        )

        logger.info(
            "Generated %d alerts",
            len(alerts)
        )

        # -----------------------------------------
        # 5. Create output directory
        # -----------------------------------------
        OUTPUT_FILE.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        # -----------------------------------------
        # 6. Save CSV
        # -----------------------------------------
        alerts.to_csv(
            OUTPUT_FILE,
            index=False
        )

        logger.info(
            "Saved %d alerts to %s",
            len(alerts),
            OUTPUT_FILE
        )

        # -----------------------------------------
        # 7. Load alerts into MySQL
        # -----------------------------------------
        load_alerts_to_mysql(
            alerts
        )

        # -----------------------------------------
        # 8. Total grid/hour intervals
        # -----------------------------------------
        total_grid_hours = (
            df[
                [
                    "grid_id",
                    "hour_timestamp"
                ]
            ]
            .drop_duplicates()
            .shape[0]
        )

        # -----------------------------------------
        # 9. Operational summary
        # -----------------------------------------
        print_operational_summary(
            alerts,
            total_grid_hours
        )

        logger.info(
            "NP3 completed successfully"
        )

    except Exception as exc:

        logger.exception(
            "NP3 failed: %s",
            exc
        )

        raise


# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------
if __name__ == "__main__":
    main()