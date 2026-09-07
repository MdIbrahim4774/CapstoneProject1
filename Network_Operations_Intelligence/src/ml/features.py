"""
ML2 - Network Activity Feature Engineering

Network Operations Intelligence

Purpose
-------
Create a minimal, leakage-safe feature set for an operational
risk / anomaly model.

Feature convention
------------------
For a prediction about interval t+1:

    feature_timestamp = t

Every feature is calculated using only data available at or
before t.

Recent window:
    t-23 hours ... t

Prior baseline window:
    t-47 hours ... t-24 hours

Therefore:
    NOTHING after t may influence the feature row at t.

Output
------
MySQL table:

    network_feature_table
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Iterable

import mysql.connector
from mysql.connector import Error

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, TimestampNTZType


# ============================================================
# Windows / Hadoop / Java configuration
# ============================================================

os.environ["HADOOP_HOME"] = (
    r"C:\Users\ibrahim.m\Documents\Notes\hadoop"
)

os.environ["JAVA_HOME"] = (
    r"C:\Program Files\Java\jdk-17"
)

os.environ["PATH"] = (
    os.environ["PATH"]
    + r";C:\Users\ibrahim.m\Documents\Notes\hadoop\bin"
)


# ============================================================
# Configuration
# ============================================================

RECENT_WINDOW_HOURS = 24
BASELINE_WINDOW_HOURS = 24

DEFAULT_INPUT = (
    "output/SP3/hourly_grid_summary"
)

DEFAULT_DATABASE = "network_operations"
DEFAULT_TABLE = "network_feature_table"


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    ),
)

logger = logging.getLogger(
    "ml2_feature_engineering"
)


# ============================================================
# Required columns
# ============================================================

REQUIRED_COLUMNS = {
    "grid_id",
    "timestamp",
    "total_activity",
    "internet",
}


FEATURE_COLUMNS = [
    "grid_id",
    "feature_timestamp",
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]


# ============================================================
# Spark
# ============================================================

def create_spark_session() -> SparkSession:
    """
    Create SparkSession for ML2.
    """

    logger.info(
        "Creating SparkSession."
    )

    spark = (
        SparkSession.builder
        .appName(
            "NetworkOperations-ML2-Features"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "50",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    logger.info(
        "Spark version: %s",
        spark.version,
    )

    return spark


# ============================================================
# Input validation
# ============================================================

def validate_input(df: DataFrame) -> None:
    """
    Validate the SP3 hourly grid summary.
    """

    missing = (
        REQUIRED_COLUMNS
        - set(df.columns)
    )

    if missing:
        raise ValueError(
            "ML2 input is missing required columns: "
            + ", ".join(sorted(missing))
        )

    duplicate_count = (
        df
        .groupBy(
            "grid_id",
            "timestamp",
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            "ML2 input grain validation failed: "
            f"{duplicate_count} duplicate "
            "grid_id + timestamp groups found."
        )

    null_keys = (
        df
        .filter(
            F.col("grid_id").isNull()
            | F.col("timestamp").isNull()
        )
        .count()
    )

    if null_keys > 0:
        raise ValueError(
            "ML2 input contains rows with null "
            "grid_id or timestamp."
        )

    logger.info(
        "Input validation passed."
    )


# ============================================================
# Read SP3
# ============================================================

def read_hourly_summary(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    """
    Read SP3 hourly_grid_summary.
    """

    path = Path(input_path)

    if not path.exists():
        raise FileNotFoundError(
            f"SP3 input does not exist: {input_path}"
        )

    logger.info(
        "Reading SP3 hourly grid summary: %s",
        input_path,
    )

    df = spark.read.parquet(
        input_path
    )

    validate_input(df)

    logger.info(
        "SP3 rows read: %d",
        df.count(),
    )

    return df


# ============================================================
# Prepare activity data
# ============================================================

def prepare_activity(
    df: DataFrame,
) -> DataFrame:
    """
    Select only fields required by ML2 and handle null
    measures defensively.
    """

    return (
        df
        .select(
            "grid_id",
            "timestamp",
            "total_activity",
            "internet",
        )
        .withColumn(
            "total_activity",
            F.coalesce(
                F.col("total_activity"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "internet",
            F.coalesce(
                F.col("internet"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "timestamp",
            F.col("timestamp").cast("timestamp"),
        )
    )


# ============================================================
# Create recent window features
# ============================================================

def create_recent_features(
    df: DataFrame,
) -> DataFrame:
    """
    Calculate features from the trailing 24-hour window.

    Window:

        t-23 ... t

    The upper boundary is always the current row, t.

    No future timestamp can enter this window.
    """

    seconds_per_hour = 60 * 60

    recent_window = (
        Window
        .partitionBy("grid_id")
        .orderBy(
            F.col("timestamp").cast("long")
        )
        .rangeBetween(
            -(
                (RECENT_WINDOW_HOURS - 1)
                * seconds_per_hour
            ),
            0,
        )
    )

    return (
        df
        .withColumn(
            "avg_activity",
            F.avg(
                "total_activity"
            ).over(recent_window),
        )
        .withColumn(
            "peak_activity",
            F.max(
                "total_activity"
            ).over(recent_window),
        )
        .withColumn(
            "active_hours",
            F.sum(
                F.when(
                    F.col("total_activity") > 0,
                    1,
                ).otherwise(0)
            ).over(recent_window),
        )
        .withColumn(
            "activity_stddev",
            F.stddev_pop(
                "total_activity"
            ).over(recent_window),
        )
        .withColumn(
            "window_internet",
            F.sum(
                "internet"
            ).over(recent_window),
        )
        .withColumn(
            "window_total_activity",
            F.sum(
                "total_activity"
            ).over(recent_window),
        )
        .withColumn(
            "feature_timestamp",
            F.col("timestamp"),
        )
    )


# ============================================================
# Create baseline features
# ============================================================

def create_baseline_features(
    df: DataFrame,
) -> DataFrame:
    """
    Calculate the preceding 24-hour baseline.

    Baseline:

        t-47 ... t-24

    Recent:

        t-23 ... t

    The two windows do not overlap.
    """

    seconds_per_hour = 60 * 60

    baseline_window = (
        Window
        .partitionBy("grid_id")
        .orderBy(
            F.col("timestamp").cast("long")
        )
        .rangeBetween(
            -(
                (
                    RECENT_WINDOW_HOURS
                    + BASELINE_WINDOW_HOURS
                    - 1
                )
                * seconds_per_hour
            ),
            -(
                RECENT_WINDOW_HOURS
                * seconds_per_hour
            ),
        )
    )

    return (
        df
        .withColumn(
            "baseline_avg_activity",
            F.avg(
                "total_activity"
            ).over(baseline_window),
        )
    )


# ============================================================
# Calculate final features
# ============================================================

def calculate_features(
    df: DataFrame,
) -> DataFrame:
    """
    Calculate the approved ML2 feature set.
    """

    result = (
        df

        # ----------------------------------------------------
        # Activity growth
        # ----------------------------------------------------

        .withColumn(
            "activity_growth",
            F.when(
                F.col(
                    "baseline_avg_activity"
                ).isNull(),
                F.lit(None).cast("double"),
            )
            .when(
                F.col(
                    "baseline_avg_activity"
                ) == 0,
                F.when(
                    F.col("avg_activity") == 0,
                    F.lit(0.0),
                ).otherwise(
                    F.lit(None).cast("double")
                ),
            )
            .otherwise(
                (
                    F.col("avg_activity")
                    - F.col(
                        "baseline_avg_activity"
                    )
                )
                / F.col(
                    "baseline_avg_activity"
                )
            ),
        )

        # ----------------------------------------------------
        # Peak ratio
        # ----------------------------------------------------

        .withColumn(
            "peak_ratio",
            F.when(
                F.col("avg_activity") > 0,
                F.col("peak_activity")
                / F.col("avg_activity"),
            ).otherwise(
                F.lit(0.0)
            ),
        )

        # ----------------------------------------------------
        # Variability
        #
        # Coefficient of variation:
        #
        #     standard deviation / mean
        # ----------------------------------------------------

        .withColumn(
            "variability",
            F.when(
                F.col("avg_activity") > 0,
                F.coalesce(
                    F.col("activity_stddev"),
                    F.lit(0.0),
                )
                / F.col("avg_activity"),
            ).otherwise(
                F.lit(0.0)
            ),
        )

        # ----------------------------------------------------
        # Internet share
        # ----------------------------------------------------

        .withColumn(
            "internet_share",
            F.when(
                F.col(
                    "window_total_activity"
                ) > 0,
                F.col("window_internet")
                / F.col(
                    "window_total_activity"
                ),
            ).otherwise(
                F.lit(0.0)
            ),
        )

        # ----------------------------------------------------
        # Final ML2 contract
        # ----------------------------------------------------

        .select(
            "grid_id",
            "feature_timestamp",
            "avg_activity",
            "activity_growth",
            "active_hours",
            "peak_ratio",
            "variability",
            "internet_share",
        )
    )

    return result


# ============================================================
# Remove incomplete history
# ============================================================

def remove_incomplete_history(
    features: DataFrame,
) -> DataFrame:
    """
    Remove feature timestamps that do not yet have the
    complete 48-hour history required by ML2.

    This does NOT use future data.

    It only removes early timestamps that lack historical
    observations.
    """

    minimum_history = (
        RECENT_WINDOW_HOURS
        + BASELINE_WINDOW_HOURS
    )

    history_window = (
        Window
        .partitionBy("grid_id")
        .orderBy(
            F.col("feature_timestamp").cast("long")
        )
        .rowsBetween(
            -(minimum_history - 1),
            0,
        )
    )

    return (
        features
        .withColumn(
            "_history_count",
            F.count("*").over(
                history_window
            ),
        )
        .filter(
            F.col("_history_count")
            >= minimum_history
        )
        .drop("_history_count")
    )


# ============================================================
# Complete Spark feature pipeline
# ============================================================

def build_features(
    hourly_df: DataFrame,
) -> DataFrame:
    """
    Execute the complete ML2 feature pipeline.
    """

    df = prepare_activity(
        hourly_df
    )

    df = create_recent_features(
        df
    )

    df = create_baseline_features(
        df
    )

    features = calculate_features(
        df
    )

    return features


# ============================================================
# MySQL connection
# ============================================================

def create_mysql_connection(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
):
    """
    Create a MySQL connection.
    """

    logger.info(
        "Connecting to MySQL: %s:%s/%s",
        host,
        port,
        database,
    )

    connection = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )

    if not connection.is_connected():
        raise RuntimeError(
            "Could not connect to MySQL."
        )

    logger.info(
        "MySQL connection established."
    )

    return connection


# ============================================================
# Create feature table
# ============================================================

def create_feature_table(
    connection,
    table_name: str = DEFAULT_TABLE,
):
    """
    Create network_feature_table in MySQL.
    """

    cursor = connection.cursor()

    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (

            feature_key BIGINT AUTO_INCREMENT PRIMARY KEY,

            grid_id VARCHAR(50) NOT NULL,

            feature_timestamp DATETIME NOT NULL,

            avg_activity DOUBLE NULL,

            activity_growth DOUBLE NULL,

            active_hours INT NULL,

            peak_ratio DOUBLE NULL,

            variability DOUBLE NULL,

            internet_share DOUBLE NULL,

            created_at TIMESTAMP
                DEFAULT CURRENT_TIMESTAMP,

            CONSTRAINT uq_network_feature_grid_time
                UNIQUE (
                    grid_id,
                    feature_timestamp
                )
        )
    """

    cursor.execute(sql)

    # Index for API4:
    # GET /network/grid/{grid_id}/features

    try:
        cursor.execute(
            f"""
            CREATE INDEX idx_feature_grid_timestamp
            ON {table_name} (
                grid_id,
                feature_timestamp
            )
            """
        )
    except Error as exc:
        if "Duplicate key name" not in str(exc):
            cursor.close()
            raise

    connection.commit()
    cursor.close()

    logger.info(
        "MySQL table ready: %s",
        table_name,
    )


# ============================================================
# Write features to MySQL
# ============================================================

def write_features_to_mysql(
    features: DataFrame,
    connection,
    table_name: str = DEFAULT_TABLE,
    batch_size: int = 1000,
):
    """
    Persist Spark feature rows into MySQL.

    Existing grid/timestamp rows are updated.

    This makes the operation repeatable.
    """

    logger.info(
        "Writing ML2 features to MySQL table: %s",
        table_name,
    )

    rows = (
        features
        .orderBy(
            "grid_id",
            "feature_timestamp",
        )
        .collect()
    )

    if not rows:
        raise RuntimeError(
            "No ML2 feature rows were generated."
        )

    cursor = connection.cursor()

    insert_sql = f"""
        INSERT INTO {table_name} (
            grid_id,
            feature_timestamp,
            avg_activity,
            activity_growth,
            active_hours,
            peak_ratio,
            variability,
            internet_share
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE

            avg_activity =
                VALUES(avg_activity),

            activity_growth =
                VALUES(activity_growth),

            active_hours =
                VALUES(active_hours),

            peak_ratio =
                VALUES(peak_ratio),

            variability =
                VALUES(variability),

            internet_share =
                VALUES(internet_share)
    """

    processed = 0

    for start in range(
        0,
        len(rows),
        batch_size,
    ):

        batch_rows = rows[
            start:start + batch_size
        ]

        batch = []

        for row in batch_rows:

            batch.append(
                (
                    row["grid_id"],
                    row["feature_timestamp"],
                    (
                        float(row["avg_activity"])
                        if row["avg_activity"] is not None
                        else None
                    ),
                    (
                        float(row["activity_growth"])
                        if row["activity_growth"] is not None
                        else None
                    ),
                    (
                        int(row["active_hours"])
                        if row["active_hours"] is not None
                        else None
                    ),
                    (
                        float(row["peak_ratio"])
                        if row["peak_ratio"] is not None
                        else None
                    ),
                    (
                        float(row["variability"])
                        if row["variability"] is not None
                        else None
                    ),
                    (
                        float(row["internet_share"])
                        if row["internet_share"] is not None
                        else None
                    ),
                )
            )

        cursor.executemany(
            insert_sql,
            batch,
        )

        connection.commit()

        processed += len(batch)

        logger.info(
            "Feature rows processed: %d/%d",
            processed,
            len(rows),
        )

    cursor.close()

    logger.info(
        "ML2 feature rows written: %d",
        processed,
    )


# ============================================================
# Validate database table
# ============================================================

def validate_feature_table(
    connection,
    table_name: str = DEFAULT_TABLE,
):
    """
    Validate the resulting MySQL feature table.
    """

    cursor = connection.cursor()

    # Row count

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        """
    )

    row_count = cursor.fetchone()[0]

    if row_count == 0:
        cursor.close()

        raise RuntimeError(
            "network_feature_table contains zero rows."
        )

    logger.info(
        "network_feature_table rows: %d",
        row_count,
    )

    # Duplicate validation

    cursor.execute(
        f"""
        SELECT
            grid_id,
            feature_timestamp,
            COUNT(*) AS row_count
        FROM {table_name}
        GROUP BY
            grid_id,
            feature_timestamp
        HAVING COUNT(*) > 1
        """
    )

    duplicates = cursor.fetchall()

    if duplicates:
        cursor.close()

        raise RuntimeError(
            "network_feature_table contains duplicate "
            "grid_id + feature_timestamp rows."
        )

    # Null key validation

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE grid_id IS NULL
           OR feature_timestamp IS NULL
        """
    )

    null_keys = cursor.fetchone()[0]

    if null_keys != 0:
        cursor.close()

        raise RuntimeError(
            "network_feature_table contains null keys."
        )

    cursor.close()

    logger.info(
        "ML2 feature table validation passed."
    )


# ============================================================
# Arguments
# ============================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "ML2 network activity feature engineering "
            "and MySQL persistence."
        )
    )

    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help=(
            "SP3 hourly_grid_summary Parquet path."
        ),
    )

    parser.add_argument(
        "--host",
        default="localhost",
        help="MySQL host.",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=3306,
        help="MySQL port.",
    )

    parser.add_argument(
        "--user",
        default="root",
        help="MySQL username.",
    )

    parser.add_argument(
        "--password",
        default=None,
        help=(
            "MySQL password. If omitted, "
            "MYSQL_PASSWORD is used."
        ),
    )

    parser.add_argument(
        "--database",
        default=DEFAULT_DATABASE,
        help="MySQL database.",
    )

    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help="Feature table name.",
    )

    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================

def main():

    args = parse_arguments()

    password = (
        args.password
        if args.password is not None
        else os.getenv(
            "MYSQL_PASSWORD",
            "root",
        )
    )

    spark = None
    connection = None

    try:

        logger.info("=" * 70)
        logger.info(
            "ML2 NETWORK FEATURE ENGINEERING START"
        )
        logger.info("=" * 70)

        logger.info(
            "Feature convention:"
        )

        logger.info(
            "Prediction: t+1"
        )

        logger.info(
            "feature_timestamp: t"
        )

        logger.info(
            "Recent window: t-23 ... t"
        )

        logger.info(
            "Baseline window: t-47 ... t-24"
        )

        # ----------------------------------------------------
        # Spark
        # ----------------------------------------------------

        spark = create_spark_session()

        # ----------------------------------------------------
        # Read SP3
        # ----------------------------------------------------

        hourly_df = read_hourly_summary(
            spark,
            args.input,
        )

        # ----------------------------------------------------
        # Build features
        # ----------------------------------------------------

        network_feature_table = build_features(
            hourly_df
        )

        # ----------------------------------------------------
        # Remove incomplete history
        # ----------------------------------------------------

        network_feature_table = (
            remove_incomplete_history(
                network_feature_table
            )
        )

        logger.info(
            "Generated feature rows: %d",
            network_feature_table.count(),
        )

        # ----------------------------------------------------
        # Display sample
        # ----------------------------------------------------

        print(
            "\n--- ML2 Network Feature Table ---"
        )

        (
            network_feature_table
            .orderBy(
                "grid_id",
                "feature_timestamp",
            )
            .show(
                20,
                truncate=False,
            )
        )

        # ----------------------------------------------------
        # MySQL
        # ----------------------------------------------------

        connection = create_mysql_connection(
            host=args.host,
            port=args.port,
            user=args.user,
            password=password,
            database=args.database,
        )

        # ----------------------------------------------------
        # Create table
        # ----------------------------------------------------

        create_feature_table(
            connection,
            args.table,
        )

        # ----------------------------------------------------
        # Persist features
        # ----------------------------------------------------

        write_features_to_mysql(
            network_feature_table,
            connection,
            args.table,
        )

        # ----------------------------------------------------
        # Validate
        # ----------------------------------------------------

        validate_feature_table(
            connection,
            args.table,
        )

        logger.info("=" * 70)
        logger.info(
            "ML2 FEATURE ENGINEERING SUCCESS"
        )
        logger.info("=" * 70)

        return 0

    except Exception as exc:

        logger.exception(
            "ML2 FEATURE ENGINEERING FAILED: %s",
            exc,
        )

        return 1

    finally:

        if connection is not None:

            try:

                if connection.is_connected():
                    connection.close()

                    logger.info(
                        "MySQL connection closed."
                    )

            except Exception:
                pass

        if spark is not None:

            logger.info(
                "Stopping SparkSession."
            )

            spark.stop()


if __name__ == "__main__":
    sys.exit(main())