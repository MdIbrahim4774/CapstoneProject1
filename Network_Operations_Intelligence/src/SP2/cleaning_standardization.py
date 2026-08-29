from pathlib import Path
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"

FILE_PATTERN = str(DATA_DIR / "sms-call-internet-mi-*.csv")


# ---------------------------------------------------------
# Canonical column mapping
# ---------------------------------------------------------

COLUMN_MAPPING = {
    "datetime": "timestamp",
    "CellID": "grid_id",
    "countrycode": "country_code",
    "smsin": "sms_in",
    "smsout": "sms_out",
    "callin": "call_in",
    "callout": "call_out",
    "internet": "internet"
}


ACTIVITY_COLUMNS = [
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet"
]


# ---------------------------------------------------------
# Create Spark session
# ---------------------------------------------------------

def create_spark_session():

    return (
        SparkSession.builder
        .appName("SP2_Cleaning_Standardization")
        .getOrCreate()
    )


# ---------------------------------------------------------
# Load all raw daily files
# ---------------------------------------------------------

def load_raw_data(spark):

    logger.info("Reading files from:")
    logger.info(FILE_PATTERN)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(FILE_PATTERN)
    )

    logger.info("Raw record count: %d", df.count())

    return df


# ---------------------------------------------------------
# Rename columns to canonical names
# ---------------------------------------------------------

def rename_columns(df):

    logger.info("Renaming raw columns to canonical names.")

    for old_name, new_name in COLUMN_MAPPING.items():

        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    return df


# ---------------------------------------------------------
# Cast columns
# ---------------------------------------------------------

def cast_columns(df):

    logger.info("Casting timestamp and activity columns.")

    # Timestamp
    df = df.withColumn(
        "timestamp",
        F.to_timestamp(F.col("timestamp"))
    )

    # Grid ID
    df = df.withColumn(
        "grid_id",
        F.col("grid_id").cast("integer")
    )

    # Country code
    df = df.withColumn(
        "country_code",
        F.col("country_code").cast("integer")
    )

    # Activity measures
    for column in ACTIVITY_COLUMNS:

        df = df.withColumn(
            column,
            F.col(column).cast(DoubleType())
        )

    return df


# ---------------------------------------------------------
# Profile activity nulls before curated null-to-zero rule
# ---------------------------------------------------------

def profile_activity_nulls(df):

    logger.info("Profiling activity nulls before null-to-zero handling.")

    expressions = [
        F.sum(
            F.when(F.col(column).isNull(), 1).otherwise(0)
        ).alias(column)
        for column in ACTIVITY_COLUMNS
    ]

    result = df.select(*expressions).collect()[0]

    null_report = {
        column: result[column]
        for column in ACTIVITY_COLUMNS
    }

    total_nulls = sum(null_report.values())

    logger.info("Activity null counts: %s", null_report)
    logger.info("Total activity nulls: %d", total_nulls)

    return null_report


# ---------------------------------------------------------
# Quarantine invalid records
# ---------------------------------------------------------

def quarantine_invalid_records(df):

    logger.info("Identifying invalid records.")

    negative_condition = None

    for column in ACTIVITY_COLUMNS:

        condition = F.col(column) < 0

        if negative_condition is None:
            negative_condition = condition
        else:
            negative_condition = negative_condition | condition

    invalid_condition = (
        F.col("grid_id").isNull()
        | F.col("timestamp").isNull()
        | negative_condition
    )

    rejected_df = df.filter(invalid_condition)

    clean_df = df.filter(~invalid_condition)

    rejected_count = rejected_df.count()
    clean_count = clean_df.count()

    logger.info("Rejected records: %d", rejected_count)
    logger.info("Clean records: %d", clean_count)

    rejected_summary = {
        "rejected_records": rejected_count,
        "clean_records": clean_count
    }

    return clean_df, rejected_df, rejected_summary


# ---------------------------------------------------------
# Apply curated-layer null-to-zero rule
# ---------------------------------------------------------

def apply_null_to_zero(df):

    logger.info(
        "Applying curated-layer null-to-zero rule to activity measures."
    )

    for column in ACTIVITY_COLUMNS:

        df = df.withColumn(
            column,
            F.coalesce(F.col(column), F.lit(0.0))
        )

    return df


# ---------------------------------------------------------
# Derive total measures
# ---------------------------------------------------------

def derive_activity_totals(df):

    logger.info("Creating total_sms, total_calls and total_activity.")

    df = df.withColumn(
        "total_sms",
        F.col("sms_in") + F.col("sms_out")
    )

    df = df.withColumn(
        "total_calls",
        F.col("call_in") + F.col("call_out")
    )

    # Project-defined total activity indicator
    df = df.withColumn(
        "total_activity",
        F.col("total_sms")
        + F.col("total_calls")
        + F.col("internet")
    )

    return df


# ---------------------------------------------------------
# Derive time features
# ---------------------------------------------------------

def derive_time_features(df):

    logger.info("Creating date, hour and day_of_week.")

    df = (
        df
        .withColumn("date", F.to_date("timestamp"))
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("day_of_week", F.dayofweek("timestamp"))
    )

    return df


# ---------------------------------------------------------
# Verify hourly cadence
# ---------------------------------------------------------

def verify_hourly_cadence(df):

    logger.info("Checking hourly timestamp cadence.")

    cadence_df = (
        df
        .select("grid_id", "timestamp")
        .dropDuplicates()
        .withColumn(
            "previous_timestamp",
            F.lag("timestamp").over(
                __import__(
                    "pyspark.sql.window",
                    fromlist=["Window"]
                ).Window
                .partitionBy("grid_id")
                .orderBy("timestamp")
            )
        )
        .withColumn(
            "gap_hours",
            (
                F.col("timestamp").cast("long")
                - F.col("previous_timestamp").cast("long")
            ) / 3600
        )
    )

    invalid_cadence = cadence_df.filter(
        F.col("previous_timestamp").isNotNull()
        & (F.col("gap_hours") != 1)
    )

    invalid_count = invalid_cadence.count()

    if invalid_count == 0:
        logger.info("Hourly cadence verification passed.")
    else:
        logger.warning(
            "Hourly cadence verification found %d irregular intervals.",
            invalid_count
        )

    return {
        "irregular_intervals": invalid_count
    }


# ---------------------------------------------------------
# Build SP2 pipeline
# ---------------------------------------------------------

def build_clean_network_df(spark):

    # 1. Load
    raw_df = load_raw_data(spark)

    raw_count = raw_df.count()

    # 2. Rename
    df = rename_columns(raw_df)

    # 3. Cast
    df = cast_columns(df)

    # 4. Profile nulls BEFORE replacing them
    null_report = profile_activity_nulls(df)

    # 5. Quarantine invalid records
    clean_df, rejected_df, rejected_summary = (
        quarantine_invalid_records(df)
    )

    # 6. Apply curated null-to-zero rule
    clean_df = apply_null_to_zero(clean_df)

    # 7. Derived activity measures
    clean_df = derive_activity_totals(clean_df)

    # 8. Derived time features
    clean_df = derive_time_features(clean_df)

    # 9. Verify cadence
    cadence_report = verify_hourly_cadence(clean_df)

    clean_count = clean_df.count()

    # -----------------------------------------------------
    # Reports
    # -----------------------------------------------------

    rejected_summary["raw_records"] = raw_count
    rejected_summary["final_clean_records"] = clean_count

    null_handling_report = {
        "sms_in_nulls": null_report["sms_in"],
        "sms_out_nulls": null_report["sms_out"],
        "call_in_nulls": null_report["call_in"],
        "call_out_nulls": null_report["call_out"],
        "internet_nulls": null_report["internet"],
        "total_activity_nulls_handled": sum(null_report.values())
    }

    return (
        clean_df,
        rejected_df,
        rejected_summary,
        null_handling_report,
        cadence_report
    )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    spark = create_spark_session()

    try:

        (
            clean_network_df,
            rejected_df,
            rejected_summary,
            null_handling_report,
            cadence_report
        ) = build_clean_network_df(spark)

        # -------------------------------------------------
        # Expected Output
        # -------------------------------------------------

        print("\n" + "=" * 60)
        print("SP2 — CLEANING & STANDARDIZATION")
        print("=" * 60)

        print("\nClean Network Schema:")
        clean_network_df.printSchema()

        print("\nClean Network Sample:")
        clean_network_df.show(10, truncate=False)

        print("\nRejected Record Summary:")
        for key, value in rejected_summary.items():
            print(f"{key}: {value}")

        print("\nNull Handling Report:")
        for key, value in null_handling_report.items():
            print(f"{key}: {value}")

        print("\nCadence Report:")
        for key, value in cadence_report.items():
            print(f"{key}: {value}")

        print("\nRejected Records:")
        rejected_df.show(10, truncate=False)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()