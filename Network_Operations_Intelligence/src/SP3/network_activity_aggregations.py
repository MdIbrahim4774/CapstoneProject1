from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cleaning_standardization import build_clean_network_df


def consolidate_grid_hour(df):
    """
    Collapse country-code-level records into one record
    per grid_id + timestamp.
    """

    required_columns = [
        "timestamp",
        "grid_id",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    missing = [c for c in required_columns if c not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    hourly_grid_summary = (
        df.groupBy("grid_id", "timestamp")
        .agg(
            F.sum("sms_in").alias("sms_in"),
            F.sum("sms_out").alias("sms_out"),
            F.sum("call_in").alias("call_in"),
            F.sum("call_out").alias("call_out"),
            F.sum("internet_activity").alias("internet_activity")
        )
    )

    return hourly_grid_summary


def validate_hourly_grain(df):
    """
    Validate the canonical grain.

    Expected:
        exactly one row per grid_id + timestamp
    """

    duplicate_groups = (
        df.groupBy("grid_id", "timestamp")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_count = duplicate_groups.count()

    if duplicate_count > 0:
        raise ValueError(
            f"Grain validation failed: "
            f"{duplicate_count} duplicate grid_id + timestamp groups found."
        )

    print("✓ Grain validation passed.")
    print("  Grain: one row per grid_id + timestamp")


def add_activity_metrics(df):
    """
    Add operational activity KPIs.
    """

    df = (
        df
        .withColumn(
            "total_sms_activity",
            F.coalesce(F.col("sms_in"), F.lit(0))
            + F.coalesce(F.col("sms_out"), F.lit(0))
        )
        .withColumn(
            "total_call_activity",
            F.coalesce(F.col("call_in"), F.lit(0))
            + F.coalesce(F.col("call_out"), F.lit(0))
        )
        .withColumn(
            "total_activity",
            F.coalesce(F.col("total_sms_activity"), F.lit(0))
            + F.coalesce(F.col("total_call_activity"), F.lit(0))
            + F.coalesce(F.col("internet_activity"), F.lit(0))
        )
        .withColumn(
            "activity_date",
            F.to_date("timestamp")
        )
        .withColumn(
            "activity_hour",
            F.hour("timestamp")
        )
    )

    return df


def add_internet_share(df):
    """
    Calculate internet activity as a share of total activity.
    """

    return df.withColumn(
        "internet_share",
        F.when(
            F.col("total_activity") > 0,
            F.col("internet_activity") / F.col("total_activity")
        ).otherwise(F.lit(0.0))
    )


def create_daily_traffic_summary(hourly_grid_summary):
    """
    Create daily traffic summary for each grid.
    """

    daily_traffic_summary = (
        hourly_grid_summary
        .groupBy("grid_id", "activity_date")
        .agg(
            F.sum("total_sms_activity").alias("daily_sms_activity"),
            F.sum("total_call_activity").alias("daily_call_activity"),
            F.sum("internet_activity").alias("daily_internet_activity"),
            F.sum("total_activity").alias("daily_total_activity"),
            F.avg("total_activity").alias("avg_hourly_activity")
        )
        .orderBy(
            F.col("activity_date"),
            F.col("daily_total_activity").desc()
        )
    )

    return daily_traffic_summary


def create_hotspot_ranking(daily_traffic_summary):
    """
    Rank the top 10 high-activity grids within each day.
    """

    ranking_window = (
        Window
        .partitionBy("activity_date")
        .orderBy(
            F.col("daily_total_activity").desc()
        )
    )

    hotspot_ranking = (
        daily_traffic_summary
        .withColumn(
            "activity_rank",
            F.row_number().over(ranking_window)
        )
        .filter(
            F.col("activity_rank") <= 10
        )
    )

    return hotspot_ranking


def find_peak_activity_hour(hourly_grid_summary):
    """
    Identify the peak activity hour for each grid.
    """

    peak_window = (
        Window
        .partitionBy("grid_id")
        .orderBy(
            F.col("total_activity").desc(),
            F.col("timestamp").asc()
        )
    )

    peak_hours = (
        hourly_grid_summary
        .withColumn(
            "peak_rank",
            F.row_number().over(peak_window)
        )
        .filter(
            F.col("peak_rank") == 1
        )
        .select(
            "grid_id",
            "timestamp",
            "total_activity"
        )
        .withColumnRenamed(
            "timestamp",
            "peak_activity_timestamp"
        )
        .withColumnRenamed(
            "total_activity",
            "peak_activity"
        )
    )

    return peak_hours


def main():

    # ---------------------------------------------------------
    # Create Spark session
    # ---------------------------------------------------------

    spark = (
        SparkSession.builder
        .appName("SP3-Network-Activity-Aggregations")
        .getOrCreate()
    )

    # ---------------------------------------------------------
    # Get cleaned DataFrame directly from SP2
    # ---------------------------------------------------------

    print("\n--- Running SP2 ---")

    df,_,_,_,_ = build_clean_network_df(spark)

    print("\n--- SP2 Output / SP3 Input ---")
    df.printSchema()

    df.show(10, truncate=False)

    # ---------------------------------------------------------
    # 1. Collapse country-code records
    # ---------------------------------------------------------

    print("\n--- Consolidating Grid + Timestamp ---")

    hourly_grid_summary = consolidate_grid_hour(df)

    # ---------------------------------------------------------
    # 2. Add operational KPIs
    # ---------------------------------------------------------

    hourly_grid_summary = add_activity_metrics(
        hourly_grid_summary
    )

    # ---------------------------------------------------------
    # 3. Internet share
    # ---------------------------------------------------------

    hourly_grid_summary = add_internet_share(
        hourly_grid_summary
    )

    # ---------------------------------------------------------
    # 4. Validate canonical grain
    # ---------------------------------------------------------

    print("\n--- Validating Grain ---")

    validate_hourly_grain(
        hourly_grid_summary
    )

    # ---------------------------------------------------------
    # 5. Daily traffic summary
    # ---------------------------------------------------------

    daily_traffic_summary = create_daily_traffic_summary(
        hourly_grid_summary
    )

    # ---------------------------------------------------------
    # 6. Top 10 hotspots
    # ---------------------------------------------------------

    hotspot_ranking = create_hotspot_ranking(
        daily_traffic_summary
    )

    # ---------------------------------------------------------
    # 7. Peak activity hour
    # ---------------------------------------------------------

    peak_hours = find_peak_activity_hour(
        hourly_grid_summary
    )

    # ---------------------------------------------------------
    # Display results
    # ---------------------------------------------------------

    print("\n--- Hourly Grid Summary ---")
    hourly_grid_summary.show(20, truncate=False)

    print("\n--- Daily Traffic Summary ---")
    daily_traffic_summary.show(20, truncate=False)

    print("\n--- Top 10 Hotspots ---")
    hotspot_ranking.show(20, truncate=False)

    print("\n--- Peak Activity Hours ---")
    peak_hours.show(20, truncate=False)

    # ---------------------------------------------------------
    # Final schema checks
    # ---------------------------------------------------------

    print("\n--- Hourly Grid Summary Schema ---")
    hourly_grid_summary.printSchema()

    # ---------------------------------------------------------
    # Stop Spark
    # ---------------------------------------------------------

    spark.stop()


if __name__ == "__main__":
    main()