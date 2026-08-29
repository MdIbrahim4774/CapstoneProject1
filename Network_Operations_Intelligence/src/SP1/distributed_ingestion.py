from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.functions import (
    input_file_name,
    col,
    count,
    countDistinct,
    hour,
)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

DATA_DIR = BASE_DIR / "data"

FILE_PATTERN = list(DATA_DIR.glob("sms-call-internet-mi-*.csv"))


# ---------------------------------------------------------
# Manual schema
# ---------------------------------------------------------

MANUAL_SCHEMA = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("CellID", IntegerType(), True),
    StructField("countrycode", IntegerType(), True),
    StructField("smsin", DoubleType(), True),
    StructField("smsout", DoubleType(), True),
    StructField("callin", DoubleType(), True),
    StructField("callout", DoubleType(), True),
    StructField("internet", DoubleType(), True),
])


# ---------------------------------------------------------
# Create SparkSession
# ---------------------------------------------------------

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("NetworkOperations-SP1-DistributedIngestion")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ---------------------------------------------------------
# Read using schema inference
# ---------------------------------------------------------

def read_with_inference(spark):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv([str(file) for file in FILE_PATTERN])
    )


# ---------------------------------------------------------
# Read using manual schema
# ---------------------------------------------------------

def read_with_manual_schema(spark):
    return (
        spark.read
        .option("header", True)
        .schema(MANUAL_SCHEMA)
        .csv([str(file) for file in FILE_PATTERN])
    )


# ---------------------------------------------------------
# Add source-file traceability
# ---------------------------------------------------------

def add_file_traceability(df):
    return df.withColumn(
        "source_file",
        input_file_name()
    )


# ---------------------------------------------------------
# File-level row count report
# ---------------------------------------------------------

def create_file_report(df):
    return (
        df.groupBy("source_file")
        .agg(
            count("*").alias("row_count")
        )
        .orderBy("source_file")
    )


# ---------------------------------------------------------
# Dataset statistics
# ---------------------------------------------------------

def print_dataset_statistics(df):

    print("\n========== DATASET STATISTICS ==========")

    row_count = df.count()

    source_file_count = (
        df.select("source_file")
        .distinct()
        .count()
    )

    unique_cells = (
        df.select("CellID")
        .distinct()
        .count()
    )

    country_categories = (
        df.select("countrycode")
        .distinct()
        .count()
    )

    hourly_intervals = (
        df.select(
            col("datetime")
        )
        .distinct()
        .count()
    )

    partition_count = df.rdd.getNumPartitions()

    print(f"Total rows              : {row_count}")
    print(f"Source files            : {source_file_count}")
    print(f"Unique CellID values    : {unique_cells}")
    print(f"Country-code categories : {country_categories}")
    print(f"Distinct timestamps     : {hourly_intervals}")
    print(f"Spark partitions        : {partition_count}")


# ---------------------------------------------------------
# Hourly interval statistics
# ---------------------------------------------------------

def count_distinct_hours(df):

    hourly_df = (
        df
        .select(
            col("datetime"),
            hour("datetime").alias("hour")
        )
        .distinct()
    )

    return hourly_df.count()


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    spark = create_spark_session()

    try:

        print("\n==========================================")
        print("SP1 - DISTRIBUTED INGESTION")
        print("==========================================")

        print(f"\nReading files from:")
        print(FILE_PATTERN)

        # -----------------------------------------
        # 1. Infer schema
        # -----------------------------------------

        print("\n--- Inferred Schema ---")

        inferred_df = read_with_inference(spark)

        inferred_df.printSchema()

        # -----------------------------------------
        # 2. Manual schema
        # -----------------------------------------

        print("\n--- Manual Schema ---")

        raw_network_df = read_with_manual_schema(spark)

        raw_network_df.printSchema()

        # -----------------------------------------
        # 3. Add traceability
        # -----------------------------------------

        raw_network_df = add_file_traceability(
            raw_network_df
        )

        # -----------------------------------------
        # 4. Preview data
        # -----------------------------------------

        print("\n--- Sample Data ---")

        raw_network_df.show(
            10,
            truncate=False
        )

        # -----------------------------------------
        # 5. Dataset statistics
        # -----------------------------------------

        print_dataset_statistics(
            raw_network_df
        )

        # -----------------------------------------
        # 6. Distinct hourly intervals
        # -----------------------------------------

        distinct_hours = count_distinct_hours(
            raw_network_df
        )

        print(
            f"Distinct hourly intervals : {distinct_hours}"
        )

        # -----------------------------------------
        # 7. File-level report
        # -----------------------------------------

        print("\n========== FILE LEVEL ROW COUNT ==========")

        file_report = create_file_report(
            raw_network_df
        )

        file_report.show(
            truncate=False
        )

        # -----------------------------------------
        # 8. Partition information
        # -----------------------------------------

        print("\n========== PARTITION INFORMATION ==========")

        print(
            "Number of Spark partitions:",
            raw_network_df.rdd.getNumPartitions()
        )

        # -----------------------------------------
        # 9. Execution plan
        # -----------------------------------------

        print("\n========== EXECUTION PLAN ==========")

        raw_network_df.explain()

        print("\nSP1 ingestion completed successfully.")

        return raw_network_df

    finally:
        spark.stop()


if __name__ == "__main__":
    main()