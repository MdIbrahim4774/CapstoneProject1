"""
SP7 - Reusable Spark ETL Job
Network Operations Intelligence

Pipeline:
    Raw CSV files
        ↓
    read_raw()
        ↓
    clean()
        ↓
    aggregate()
        ↓
    enrich()
        ↓
    write_outputs()

Usage:
    python spark/telecom_pipeline.py ^
        --input data/raw ^
        --output data/output/telecom_activity ^
        --reference data/reference/milano-grid.geojson

Linux/macOS:
    python spark/telecom_pipeline.py \
        --input data/raw \
        --output data/output/telecom_activity \
        --reference data/reference/milano-grid.geojson
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

# os.environ["HADOOP_HOME"] = r"C:\Users\ibrahim.m\Documents\Notes\hadoop"
# os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
# os.environ["PATH"] += r";C:\Users\ibrahim.m\Documents\Notes\hadoop\bin"

# ============================================================
# Logging
# ============================================================

def configure_logging(log_dir: str = "logs") -> logging.Logger:
    """
    Configure console + file logging.
    """

    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("telecom_pipeline")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if the function is called again.
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(
        Path(log_dir) / "telecom_pipeline.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


logger = configure_logging()


# ============================================================
# Constants
# ============================================================

ACTIVITY_COLUMNS = [
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet",
]

RAW_REQUIRED_COLUMNS = [
    "datetime",
    "CellID",
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
]


# ============================================================
# Utility functions
# ============================================================

def normalize_column_name(column_name: str) -> str:
    """
    Convert source column names to the canonical naming convention.
    """

    replacements = {
        "datetime": "timestamp",
        "CellID": "grid_id",
        "cellid": "grid_id",
        "smsin": "sms_in",
        "smsout": "sms_out",
        "callin": "call_in",
        "callout": "call_out",
    }

    cleaned = column_name.strip()

    if cleaned in replacements:
        return replacements[cleaned]

    return cleaned.lower().replace(" ", "_")


def validate_input_directory(input_path: str) -> list:
    """
    Validate that the input path exists and contains CSV files.
    """

    path = Path(input_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Input path does not exist: {input_path}"
        )

    if not path.is_dir():
        raise NotADirectoryError(
            f"Input path is not a directory: {input_path}"
        )

    csv_files = list(path.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV input files found in: {input_path}"
        )

    return csv_files


def validate_reference_file(reference_path: str):
    """
    Validate the GeoJSON reference file.
    """

    path = Path(reference_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Reference file does not exist: {reference_path}"
        )

    if not path.is_file():
        raise FileNotFoundError(
            f"Reference path is not a file: {reference_path}"
        )


# ============================================================
# ETL Pipeline
# ============================================================

class TelecomPipeline:
    """
    Reusable Spark ETL pipeline for telecom network activity.

    Stages:

        read_raw()
        clean()
        aggregate()
        enrich()
        write_outputs()
    """

    def __init__(
        self,
        spark: SparkSession,
        input_path: str,
        output_path: str,
        reference_path: str,
    ):
        self.spark = spark

        self.input_path = input_path
        self.output_path = output_path
        self.reference_path = reference_path

        self.input_rows = 0
        self.rejected_rows = 0
        self.nulls_handled = 0
        self.output_rows = 0

    # ========================================================
    # 1. READ RAW
    # ========================================================

    def read_raw(self) -> DataFrame:
        """
        Read all daily telecom CSV files.

        Expected raw columns:

            datetime
            CellID
            countrycode
            smsin
            smsout
            callin
            callout
            internet
        """

        logger.info("Starting read_raw()")
        logger.info("Input path: %s", self.input_path)

        csv_files = validate_input_directory(self.input_path)

        logger.info(
            "Found %d CSV input files",
            len(csv_files),
        )

        for file in csv_files:
            logger.info("Input file: %s", file)

        df = (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv([str(file) for file in csv_files])
        )

        self.input_rows = df.count()

        logger.info(
            "Input rows read: %d",
            self.input_rows,
        )

        if self.input_rows == 0:
            raise RuntimeError(
                "Input files are present but contain zero rows."
            )

        logger.info(
            "Raw columns: %s",
            df.columns,
        )

        return df

    # ========================================================
    # 2. CLEAN
    # ========================================================

    def clean(self, df: DataFrame) -> DataFrame:
        """
        Clean and standardize raw telecom data.

        Operations:
            - normalize column names
            - validate required columns
            - parse timestamp
            - convert CellID to grid_id
            - convert activity fields to numeric
            - reject invalid rows
            - replace valid missing activity values with zero
            - derive date/hour
        """

        logger.info("Starting clean()")

        # ----------------------------------------------------
        # Normalize column names
        # ----------------------------------------------------

        for old_name in df.columns:
            new_name = normalize_column_name(old_name)

            if old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)

        logger.info(
            "Standardized columns: %s",
            df.columns,
        )

        # ----------------------------------------------------
        # Validate required columns
        # ----------------------------------------------------

        missing_columns = [
            column
            for column in RAW_REQUIRED_COLUMNS
            if normalize_column_name(column) not in df.columns
        ]

        # RAW_REQUIRED_COLUMNS contains source names, so check
        # against canonical names explicitly.
        canonical_required = [
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet",
        ]

        missing_columns = [
            column
            for column in canonical_required
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                "Required columns are missing: "
                + ", ".join(missing_columns)
            )

        # ----------------------------------------------------
        # Select relevant columns
        # ----------------------------------------------------

        df = df.select(
            "timestamp",
            "grid_id",
            *ACTIVITY_COLUMNS,
        )

        before_clean = df.count()

        # ----------------------------------------------------
        # Parse timestamp
        # ----------------------------------------------------

        df = df.withColumn(
            "timestamp",
            F.to_timestamp(F.col("timestamp")),
        )

        # ----------------------------------------------------
        # Standardize grid_id
        # ----------------------------------------------------

        df = df.withColumn(
            "grid_id",
            F.trim(F.col("grid_id").cast("string")),
        )

        # ----------------------------------------------------
        # Convert activity columns to numeric
        # ----------------------------------------------------

        for column in ACTIVITY_COLUMNS:
            df = df.withColumn(
                column,
                F.col(column).cast("double"),
            )

        # ----------------------------------------------------
        # Identify invalid rows
        #
        # Invalid:
        #   - missing timestamp
        #   - missing grid_id
        #   - negative activity
        # ----------------------------------------------------

        invalid_condition = (
            F.col("timestamp").isNull()
            | F.col("grid_id").isNull()
            | (F.trim(F.col("grid_id")) == "")
        )

        for column in ACTIVITY_COLUMNS:
            invalid_condition = invalid_condition | (
                F.col(column).isNotNull()
                & (F.col(column) < 0)
            )

        invalid_rows = df.filter(invalid_condition).count()

        self.rejected_rows = invalid_rows

        logger.info(
            "Rejected rows: %d",
            self.rejected_rows,
        )

        # ----------------------------------------------------
        # Remove invalid rows
        # ----------------------------------------------------

        df = df.filter(~invalid_condition)

        # ----------------------------------------------------
        # Handle null activity values
        #
        # Missing activity measurements are treated as zero
        # after structural validation.
        # ----------------------------------------------------

        null_count = 0

        for column in ACTIVITY_COLUMNS:
            count = df.filter(F.col(column).isNull()).count()
            null_count += count

            df = df.withColumn(
                column,
                F.coalesce(
                    F.col(column),
                    F.lit(0.0),
                ),
            )

        self.nulls_handled = null_count

        logger.info(
            "Null activity values handled: %d",
            self.nulls_handled,
        )

        # ----------------------------------------------------
        # Derive time fields
        # ----------------------------------------------------

        df = (
            df
            .withColumn("date", F.to_date("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
            .withColumn(
                "total_activity",
                F.col("sms_in")
                + F.col("sms_out")
                + F.col("call_in")
                + F.col("call_out")
                + F.col("internet"),
            )
        )

        cleaned_rows = df.count()

        logger.info(
            "Rows after cleaning: %d",
            cleaned_rows,
        )

        if cleaned_rows == 0:
            raise RuntimeError(
                "All input rows were rejected during cleaning."
            )

        logger.info(
            "Rows processed: %d | Rows rejected: %d",
            before_clean,
            self.rejected_rows,
        )

        return df

    # ========================================================
    # 3. AGGREGATE
    # ========================================================

    def aggregate(self, df: DataFrame) -> DataFrame:
        """
        Aggregate telecom activity at:

            grid_id + hourly timestamp

        This is the analytics grain used by the NOC pipeline.
        """

        logger.info("Starting aggregate()")

        # ----------------------------------------------------
        # Round timestamp down to the hour
        # ----------------------------------------------------

        df = df.withColumn(
            "timestamp",
            F.date_trunc("hour", F.col("timestamp")),
        )

        aggregated = (
            df.groupBy(
                "timestamp",
                "date",
                "hour",
                "grid_id",
            )
            .agg(
                F.sum("sms_in").alias("sms_in"),
                F.sum("sms_out").alias("sms_out"),
                F.sum("call_in").alias("call_in"),
                F.sum("call_out").alias("call_out"),
                F.sum("internet").alias("internet"),
                F.sum("total_activity").alias("total_activity"),
            )
        )

        aggregated = aggregated.orderBy(
            "timestamp",
            "grid_id",
        )

        rows = aggregated.count()

        logger.info(
            "Aggregated output rows: %d",
            rows,
        )

        if rows == 0:
            raise RuntimeError(
                "Aggregation produced zero rows."
            )

        return aggregated

    # ========================================================
    # 4. GEOJSON REFERENCE LOADING
    # ========================================================

    def load_grid_reference(self) -> DataFrame:
        """
        Load the Milan GeoJSON reference.

        The GeoJSON is read with Python because standard Spark
        does not natively provide a GeoJSON reader.

        The resulting reference DataFrame contains:

            grid_id
            geometry

        The function supports common grid identifier names in
        GeoJSON properties.
        """

        logger.info(
            "Loading grid reference: %s",
            self.reference_path,
        )

        validate_reference_file(self.reference_path)

        with open(
            self.reference_path,
            "r",
            encoding="utf-8",
        ) as file:
            geojson = json.load(file)

        if geojson.get("type") != "FeatureCollection":
            raise ValueError(
                "Reference file must be a GeoJSON FeatureCollection."
            )

        features = geojson.get("features", [])

        if not features:
            raise ValueError(
                "GeoJSON reference contains no features."
            )

        logger.info(
            "GeoJSON features found: %d",
            len(features),
        )

        records = []

        # Possible identifier names found in grid GeoJSON files.
        id_candidates = [
            "grid_id",
            "gridid",
            "cellid",
            "cell_id",
            "CellID",
            "id",
            "ID",
        ]

        for feature in features:
            properties = feature.get("properties") or {}
            geometry = feature.get("geometry")

            grid_id = None

            # First look inside properties.
            for candidate in id_candidates:
                if candidate in properties:
                    grid_id = properties[candidate]
                    break

            # Some GeoJSON files store the identifier as feature id.
            if grid_id is None:
                grid_id = feature.get("id")

            if grid_id is None:
                continue

            records.append(
                (
                    str(grid_id),
                    json.dumps(geometry),
                )
            )

        if not records:
            raise ValueError(
                "Could not find a grid identifier in the GeoJSON "
                "features."
            )

        schema = StructType(
            [
                StructField(
                    "grid_id",
                    StringType(),
                    False,
                ),
                StructField(
                    "geometry",
                    StringType(),
                    True,
                ),
            ]
        )

        grid_df = self.spark.createDataFrame(
            records,
            schema=schema,
        )

        grid_df = (
            grid_df
            .withColumn(
                "grid_id",
                F.trim(F.col("grid_id")),
            )
            .dropDuplicates(["grid_id"])
        )

        reference_rows = grid_df.count()

        logger.info(
            "Grid reference rows: %d",
            reference_rows,
        )

        return grid_df

    # ========================================================
    # 5. ENRICH
    # ========================================================

    def enrich(self, df: DataFrame) -> DataFrame:
        """
        Enrich aggregated activity with Milan grid geometry.
        """

        logger.info("Starting enrich()")

        grid_df = self.load_grid_reference()

        # ----------------------------------------------------
        # Broadcast reference because the static grid is
        # relatively small compared with daily activity data.
        # ----------------------------------------------------

        enriched = (
            df.alias("activity")
            .join(
                F.broadcast(grid_df).alias("grid"),
                F.col("activity.grid_id")
                == F.col("grid.grid_id"),
                "left",
            )
            .select(
                F.col("activity.timestamp"),
                F.col("activity.date"),
                F.col("activity.hour"),
                F.col("activity.grid_id"),
                F.col("activity.sms_in"),
                F.col("activity.sms_out"),
                F.col("activity.call_in"),
                F.col("activity.call_out"),
                F.col("activity.internet"),
                F.col("activity.total_activity"),
                F.col("grid.geometry"),
            )
        )

        missing_geometry = (
            enriched
            .filter(F.col("geometry").isNull())
            .count()
        )

        logger.info(
            "Rows without matching grid geometry: %d",
            missing_geometry,
        )

        enriched_rows = enriched.count()

        logger.info(
            "Enriched output rows: %d",
            enriched_rows,
        )

        if enriched_rows == 0:
            raise RuntimeError(
                "Enrichment produced zero rows."
            )

        return enriched

    # ========================================================
    # 6. WRITE OUTPUTS
    # ========================================================

    def write_outputs(
        self,
        cleaned_df: DataFrame,
        aggregated_df: DataFrame,
        enriched_df: DataFrame,
    ):
        """
        Persist reusable Spark outputs as Parquet.

        Output structure:

            output/
                cleaned/
                aggregated/
                enriched/
        """

        logger.info(
            "Starting write_outputs()"
        )

        output_root = Path(self.output_path)
        output_root.mkdir(
            parents=True,
            exist_ok=True,
        )

        cleaned_path = str(
            output_root / "cleaned"
        )

        aggregated_path = str(
            output_root / "aggregated"
        )

        enriched_path = str(
            output_root / "enriched"
        )

        logger.info(
            "Writing cleaned Parquet: %s",
            cleaned_path,
        )

        (
            cleaned_df
            .write
            .mode("overwrite")
            .parquet(cleaned_path)
        )

        logger.info(
            "Writing aggregated Parquet: %s",
            aggregated_path,
        )

        (
            aggregated_df
            .write
            .mode("overwrite")
            .parquet(aggregated_path)
        )

        logger.info(
            "Writing enriched Parquet: %s",
            enriched_path,
        )

        (
            enriched_df
            .write
            .mode("overwrite")
            .parquet(enriched_path)
        )

        self.output_rows = enriched_df.count()

        logger.info(
            "Final output rows: %d",
            self.output_rows,
        )

        logger.info(
            "Output writing completed successfully."
        )

    # ========================================================
    # RUN
    # ========================================================

    def run(self):
        """
        Execute the complete ETL pipeline.
        """

        start_time = time.time()

        logger.info("=" * 70)
        logger.info("TELECOM SPARK ETL JOB START")
        logger.info("=" * 70)

        logger.info(
            "Job start time: %s",
            datetime.now().isoformat(),
        )

        logger.info(
            "Input: %s",
            self.input_path,
        )

        logger.info(
            "Output: %s",
            self.output_path,
        )

        logger.info(
            "Reference: %s",
            self.reference_path,
        )

        try:
            # ------------------------------------------------
            # Stage 1
            # ------------------------------------------------

            raw_df = self.read_raw()

            # ------------------------------------------------
            # Stage 2
            # ------------------------------------------------

            cleaned_df = self.clean(raw_df)

            # ------------------------------------------------
            # Stage 3
            # ------------------------------------------------

            aggregated_df = self.aggregate(cleaned_df)

            # ------------------------------------------------
            # Stage 4
            # ------------------------------------------------

            enriched_df = self.enrich(aggregated_df)

            # ------------------------------------------------
            # Stage 5
            # ------------------------------------------------

            self.write_outputs(
                cleaned_df,
                aggregated_df,
                enriched_df,
            )

            elapsed = time.time() - start_time

            logger.info("=" * 70)
            logger.info("TELECOM SPARK ETL JOB SUCCESS")
            logger.info("=" * 70)

            logger.info(
                "Input rows: %d",
                self.input_rows,
            )

            logger.info(
                "Rejected rows: %d",
                self.rejected_rows,
            )

            logger.info(
                "Null values handled: %d",
                self.nulls_handled,
            )

            logger.info(
                "Output rows: %d",
                self.output_rows,
            )

            logger.info(
                "Job end time: %s",
                datetime.now().isoformat(),
            )

            logger.info(
                "Execution time: %.2f seconds",
                elapsed,
            )

            logger.info(
                "Final status: SUCCESS"
            )

        except Exception:
            elapsed = time.time() - start_time

            logger.exception(
                "TELECOM SPARK ETL JOB FAILED"
            )

            logger.error(
                "Execution time before failure: %.2f seconds",
                elapsed,
            )

            logger.error(
                "Final status: FAILED"
            )

            raise


# ============================================================
# Spark Session
# ============================================================

def create_spark_session() -> SparkSession:
    """
    Create the Spark session.
    """

    logger.info("Creating SparkSession")

    spark = (
        SparkSession.builder
        .appName("NetworkOperations-TelecomPipeline-SP7")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "Spark version: %s",
        spark.version,
    )

    return spark


# ============================================================
# Argument Parsing
# ============================================================

def parse_arguments():
    """
    Parse command-line arguments.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Reusable Spark ETL job for Network Operations "
            "Intelligence telecom activity data."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Directory containing raw daily CSV files.",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Directory where Parquet outputs will be written.",
    )

    parser.add_argument(
        "--reference",
        required=True,
        help="Path to the Milan grid GeoJSON reference.",
    )

    parser.add_argument(
        "--log-dir",
        default="logs",
        help="Directory for execution logs. Default: logs",
    )

    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================

def main():
    """
    Application entry point.
    """

    args = parse_arguments()

    global logger

    # Reconfigure logging if a custom log directory was supplied.
    logger = configure_logging(args.log_dir)

    spark = None

    try:
        spark = create_spark_session()

        pipeline = TelecomPipeline(
            spark=spark,
            input_path=args.input,
            output_path=args.output,
            reference_path=args.reference,
        )

        pipeline.run()

        return 0

    except Exception as exc:
        logger.error(
            "Pipeline terminated with error: %s",
            exc,
        )

        return 1

    finally:
        if spark is not None:
            logger.info("Stopping SparkSession")
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
