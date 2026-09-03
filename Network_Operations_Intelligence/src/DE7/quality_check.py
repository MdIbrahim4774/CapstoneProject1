"""
DE7 - End-to-End Quality Checks

Validates the outputs produced by:

    DE2 -> SP7 -> DE6

Checks include:

    - raw input availability
    - reference file availability
    - processed Parquet availability
    - processed Parquet schema
    - warehouse table availability
    - warehouse fact rows
    - orphan dimension keys
    - duplicate grid/time facts
    - null fact keys

The function returns a machine-readable dictionary.
It does not write the pipeline status itself.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import mysql.connector
from mysql.connector import Error

from pyspark.sql import SparkSession


logger = logging.getLogger(
    "de7_quality_check"
)


REQUIRED_PROCESSED_COLUMNS = {
    "timestamp",
    "date",
    "hour",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet",
    "total_activity",
}


# ============================================================
# SPARK
# ============================================================

def create_quality_spark_session() -> SparkSession:
    """
    Create a lightweight Spark session used only for
    inspecting the SP7 Parquet output.
    """

    spark = (
        SparkSession.builder
        .appName(
            "NetworkOperations-DE7-QualityCheck"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "20",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    return spark


# ============================================================
# CHECK HELPERS
# ============================================================

def make_check(
    name: str,
    status: str,
    message: str,
    value: Any = None,
) -> dict[str, Any]:
    """
    Create a standard machine-readable quality result.
    """

    result = {
        "name": name,
        "status": status,
        "message": message,
    }

    if value is not None:
        result["value"] = value

    return result


# ============================================================
# RAW FILE CHECK
# ============================================================

def check_raw_files(
    raw_dir: str,
) -> tuple[dict[str, Any], int]:

    path = Path(
        raw_dir
    )

    files = []

    if path.exists():
        files = [
            file
            for file in path.glob("*.csv")
            if file.is_file()
        ]

    count = len(
        files
    )

    if count == 0:

        return (
            make_check(
                "raw_files",
                "FAIL",
                "No raw CSV files found.",
                count,
            ),
            count,
        )

    return (
        make_check(
            "raw_files",
            "PASS",
            "Raw CSV files are available.",
            count,
        ),
        count,
    )


# ============================================================
# REFERENCE CHECK
# ============================================================

def check_reference_file(
    reference_file: str,
) -> dict[str, Any]:

    path = Path(
        reference_file
    )

    if not path.exists():

        return make_check(
            "reference_file",
            "FAIL",
            f"GeoJSON reference does not exist: {path}",
        )

    if not path.is_file():

        return make_check(
            "reference_file",
            "FAIL",
            f"GeoJSON reference is not a file: {path}",
        )

    try:

        with open(
            path,
            "r",
            encoding="utf-8",
        ) as file:

            geojson = json.load(
                file
            )

        if geojson.get("type") != "FeatureCollection":

            return make_check(
                "reference_file",
                "FAIL",
                "Reference is not a GeoJSON FeatureCollection.",
            )

        features = geojson.get(
            "features",
            [],
        )

        if not features:

            return make_check(
                "reference_file",
                "FAIL",
                "GeoJSON contains no features.",
            )

        return make_check(
            "reference_file",
            "PASS",
            "GeoJSON reference is valid.",
            len(features),
        )

    except Exception as exc:

        return make_check(
            "reference_file",
            "FAIL",
            f"Could not read GeoJSON reference: {exc}",
        )


# ============================================================
# PARQUET CHECK
# ============================================================

def check_processed_output(
    spark: SparkSession,
    processed_dir: str,
) -> tuple[list[dict[str, Any]], int]:

    path = Path(
        processed_dir
    )

    checks = []

    if not path.exists():

        checks.append(
            make_check(
                "processed_output",
                "FAIL",
                f"Processed directory does not exist: {path}",
            )
        )

        return checks, 0

    parquet_files = list(
        path.rglob("*.parquet")
    )

    parquet_count = len(
        parquet_files
    )

    if parquet_count == 0:

        checks.append(
            make_check(
                "processed_output",
                "FAIL",
                "No Parquet files found.",
                parquet_count,
            )
        )

        return checks, 0

    checks.append(
        make_check(
            "processed_output",
            "PASS",
            "Processed Parquet files are available.",
            parquet_count,
        )
    )

    try:

        df = spark.read.parquet(
            str(path)
        )

        row_count = df.count()

        if row_count == 0:

            checks.append(
                make_check(
                    "processed_row_count",
                    "FAIL",
                    "Processed Parquet contains zero rows.",
                    row_count,
                )
            )

        else:

            checks.append(
                make_check(
                    "processed_row_count",
                    "PASS",
                    "Processed Parquet contains data.",
                    row_count,
                )
            )

        missing_columns = (
            REQUIRED_PROCESSED_COLUMNS
            - set(df.columns)
        )

        if missing_columns:

            checks.append(
                make_check(
                    "processed_schema",
                    "FAIL",
                    "Required processed columns are missing.",
                    sorted(
                        missing_columns
                    ),
                )
            )

        else:

            checks.append(
                make_check(
                    "processed_schema",
                    "PASS",
                    "Processed Parquet schema is valid.",
                    df.columns,
                )
            )

        null_grid_ids = (
            df.filter(
                df["grid_id"].isNull()
            ).count()
        )

        null_timestamps = (
            df.filter(
                df["timestamp"].isNull()
            ).count()
        )

        if (
            null_grid_ids > 0
            or null_timestamps > 0
        ):

            checks.append(
                make_check(
                    "processed_required_values",
                    "FAIL",
                    "Processed output contains null grid_id "
                    "or timestamp values.",
                    {
                        "null_grid_id": null_grid_ids,
                        "null_timestamp": null_timestamps,
                    },
                )
            )

        else:

            checks.append(
                make_check(
                    "processed_required_values",
                    "PASS",
                    "Processed grid_id and timestamp values "
                    "are non-null.",
                    {
                        "null_grid_id": 0,
                        "null_timestamp": 0,
                    },
                )
            )

        return checks, row_count

    except Exception as exc:

        checks.append(
            make_check(
                "processed_read",
                "FAIL",
                f"Could not read processed Parquet: {exc}",
            )
        )

        return checks, 0


# ============================================================
# MYSQL CONNECTION
# ============================================================

def create_mysql_connection(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
):
    """
    Create a MySQL connection for quality checks.
    """

    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )


# ============================================================
# MYSQL CHECKS
# ============================================================

def check_warehouse(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> tuple[list[dict[str, Any]], int]:

    checks = []

    connection = None
    cursor = None

    try:

        connection = create_mysql_connection(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )

        cursor = connection.cursor()

        required_tables = [
            "dim_grid",
            "dim_time",
            "fact_network_activity",
        ]

        existing_tables = []

        cursor.execute(
            """
            SELECT TABLE_NAME
            FROM information_schema.tables
            WHERE table_schema = %s
            """,
            (
                database,
            ),
        )

        existing_tables = {
            row[0]
            for row in cursor.fetchall()
        }

        missing_tables = [
            table
            for table in required_tables
            if table not in existing_tables
        ]

        if missing_tables:

            checks.append(
                make_check(
                    "warehouse_tables",
                    "FAIL",
                    "Required warehouse tables are missing.",
                    missing_tables,
                )
            )

            return checks, 0

        checks.append(
            make_check(
                "warehouse_tables",
                "PASS",
                "All required warehouse tables exist.",
                required_tables,
            )
        )

        # ----------------------------------------------------
        # Row counts
        # ----------------------------------------------------

        table_counts = {}

        for table in required_tables:

            cursor.execute(
                f"SELECT COUNT(*) FROM {table}"
            )

            table_counts[table] = (
                cursor.fetchone()[0]
            )

        fact_rows = table_counts[
            "fact_network_activity"
        ]

        if fact_rows == 0:

            checks.append(
                make_check(
                    "warehouse_fact_rows",
                    "FAIL",
                    "Warehouse fact table contains zero rows.",
                    fact_rows,
                )
            )

        else:

            checks.append(
                make_check(
                    "warehouse_fact_rows",
                    "PASS",
                    "Warehouse fact table contains data.",
                    fact_rows,
                )
            )

        # ----------------------------------------------------
        # Orphan grid keys
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM fact_network_activity f
            LEFT JOIN dim_grid g
                ON f.grid_key = g.grid_key
            WHERE g.grid_key IS NULL
            """
        )

        orphan_grids = (
            cursor.fetchone()[0]
        )

        if orphan_grids != 0:

            checks.append(
                make_check(
                    "orphan_grid_keys",
                    "FAIL",
                    "Fact rows contain orphan grid keys.",
                    orphan_grids,
                )
            )

        else:

            checks.append(
                make_check(
                    "orphan_grid_keys",
                    "PASS",
                    "No orphan grid keys found.",
                    0,
                )
            )

        # ----------------------------------------------------
        # Orphan time keys
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM fact_network_activity f
            LEFT JOIN dim_time t
                ON f.time_key = t.time_key
            WHERE t.time_key IS NULL
            """
        )

        orphan_times = (
            cursor.fetchone()[0]
        )

        if orphan_times != 0:

            checks.append(
                make_check(
                    "orphan_time_keys",
                    "FAIL",
                    "Fact rows contain orphan time keys.",
                    orphan_times,
                )
            )

        else:

            checks.append(
                make_check(
                    "orphan_time_keys",
                    "PASS",
                    "No orphan time keys found.",
                    0,
                )
            )

        # ----------------------------------------------------
        # Null fact keys
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM fact_network_activity
            WHERE grid_key IS NULL
               OR time_key IS NULL
            """
        )

        null_keys = (
            cursor.fetchone()[0]
        )

        if null_keys != 0:

            checks.append(
                make_check(
                    "null_fact_keys",
                    "FAIL",
                    "Fact rows contain null dimension keys.",
                    null_keys,
                )
            )

        else:

            checks.append(
                make_check(
                    "null_fact_keys",
                    "PASS",
                    "No null fact dimension keys found.",
                    0,
                )
            )

        # ----------------------------------------------------
        # Duplicate grid/time grain
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    grid_key,
                    time_key
                FROM fact_network_activity
                GROUP BY
                    grid_key,
                    time_key
                HAVING COUNT(*) > 1
            ) duplicate_groups
            """
        )

        duplicate_groups = (
            cursor.fetchone()[0]
        )

        if duplicate_groups != 0:

            checks.append(
                make_check(
                    "fact_grain",
                    "FAIL",
                    "Duplicate grid/time fact groups found.",
                    duplicate_groups,
                )
            )

        else:

            checks.append(
                make_check(
                    "fact_grain",
                    "PASS",
                    "Fact grain is unique by grid_key + time_key.",
                    0,
                )
            )

        return (
            checks,
            fact_rows,
        )

    except Error as exc:

        checks.append(
            make_check(
                "warehouse_connection",
                "FAIL",
                f"MySQL quality check failed: {exc}",
            )
        )

        return (
            checks,
            0,
        )

    finally:

        if cursor is not None:

            try:
                cursor.close()
            except Exception:
                pass

        if connection is not None:

            try:
                connection.close()
            except Exception:
                pass


# ============================================================
# MAIN QUALITY CHECK
# ============================================================

def run_quality_checks(
    raw_dir: str,
    processed_dir: str,
    reference_file: str,
    mysql_host: str,
    mysql_port: int,
    mysql_user: str,
    mysql_password: str,
    mysql_database: str,
) -> dict[str, Any]:
    """
    Run all DE7 quality checks.

    Returns a dictionary suitable for inclusion in
    pipeline_status.json.
    """

    checks = []

    # --------------------------------------------------------
    # Raw
    # --------------------------------------------------------

    raw_check, raw_file_count = (
        check_raw_files(
            raw_dir
        )
    )

    checks.append(
        raw_check
    )

    # --------------------------------------------------------
    # Reference
    # --------------------------------------------------------

    checks.append(
        check_reference_file(
            reference_file
        )
    )

    # --------------------------------------------------------
    # Spark / Parquet
    # --------------------------------------------------------

    spark = None
    processed_row_count = 0

    try:

        spark = (
            create_quality_spark_session()
        )

        parquet_checks, processed_row_count = (
            check_processed_output(
                spark,
                processed_dir,
            )
        )

        checks.extend(
            parquet_checks
        )

    finally:

        if spark is not None:

            spark.stop()

    # --------------------------------------------------------
    # MySQL
    # --------------------------------------------------------

    warehouse_checks, fact_rows = (
        check_warehouse(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
        )
    )

    checks.extend(
        warehouse_checks
    )

    # --------------------------------------------------------
    # Overall result
    # --------------------------------------------------------

    failed_checks = [
        check
        for check in checks
        if check["status"] != "PASS"
    ]

    overall_status = (
        "PASS"
        if not failed_checks
        else "FAIL"
    )

    logger.info(
        "DE7 quality result: %s",
        overall_status,
    )

    return {
        "status": overall_status,
        "raw_files": raw_file_count,
        "processed_rows": processed_row_count,
        "processed_parquet_files": len(
            list(
                Path(processed_dir).rglob(
                    "*.parquet"
                )
            )
        )
        if Path(processed_dir).exists()
        else 0,
        "warehouse_fact_rows": fact_rows,
        "checks": checks,
        "failed_checks": failed_checks,
    }


if __name__ == "__main__":

    print(
        "This module is intended to be called by the "
        "DE7 Airflow DAG."
    )