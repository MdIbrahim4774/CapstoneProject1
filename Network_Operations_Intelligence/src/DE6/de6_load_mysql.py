"""
DE6 - Load Network Analytics Warehouse into MySQL

Network Operations Intelligence

Source:
    SP7 aggregated Parquet output
    Milan grid GeoJSON reference

Target:
    MySQL

Warehouse model:

        dim_grid
            |
            | grid_key
            |
            v
    fact_network_activity
            ^
            |
            | time_key
            |
        dim_time

Fact grain:
    One row per grid_id + hourly timestamp

Usage:

    python src/de6_load_mysql.py --input data/output/telecom_activity/aggregated --reference data/reference/milano-grid.geojson --host localhost --port 3306 --user root --database network_operations

Linux/macOS:

    python src/de6_load_mysql.py \
        --input data/output/telecom_activity/aggregated \
        --reference data/reference/milano-grid.geojson \
        --host localhost \
        --port 3306 \
        --user root \
        --database network_operations
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from datetime import datetime

import mysql.connector
from mysql.connector import Error

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

os.environ["HADOOP_HOME"] = r"C:\Users\ibrahim.m\Documents\Notes\hadoop"
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] += r";C:\Users\ibrahim.m\Documents\Notes\hadoop\bin"

# ============================================================
# Logging
# ============================================================

def configure_logging(log_dir: str = "logs") -> logging.Logger:
    """
    Configure console + file logging.
    """

    Path(log_dir).mkdir(
        parents=True,
        exist_ok=True,
    )

    logger = logging.getLogger("de6_mysql_loader")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(
        Path(log_dir) / "de6_mysql_loader.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


logger = configure_logging()


# ============================================================
# Required Spark columns
# ============================================================

REQUIRED_COLUMNS = [
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
]


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

    The database must already exist.
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
            "MySQL connection could not be established."
        )

    logger.info("MySQL connection established.")

    return connection


# ============================================================
# Database schema
# ============================================================

def create_tables(connection):
    """
    Create the DE6 warehouse tables and indexes.
    """

    logger.info("Creating DE6 warehouse tables.")

    cursor = connection.cursor()

    statements = [

        # ----------------------------------------------------
        # dim_grid
        # ----------------------------------------------------

        """
        CREATE TABLE IF NOT EXISTS dim_grid (
            grid_key INT AUTO_INCREMENT PRIMARY KEY,

            grid_id VARCHAR(50) NOT NULL UNIQUE,

            centroid_latitude DECIMAL(10, 7),
            centroid_longitude DECIMAL(10, 7),

            geometry_ref VARCHAR(255),

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,

        # ----------------------------------------------------
        # dim_time
        # ----------------------------------------------------

        """
        CREATE TABLE IF NOT EXISTS dim_time (
            time_key BIGINT PRIMARY KEY,

            timestamp DATETIME NOT NULL UNIQUE,

            date DATE NOT NULL,
            hour TINYINT NOT NULL,

            day_of_week TINYINT NOT NULL,
            day_name VARCHAR(10) NOT NULL,

            month TINYINT NOT NULL,
            year SMALLINT NOT NULL
        )
        """,

        # ----------------------------------------------------
        # fact_network_activity
        # ----------------------------------------------------

        """
        CREATE TABLE IF NOT EXISTS fact_network_activity (
            activity_key BIGINT AUTO_INCREMENT PRIMARY KEY,

            grid_key INT NOT NULL,
            time_key BIGINT NOT NULL,

            sms_in DOUBLE NOT NULL DEFAULT 0,
            sms_out DOUBLE NOT NULL DEFAULT 0,

            call_in DOUBLE NOT NULL DEFAULT 0,
            call_out DOUBLE NOT NULL DEFAULT 0,

            internet DOUBLE NOT NULL DEFAULT 0,

            total_activity DOUBLE NOT NULL DEFAULT 0,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            CONSTRAINT fk_fact_grid
                FOREIGN KEY (grid_key)
                REFERENCES dim_grid(grid_key),

            CONSTRAINT fk_fact_time
                FOREIGN KEY (time_key)
                REFERENCES dim_time(time_key),

            CONSTRAINT uq_fact_grid_time
                UNIQUE (grid_key, time_key)
        )
        """,

        # ----------------------------------------------------
        # Indexes
        # ----------------------------------------------------

        """
        CREATE INDEX idx_fact_grid
        ON fact_network_activity(grid_key)
        """,

        """
        CREATE INDEX idx_fact_time
        ON fact_network_activity(time_key)
        """,

        """
        CREATE INDEX idx_fact_grid_time
        ON fact_network_activity(grid_key, time_key)
        """,

        """
        CREATE INDEX idx_time_date
        ON dim_time(date)
        """,

        """
        CREATE INDEX idx_time_hour
        ON dim_time(hour)
        """,
    ]

    for statement in statements:
        try:
            cursor.execute(statement)
        except Error as exc:
            # MySQL reports an error if an index already exists.
            # Tables themselves remain safe because IF NOT EXISTS
            # is used.
            if "Duplicate key name" in str(exc):
                logger.info(
                    "Index already exists: %s",
                    exc,
                )
            else:
                cursor.close()
                raise

    connection.commit()
    cursor.close()

    logger.info("DE6 warehouse schema ready.")


# ============================================================
# Validate Spark input
# ============================================================

def validate_input_dataframe(df: DataFrame):
    """
    Validate that the SP7 aggregated output contains
    the columns required by DE6.
    """

    logger.info(
        "Validating aggregated Spark output."
    )

    missing = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            "Required columns missing from aggregated output: "
            + ", ".join(missing)
        )

    logger.info(
        "Input schema validation passed."
    )


# ============================================================
# Read aggregated Parquet
# ============================================================

def read_aggregated_output(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    """
    Read the aggregated Parquet produced by SP7.
    """

    path = Path(input_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Aggregated output does not exist: {input_path}"
        )

    logger.info(
        "Reading aggregated Parquet: %s",
        input_path,
    )

    df = spark.read.parquet(input_path)

    row_count = df.count()

    logger.info(
        "Aggregated rows read: %d",
        row_count,
    )

    if row_count == 0:
        raise RuntimeError(
            "Aggregated Parquet contains zero rows."
        )

    logger.info(
        "Aggregated columns: %s",
        df.columns,
    )

    validate_input_dataframe(df)

    return df


# ============================================================
# Load Grid Dimension
# ============================================================

def load_grid_dimension(
    connection,
    reference_path: str,
):
    """
    Populate dim_grid from the static Milan GeoJSON.

    IMPORTANT:

    dim_grid is populated once per grid.

    The full Polygon is NOT copied into
    fact_network_activity.

    geometry_ref stores a lightweight reference to the
    source geometry.
    """

    logger.info(
        "Loading grid dimension from: %s",
        reference_path,
    )

    path = Path(reference_path)

    if not path.exists():
        raise FileNotFoundError(
            f"GeoJSON reference does not exist: {reference_path}"
        )

    with open(
        path,
        "r",
        encoding="utf-8",
    ) as file:
        geojson = json.load(file)

    if geojson.get("type") != "FeatureCollection":
        raise ValueError(
            "GeoJSON must be a FeatureCollection."
        )

    features = geojson.get("features", [])

    if not features:
        raise ValueError(
            "GeoJSON contains no features."
        )

    logger.info(
        "GeoJSON features: %d",
        len(features),
    )

    id_candidates = [
        "grid_id",
        "gridid",
        "cellid",
        "cell_id",
        "CellID",
        "cellId",
        "id",
        "ID",
    ]

    rows = []

    for feature in features:

        properties = feature.get("properties") or {}

        geometry = feature.get("geometry")

        grid_id = None

        # ----------------------------------------------------
        # Find grid identifier
        # ----------------------------------------------------

        for candidate in id_candidates:

            if candidate in properties:

                grid_id = properties[candidate]

                break

        # Fallback to GeoJSON feature id

        if grid_id is None:
            grid_id = feature.get("id")

        if grid_id is None:
            logger.warning(
                "Skipping GeoJSON feature with no grid identifier."
            )
            continue

        grid_id = str(grid_id).strip()

        if not grid_id:
            continue

        # ----------------------------------------------------
        # Calculate approximate centroid
        #
        # For the Milan grid polygons, use the average of
        # polygon vertices as a lightweight centroid.
        # ----------------------------------------------------

        centroid_latitude = None
        centroid_longitude = None

        if geometry:

            geometry_type = geometry.get("type")
            coordinates = geometry.get("coordinates")

            points = []

            if geometry_type == "Polygon":

                if coordinates:

                    for ring in coordinates:

                        if ring:

                            points.extend(ring)

            elif geometry_type == "MultiPolygon":

                if coordinates:

                    for polygon in coordinates:

                        for ring in polygon:

                            if ring:

                                points.extend(ring)

            if points:

                longitude_values = [
                    point[0]
                    for point in points
                    if len(point) >= 2
                ]

                latitude_values = [
                    point[1]
                    for point in points
                    if len(point) >= 2
                ]

                if longitude_values and latitude_values:

                    centroid_longitude = (
                        sum(longitude_values)
                        / len(longitude_values)
                    )

                    centroid_latitude = (
                        sum(latitude_values)
                        / len(latitude_values)
                    )

        geometry_ref = (
            f"milano-grid.geojson:{grid_id}"
        )

        rows.append(
            (
                grid_id,
                centroid_latitude,
                centroid_longitude,
                geometry_ref,
            )
        )

    if not rows:
        raise RuntimeError(
            "No valid grid records found in GeoJSON."
        )

    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO dim_grid (
            grid_id,
            centroid_latitude,
            centroid_longitude,
            geometry_ref
        )
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            centroid_latitude = VALUES(centroid_latitude),
            centroid_longitude = VALUES(centroid_longitude),
            geometry_ref = VALUES(geometry_ref)
    """

    cursor.executemany(
        insert_sql,
        rows,
    )

    connection.commit()

    cursor.close()

    logger.info(
        "dim_grid records processed: %d",
        len(rows),
    )

    # --------------------------------------------------------
    # Validation
    # --------------------------------------------------------

    cursor = connection.cursor()

    cursor.execute(
        "SELECT COUNT(*) FROM dim_grid"
    )

    count = cursor.fetchone()[0]

    cursor.close()

    logger.info(
        "dim_grid total rows: %d",
        count,
    )


# ============================================================
# Load Time Dimension
# ============================================================

def load_time_dimension(
    connection,
    df: DataFrame,
):
    """
    Populate dim_time from distinct timestamps in the
    aggregated Spark output.
    """

    logger.info(
        "Loading time dimension."
    )

    timestamps = (
        df
        .select("timestamp")
        .where(F.col("timestamp").isNotNull())
        .dropDuplicates(["timestamp"])
        .collect()
    )

    rows = []

    for row in timestamps:

        timestamp = row["timestamp"]

        if timestamp is None:
            continue

        time_key = int(
            timestamp.strftime("%Y%m%d%H")
        )

        date_value = timestamp.date()

        hour = timestamp.hour

        # Python weekday:
        # Monday = 0
        # Sunday = 6

        day_of_week = timestamp.weekday()

        day_name = timestamp.strftime("%A")

        month = timestamp.month

        year = timestamp.year

        rows.append(
            (
                time_key,
                timestamp,
                date_value,
                hour,
                day_of_week,
                day_name,
                month,
                year,
            )
        )

    if not rows:
        raise RuntimeError(
            "No valid timestamps found for dim_time."
        )

    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO dim_time (
            time_key,
            timestamp,
            date,
            hour,
            day_of_week,
            day_name,
            month,
            year
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            date = VALUES(date),
            hour = VALUES(hour),
            day_of_week = VALUES(day_of_week),
            day_name = VALUES(day_name),
            month = VALUES(month),
            year = VALUES(year)
    """

    cursor.executemany(
        insert_sql,
        rows,
    )

    connection.commit()

    cursor.close()

    logger.info(
        "dim_time records processed: %d",
        len(rows),
    )

    cursor = connection.cursor()

    cursor.execute(
        "SELECT COUNT(*) FROM dim_time"
    )

    count = cursor.fetchone()[0]

    cursor.close()

    logger.info(
        "dim_time total rows: %d",
        count,
    )


# ============================================================
# Create Dimension Lookup
# ============================================================

def load_dimension_lookups(connection):
    """
    Load dimension surrogate-key mappings into Python
    dictionaries.

    Returns:

        grid_lookup:
            grid_id -> grid_key

        time_lookup:
            timestamp -> time_key
    """

    logger.info(
        "Loading dimension key lookups."
    )

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT grid_id, grid_key
        FROM dim_grid
        """
    )

    grid_lookup = {
        str(grid_id): grid_key
        for grid_id, grid_key in cursor.fetchall()
    }

    cursor.execute(
        """
        SELECT timestamp, time_key
        FROM dim_time
        """
    )

    time_lookup = {
        timestamp: time_key
        for timestamp, time_key in cursor.fetchall()
    }

    cursor.close()

    logger.info(
        "Grid lookup entries: %d",
        len(grid_lookup),
    )

    logger.info(
        "Time lookup entries: %d",
        len(time_lookup),
    )

    return grid_lookup, time_lookup


# ============================================================
# Load Fact Table
# ============================================================

def load_fact_table(
    connection,
    df: DataFrame,
):
    """
    Populate fact_network_activity.

    Fact grain:

        grid_id + timestamp

    Dimension natural keys are converted to warehouse
    surrogate keys before insertion.
    """

    logger.info(
        "Loading fact_network_activity."
    )

    grid_lookup, time_lookup = (
        load_dimension_lookups(connection)
    )

    # --------------------------------------------------------
    # Collect only the columns needed by the warehouse.
    # --------------------------------------------------------

    fact_df = (
        df
        .select(
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet",
            "total_activity",
        )
        .dropDuplicates(
            ["timestamp", "grid_id"]
        )
    )

    source_rows = fact_df.count()

    logger.info(
        "Fact source rows after grid/hour deduplication: %d",
        source_rows,
    )

    rows = []

    missing_grid_keys = 0
    missing_time_keys = 0

    for row in fact_df.collect():

        timestamp = row["timestamp"]

        grid_id = (
            str(row["grid_id"]).strip()
            if row["grid_id"] is not None
            else None
        )

        if grid_id not in grid_lookup:

            missing_grid_keys += 1

            logger.warning(
                "No dim_grid match for grid_id=%s",
                grid_id,
            )

            continue

        if timestamp not in time_lookup:

            missing_time_keys += 1

            logger.warning(
                "No dim_time match for timestamp=%s",
                timestamp,
            )

            continue

        grid_key = grid_lookup[grid_id]

        time_key = time_lookup[timestamp]

        rows.append(
            (
                grid_key,
                time_key,
                float(row["sms_in"] or 0),
                float(row["sms_out"] or 0),
                float(row["call_in"] or 0),
                float(row["call_out"] or 0),
                float(row["internet"] or 0),
                float(row["total_activity"] or 0),
            )
        )

    if missing_grid_keys:
        logger.warning(
            "Rows missing grid dimension: %d",
            missing_grid_keys,
        )

    if missing_time_keys:
        logger.warning(
            "Rows missing time dimension: %d",
            missing_time_keys,
        )

    if not rows:
        raise RuntimeError(
            "No fact rows could be mapped to the dimensions."
        )

    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO fact_network_activity (
            grid_key,
            time_key,
            sms_in,
            sms_out,
            call_in,
            call_out,
            internet,
            total_activity
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            sms_in = VALUES(sms_in),
            sms_out = VALUES(sms_out),
            call_in = VALUES(call_in),
            call_out = VALUES(call_out),
            internet = VALUES(internet),
            total_activity = VALUES(total_activity)
    """

    # --------------------------------------------------------
    # Insert in batches to avoid a very large single request.
    # --------------------------------------------------------

    batch_size = 1000

    inserted = 0

    for start in range(
        0,
        len(rows),
        batch_size,
    ):

        batch = rows[
            start:start + batch_size
        ]

        cursor.executemany(
            insert_sql,
            batch,
        )

        connection.commit()

        inserted += len(batch)

        logger.info(
            "Fact rows processed: %d/%d",
            inserted,
            len(rows),
        )

    cursor.close()

    logger.info(
        "fact_network_activity rows processed: %d",
        len(rows),
    )


# ============================================================
# Warehouse Validation
# ============================================================

def validate_warehouse(connection):
    """
    Perform DE6 warehouse validation.
    """

    logger.info(
        "Starting warehouse validation."
    )

    cursor = connection.cursor()

    # --------------------------------------------------------
    # Row counts
    # --------------------------------------------------------

    tables = [
        "dim_grid",
        "dim_time",
        "fact_network_activity",
    ]

    for table in tables:

        cursor.execute(
            f"SELECT COUNT(*) FROM {table}"
        )

        count = cursor.fetchone()[0]

        logger.info(
            "%s rows: %d",
            table,
            count,
        )

    # --------------------------------------------------------
    # Fact rows without grid dimension
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM fact_network_activity f
        LEFT JOIN dim_grid g
            ON f.grid_key = g.grid_key
        WHERE g.grid_key IS NULL
        """
    )

    orphan_grids = cursor.fetchone()[0]

    logger.info(
        "Fact rows without grid dimension: %d",
        orphan_grids,
    )

    if orphan_grids != 0:
        raise RuntimeError(
            "Warehouse validation failed: orphan grid keys found."
        )

    # --------------------------------------------------------
    # Fact rows without time dimension
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM fact_network_activity f
        LEFT JOIN dim_time t
            ON f.time_key = t.time_key
        WHERE t.time_key IS NULL
        """
    )

    orphan_times = cursor.fetchone()[0]

    logger.info(
        "Fact rows without time dimension: %d",
        orphan_times,
    )

    if orphan_times != 0:
        raise RuntimeError(
            "Warehouse validation failed: orphan time keys found."
        )

    # --------------------------------------------------------
    # Duplicate grid/hour records
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT
            grid_key,
            time_key,
            COUNT(*) AS record_count
        FROM fact_network_activity
        GROUP BY
            grid_key,
            time_key
        HAVING COUNT(*) > 1
        """
    )

    duplicates = cursor.fetchall()

    logger.info(
        "Duplicate grid/hour fact groups: %d",
        len(duplicates),
    )

    if duplicates:
        raise RuntimeError(
            "Warehouse validation failed: duplicate "
            "grid/hour fact records found."
        )

    # --------------------------------------------------------
    # Null key validation
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM fact_network_activity
        WHERE grid_key IS NULL
           OR time_key IS NULL
        """
    )

    null_keys = cursor.fetchone()[0]

    logger.info(
        "Fact rows with null keys: %d",
        null_keys,
    )

    if null_keys != 0:
        raise RuntimeError(
            "Warehouse validation failed: null fact keys found."
        )

    # --------------------------------------------------------
    # Sample records
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT
            f.activity_key,
            g.grid_id,
            t.timestamp,
            f.total_activity
        FROM fact_network_activity f
        JOIN dim_grid g
            ON f.grid_key = g.grid_key
        JOIN dim_time t
            ON f.time_key = t.time_key
        ORDER BY
            t.timestamp,
            g.grid_id
        LIMIT 5
        """
    )

    sample_rows = cursor.fetchall()

    logger.info(
        "Sample warehouse records:"
    )

    for row in sample_rows:
        logger.info(
            "activity_key=%s | grid_id=%s | timestamp=%s | "
            "total_activity=%s",
            row[0],
            row[1],
            row[2],
            row[3],
        )

    cursor.close()

    logger.info(
        "Warehouse validation completed successfully."
    )


# ============================================================
# Spark Session
# ============================================================

def create_spark_session() -> SparkSession:
    """
    Create SparkSession for reading Parquet.
    """

    logger.info(
        "Creating SparkSession."
    )

    spark = (
        SparkSession.builder
        .appName(
            "NetworkOperations-DE6-MySQLLoader"
        )
        .config(
            "spark.driver.memory",
            "4g",
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
# Arguments
# ============================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "DE6 loader for the Network Operations "
            "Intelligence MySQL analytics warehouse."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help=(
            "SP7 aggregated Parquet directory."
        ),
    )

    parser.add_argument(
        "--reference",
        required=True,
        help=(
            "Path to Milan grid GeoJSON."
        ),
    )

    parser.add_argument(
        "--host",
        default="localhost",
        help=(
            "MySQL host. Default: localhost"
        ),
    )

    parser.add_argument(
        "--port",
        type=int,
        default=3306,
        help=(
            "MySQL port. Default: 3306"
        ),
    )

    parser.add_argument(
        "--user",
        required=True,
        help=(
            "MySQL username."
        ),
    )

    parser.add_argument(
        "--password",
        default=None,
        help=(
            "MySQL password. If omitted, MYSQL_PASSWORD "
            "environment variable is used."
        ),
    )

    parser.add_argument(
        "--database",
        default="network_operations",
        help=(
            "MySQL database. Default: network_operations"
        ),
    )

    parser.add_argument(
        "--log-dir",
        default="logs",
        help=(
            "Log directory. Default: logs"
        ),
    )

    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()

    args = parse_arguments()

    global logger

    logger = configure_logging(
        args.log_dir
    )

    spark = None
    connection = None

    password = (
        args.password
        if args.password is not None
        else os.getenv("MYSQL_PASSWORD", "root")
    )

    try:

        logger.info("=" * 70)
        logger.info(
            "DE6 MYSQL WAREHOUSE LOAD START"
        )
        logger.info("=" * 70)

        logger.info(
            "Start time: %s",
            datetime.now().isoformat(),
        )

        logger.info(
            "Input: %s",
            args.input,
        )

        logger.info(
            "Reference: %s",
            args.reference,
        )

        # ----------------------------------------------------
        # Spark
        # ----------------------------------------------------

        spark = create_spark_session()

        # ----------------------------------------------------
        # Read SP7 aggregated Parquet
        # ----------------------------------------------------

        df = read_aggregated_output(
            spark,
            args.input,
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
        # Schema
        # ----------------------------------------------------

        create_tables(connection)

        # ----------------------------------------------------
        # Dimension: Grid
        # ----------------------------------------------------

        load_grid_dimension(
            connection,
            args.reference,
        )

        # ----------------------------------------------------
        # Dimension: Time
        # ----------------------------------------------------

        load_time_dimension(
            connection,
            df,
        )

        # ----------------------------------------------------
        # Fact
        # ----------------------------------------------------

        load_fact_table(
            connection,
            df,
        )

        # ----------------------------------------------------
        # Validation
        # ----------------------------------------------------

        validate_warehouse(
            connection
        )

        elapsed = time.time() - start_time

        logger.info("=" * 70)
        logger.info(
            "DE6 MYSQL WAREHOUSE LOAD SUCCESS"
        )
        logger.info("=" * 70)

        logger.info(
            "Execution time: %.2f seconds",
            elapsed,
        )

        logger.info(
            "Final status: SUCCESS"
        )

        return 0

    except Exception as exc:

        elapsed = time.time() - start_time

        logger.exception(
            "DE6 MYSQL WAREHOUSE LOAD FAILED"
        )

        logger.error(
            "Error: %s",
            exc,
        )

        logger.error(
            "Execution time before failure: %.2f seconds",
            elapsed,
        )

        logger.error(
            "Final status: FAILED"
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