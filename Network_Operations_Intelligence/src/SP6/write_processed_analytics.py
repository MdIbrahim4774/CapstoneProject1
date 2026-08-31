from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
import os
import shutil


# =========================================================
# Windows Hadoop configuration
# =========================================================

os.environ["HADOOP_HOME"] = (
    r"C:\Users\ibrahim.m\Documents\Notes\hadoop"
)

os.environ["PATH"] = (
    os.environ["PATH"]
    + r";C:\Users\ibrahim.m\Documents\Notes\hadoop\bin"
)


# =========================================================
# PROJECT PATHS
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[2]

OUT_DIR = BASE_DIR / "output" / "SP6"

# SP4 Parquet input
SP4_OUTPUT = "output/SP4/grid_activity_geo"

# SP6 outputs
ACTIVITY_OUTPUT = OUT_DIR / "processed" / "activity"

HOURLY_OUTPUT = OUT_DIR / "analytics" / "hourly_grid_summary"

DASHBOARD_OUTPUT = OUT_DIR / "dashboard_summary.csv"

# Static geometry reference
GRID_REFERENCE = BASE_DIR / "data" / "ref" / "milano-grid.geojson"


# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("SP6_Write_Processed_Analytics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# HELPER
# =========================================================

def remove_output(path):
    """
    Remove an existing Spark output directory/file
    so that overwrite behaviour is explicit.
    """

    path = Path(path)

    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


# =========================================================
# 1. READ SP4 PARQUET
# =========================================================

print("\n" + "=" * 60)
print("READING SP4 PARQUET")
print("=" * 60)

df = spark.read.parquet(str(SP4_OUTPUT))

print("\nSP4 schema:")
df.printSchema()

print(f"SP4 record count: {df.count()}")


# =========================================================
# 2. BASIC VALIDATION
# =========================================================

required_columns = [
    "timestamp",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet",
]

missing_columns = [
    c for c in required_columns
    if c not in df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing required columns: {missing_columns}"
    )


# =========================================================
# 3. DERIVE DATE
# =========================================================

df = df.withColumn(
    "date",
    F.to_date("timestamp")
)

print("\nDate range:")

df.select(
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date")
).show()


# =========================================================
# 4. WRITE CLEAN ACTIVITY DATA AS PARQUET
#    Partition by date
# =========================================================

print("\n" + "=" * 60)
print("WRITING CLEAN ACTIVITY DATA")
print("=" * 60)

remove_output(ACTIVITY_OUTPUT)

(
    df
    .write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(str(ACTIVITY_OUTPUT))
)

print(
    f"Clean activity written to:\n{ACTIVITY_OUTPUT}"
)


# =========================================================
# 5. BUILD HOURLY GRID SUMMARY
# =========================================================

print("\n" + "=" * 60)
print("BUILDING HOURLY GRID SUMMARY")
print("=" * 60)


# Total network activity per record
df_activity = df.withColumn(
    "total_activity",
    F.coalesce(F.col("sms_in"), F.lit(0))
    + F.coalesce(F.col("sms_out"), F.lit(0))
    + F.coalesce(F.col("call_in"), F.lit(0))
    + F.coalesce(F.col("call_out"), F.lit(0))
    + F.coalesce(F.col("internet"), F.lit(0))
)


hourly_grid_summary = (
    df_activity
    .groupBy(
        "grid_id",
        "timestamp"
    )
    .agg(
        F.sum("sms_in").alias("sms_in"),
        F.sum("sms_out").alias("sms_out"),
        F.sum("call_in").alias("call_in"),
        F.sum("call_out").alias("call_out"),
        F.sum("internet").alias("internet"),
        F.sum("total_activity").alias("total_activity")
    )
)


# =========================================================
# 6. ENSURE ONE RECORD PER GRID + HOUR
# =========================================================

# If timestamp is already hourly, this is enough.
# If timestamp contains minutes/seconds, truncate it.

hourly_grid_summary = (
    hourly_grid_summary
    .withColumn(
        "hour",
        F.date_trunc("hour", "timestamp")
    )
    .drop("timestamp")
)


# Re-aggregate in case multiple records existed
# within the same grid/hour.

hourly_grid_summary = (
    hourly_grid_summary
    .groupBy(
        "grid_id",
        "hour"
    )
    .agg(
        F.sum("sms_in").alias("sms_in"),
        F.sum("sms_out").alias("sms_out"),
        F.sum("call_in").alias("call_in"),
        F.sum("call_out").alias("call_out"),
        F.sum("internet").alias("internet"),
        F.sum("total_activity").alias("total_activity")
    )
)


print("\nHourly summary schema:")
hourly_grid_summary.printSchema()

print(
    f"Hourly summary records: "
    f"{hourly_grid_summary.count()}"
)


# =========================================================
# 7. WRITE HOURLY GRID SUMMARY AS PARQUET
# =========================================================

print("\n" + "=" * 60)
print("WRITING HOURLY GRID SUMMARY")
print("=" * 60)

remove_output(HOURLY_OUTPUT)

(
    hourly_grid_summary
    .write
    .mode("overwrite")
    .parquet(str(HOURLY_OUTPUT))
)

print(
    f"Hourly grid summary written to:\n{HOURLY_OUTPUT}"
)


# =========================================================
# 8. DASHBOARD SUMMARY
# =========================================================

print("\n" + "=" * 60)
print("CREATING DASHBOARD SUMMARY")
print("=" * 60)


dashboard_summary = (
    df_activity
    .agg(
        F.count("*").alias("activity_records"),
        F.countDistinct("grid_id").alias("active_grids"),
        F.min("timestamp").alias("start_timestamp"),
        F.max("timestamp").alias("end_timestamp"),
        F.sum("sms_in").alias("total_sms_in"),
        F.sum("sms_out").alias("total_sms_out"),
        F.sum("call_in").alias("total_call_in"),
        F.sum("call_out").alias("total_call_out"),
        F.sum("internet").alias("total_internet"),
        F.sum("total_activity").alias("total_activity")
    )
)


# CSV output is intentionally small.
remove_output(DASHBOARD_OUTPUT)

(
    dashboard_summary
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(str(DASHBOARD_OUTPUT.parent / "dashboard_summary_tmp"))
)


# Spark CSV writes a directory.
# Rename the generated CSV to the requested filename.

tmp_dir = OUT_DIR / "dashboard_summary_tmp"

csv_files = list(tmp_dir.glob("*.csv"))

if csv_files:
    shutil.move(
        str(csv_files[0]),
        str(DASHBOARD_OUTPUT)
    )

shutil.rmtree(tmp_dir)


print(
    f"Dashboard summary written to:\n{DASHBOARD_OUTPUT}"
)


# =========================================================
# 9. RETAIN GEOJSON SEPARATELY
# =========================================================

print("\n" + "=" * 60)
print("CHECKING GRID REFERENCE")
print("=" * 60)

if GRID_REFERENCE.exists():
    print(
        f"Grid reference found:\n{GRID_REFERENCE}"
    )
else:
    print(
        "WARNING: milano-grid.geojson was not found at:"
    )
    print(GRID_REFERENCE)


# =========================================================
# 10. ROUND-TRIP VALIDATION
# =========================================================

print("\n" + "=" * 60)
print("ROUND-TRIP VALIDATION")
print("=" * 60)


# Read activity Parquet back
activity_check = spark.read.parquet(
    str(ACTIVITY_OUTPUT)
)

activity_original_count = df.count()
activity_readback_count = activity_check.count()

print("\nClean activity:")
print(f"Original count : {activity_original_count}")
print(f"Read-back count: {activity_readback_count}")

if activity_original_count != activity_readback_count:
    raise ValueError(
        "Activity Parquet round-trip count mismatch!"
    )


# Read hourly Parquet back
hourly_check = spark.read.parquet(
    str(HOURLY_OUTPUT)
)

hourly_original_count = hourly_grid_summary.count()
hourly_readback_count = hourly_check.count()

print("\nHourly grid summary:")
print(f"Original count : {hourly_original_count}")
print(f"Read-back count: {hourly_readback_count}")

if hourly_original_count != hourly_readback_count:
    raise ValueError(
        "Hourly Parquet round-trip count mismatch!"
    )


# =========================================================
# 11. SCHEMA VALIDATION
# =========================================================

print("\nActivity read-back schema:")
activity_check.printSchema()

print("\nHourly read-back schema:")
hourly_check.printSchema()


# =========================================================
# 12. DUPLICATE GRID + HOUR CHECK
# =========================================================

duplicate_count = (
    hourly_check
    .groupBy("grid_id", "hour")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"\nDuplicate grid/hour combinations: "
    f"{duplicate_count}"
)

if duplicate_count != 0:
    raise ValueError(
        "Hourly summary contains duplicate grid/hour records!"
    )


# =========================================================
# 13. DISPLAY SAMPLE OUTPUT
# =========================================================

print("\nHourly grid summary sample:")

hourly_check.show(
    10,
    truncate=False
)

print("\nDashboard summary:")

dashboard_summary.show(
    truncate=False
)


# =========================================================
# 14. FILE SIZE COMPARISON
# =========================================================

print("\n" + "=" * 60)
print("FILE SIZE INFORMATION")
print("=" * 60)


def directory_size(path):
    """
    Calculate total size of files under a directory.
    """

    path = Path(path)

    if not path.exists():
        return 0

    return sum(
        f.stat().st_size
        for f in path.rglob("*")
        if f.is_file()
    )


def format_size(size):
    """
    Convert bytes to readable units.
    """

    units = ["B", "KB", "MB", "GB"]

    size = float(size)

    for unit in units:
        if size < 1024:
            return f"{size:.2f} {unit}"

        size /= 1024

    return f"{size:.2f} TB"


activity_size = directory_size(ACTIVITY_OUTPUT)
hourly_size = directory_size(HOURLY_OUTPUT)

print(
    f"Activity Parquet size : "
    f"{format_size(activity_size)}"
)

print(
    f"Hourly Parquet size   : "
    f"{format_size(hourly_size)}"
)

if DASHBOARD_OUTPUT.exists():
    dashboard_size = DASHBOARD_OUTPUT.stat().st_size

    print(
        f"Dashboard CSV size    : "
        f"{format_size(dashboard_size)}"
    )


# =========================================================
# COMPLETE
# =========================================================

print("\n" + "=" * 60)
print("SP6 COMPLETED SUCCESSFULLY")
print("=" * 60)

print("\nOutputs:")

print(f"1. Clean activity : {ACTIVITY_OUTPUT}")
print(f"2. Hourly summary : {HOURLY_OUTPUT}")
print(f"3. Dashboard CSV  : {DASHBOARD_OUTPUT}")
print(f"4. Grid GeoJSON   : {GRID_REFERENCE}")


spark.stop()