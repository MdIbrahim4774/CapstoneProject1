from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
import os
import time


# =========================================================
# Windows Hadoop configuration
# =========================================================

os.environ["HADOOP_HOME"] = r"C:\Users\ibrahim.m\Documents\Notes\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\Users\ibrahim.m\Documents\Notes\hadoop\bin"


# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("SP5_Performance_Execution")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# Paths
# =========================================================

CLEANED_PATH = r"data/processed/cleaned_network"
GRID_PATH = r"data/reference/milano-grid.geojson"


# =========================================================
# Utility: timing function
# =========================================================

def time_action(description, action):
    """
    Execute a Spark action and measure elapsed time.
    """

    start = time.perf_counter()

    result = action()

    elapsed = time.perf_counter() - start

    print(f"{description}: {elapsed:.4f} seconds")

    return result, elapsed


# =========================================================
# 1. Load cleaned network data
# =========================================================

print("\n" + "=" * 70)
print("1. LOAD CLEANED DATA")
print("=" * 70)

df = spark.read.parquet(CLEANED_PATH)

print("Columns:")
print(df.columns)

print("Number of partitions:", df.rdd.getNumPartitions())


# =========================================================
# 2. EXPLAIN HOTSPOT AGGREGATION
# =========================================================

print("\n" + "=" * 70)
print("2. EXPLAIN HOTSPOT AGGREGATION")
print("=" * 70)

hotspot_df = (
    df
    .groupBy("grid_id")
    .agg(
        F.sum("total_activity").alias("total_activity"),
        F.avg("total_activity").alias("avg_activity")
    )
    .orderBy(F.desc("total_activity"))
)

print("\nPhysical execution plan:")

hotspot_df.explain(mode="formatted")


# =========================================================
# 3. CACHE / PERSIST REUSED DATAFRAME
# =========================================================

print("\n" + "=" * 70)
print("3. CACHE PERFORMANCE COMPARISON")
print("=" * 70)


# ---------------------------------------------------------
# Without cache
# ---------------------------------------------------------

print("\nWITHOUT CACHE")

uncached_df = df

_, uncached_time_1 = time_action(
    "First action",
    lambda: uncached_df.groupBy("grid_id").count().collect()
)

_, uncached_time_2 = time_action(
    "Second action",
    lambda: uncached_df.groupBy("grid_id").count().collect()
)


# ---------------------------------------------------------
# With cache
# ---------------------------------------------------------

print("\nWITH CACHE")

cached_df = df.cache()

# IMPORTANT:
# cache is lazy, so count() materializes the cache.
_, cache_materialization_time = time_action(
    "Cache materialization",
    lambda: cached_df.count()
)

_, cached_time_1 = time_action(
    "First cached action",
    lambda: cached_df.groupBy("grid_id").count().collect()
)

_, cached_time_2 = time_action(
    "Second cached action",
    lambda: cached_df.groupBy("grid_id").count().collect()
)

cached_df.unpersist()


# =========================================================
# 4. REPARTITIONING
# =========================================================

print("\n" + "=" * 70)
print("4. REPARTITIONING")
print("=" * 70)

print("Original partitions:")
print(df.rdd.getNumPartitions())


# Repartition using date

if "date" in df.columns:

    repartitioned_df = df.repartition("date")

    print("Partitions after repartition(date):")
    print(repartitioned_df.rdd.getNumPartitions())

else:

    print("'date' column not found.")

    # Alternative using timestamp
    repartitioned_df = df.repartition("timestamp")

    print("Partitions after repartition(timestamp):")
    print(repartitioned_df.rdd.getNumPartitions())


# Explicit partition count example

repartitioned_4 = df.repartition(4)

print("Partitions after repartition(4):")
print(repartitioned_4.rdd.getNumPartitions())


# =========================================================
# 5. COLUMN PRUNING
# =========================================================

print("\n" + "=" * 70)
print("5. COLUMN PRUNING")
print("=" * 70)


print("\nAggregation using full DataFrame:")

full_aggregation = (
    df
    .groupBy("grid_id")
    .agg(
        F.sum("total_activity").alias("total_activity")
    )
)

full_aggregation.explain(mode="formatted")


print("\nAggregation after selecting only required columns:")

pruned_df = df.select(
    "grid_id",
    "total_activity"
)

pruned_aggregation = (
    pruned_df
    .groupBy("grid_id")
    .agg(
        F.sum("total_activity").alias("total_activity")
    )
)

pruned_aggregation.explain(mode="formatted")


# =========================================================
# 6. STANDARD JOIN VS BROADCAST JOIN
# =========================================================

print("\n" + "=" * 70)
print("6. STANDARD JOIN VS BROADCAST JOIN")
print("=" * 70)


# ---------------------------------------------------------
# Load static grid lookup
# ---------------------------------------------------------

grid_df = (
    spark.read
    .format("json")
    .load(GRID_PATH)
)


print("\nGrid lookup columns:")
print(grid_df.columns)


# ---------------------------------------------------------
# Standard join
# ---------------------------------------------------------

print("\nSTANDARD JOIN PLAN")

standard_join = df.join(
    grid_df,
    on="grid_id",
    how="left"
)

standard_join.explain(mode="formatted")


# ---------------------------------------------------------
# Broadcast join
# ---------------------------------------------------------

print("\nBROADCAST JOIN PLAN")

broadcast_join = df.join(
    broadcast(grid_df),
    on="grid_id",
    how="left"
)

broadcast_join.explain(mode="formatted")


# =========================================================
# 7. PERFORMANCE OBSERVATIONS
# =========================================================

print("\n" + "=" * 70)
print("7. PERFORMANCE OBSERVATIONS")
print("=" * 70)

print("""
Observation 1 - Cache / Persist
-------------------------------
Caching is useful when the same DataFrame is used by multiple actions.
The first action may take longer because Spark must materialize the cache.
Later actions can reuse the cached data instead of recomputing the
upstream transformations.

Evidence:
Compare:
    uncached_time_1
    uncached_time_2

against:
    cached_time_1
    cached_time_2


Observation 2 - Column Pruning
-----------------------------
Selecting only the columns required for an aggregation reduces the amount
of data that needs to be carried through the execution plan.

Evidence:
Compare the physical plans generated by:

    df.groupBy(...)

and:

    df.select("grid_id", "total_activity")
      .groupBy(...)

Spark may also perform column pruning automatically when reading Parquet,
so the physical plan should be inspected rather than assuming that manual
select() always produces a measurable speedup.


Observation 3 - Broadcast Join
------------------------------
A broadcast join allows Spark to send the small static grid lookup to
worker executors instead of performing a large shuffle-based join.

Evidence:
Compare the physical plans for:

    df.join(grid_df, ...)

and:

    df.join(broadcast(grid_df), ...)

Look for BroadcastHashJoin in the broadcast plan.
""")


# =========================================================
# 8. FINAL OPTIMIZED TRANSFORMATION
# =========================================================

print("\n" + "=" * 70)
print("8. OPTIMIZED TRANSFORMATION")
print("=" * 70)


# Keep only columns required by the downstream hotspot calculation

optimized_df = df.select(
    "timestamp",
    "grid_id",
    "total_activity"
)


# Cache because the DataFrame is reused

optimized_df = optimized_df.cache()


# Materialize cache

optimized_df.count()


# Aggregate

optimized_hotspots = (
    optimized_df
    .groupBy("grid_id")
    .agg(
        F.sum("total_activity").alias("total_activity"),
        F.avg("total_activity").alias("avg_activity")
    )
    .orderBy(F.desc("total_activity"))
)


print("\nOptimized physical plan:")

optimized_hotspots.explain(mode="formatted")


# =========================================================
# Final results
# =========================================================

print("\n" + "=" * 70)
print("PERFORMANCE SUMMARY")
print("=" * 70)

print(f"Uncached first action : {uncached_time_1:.4f} sec")
print(f"Uncached second action: {uncached_time_2:.4f} sec")

print(f"Cached first action   : {cached_time_1:.4f} sec")
print(f"Cached second action  : {cached_time_2:.4f} sec")

print("\nOriginal partitions:",
      df.rdd.getNumPartitions())

print("Repartition(4) partitions:",
      repartitioned_4.rdd.getNumPartitions())


# =========================================================
# Cleanup
# =========================================================

optimized_df.unpersist()

spark.stop()