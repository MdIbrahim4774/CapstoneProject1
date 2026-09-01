from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType
import os
import json
from pathlib import Path


# =========================================================
# Windows Hadoop configuration
# =========================================================

os.environ["HADOOP_HOME"] = r"C:\Users\ibrahim.m\Documents\Notes\hadoop"
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] += r";C:\Users\ibrahim.m\Documents\Notes\hadoop\bin"


# =========================================================
# Paths
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[2]

GEOJSON_PATH = BASE_DIR / "data" / "ref" / "milano-grid.geojson"
SP3_PATH = BASE_DIR / "output" / "SP3" / "hourly_grid_summary"
SP4_PATH = BASE_DIR / "output" / "SP4"


# =========================================================
# Load GeoJSON
# =========================================================

def load_grid_lookup(spark):

    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        geojson = json.load(f)

    print("\n--- GeoJSON Inspection ---")
    print("Type:", geojson["type"])
    print("Number of features:", len(geojson["features"]))

    records = []

    for feature in geojson["features"]:

        cell_id = feature["properties"]["cellId"]
        geometry = feature["geometry"]

        records.append(
            (
                str(cell_id),
                json.dumps(geometry)
            )
        )

    schema = ["grid_id", "geometry"]

    grid_lookup = spark.createDataFrame(
        records,
        schema
    ).dropDuplicates(["grid_id"])

    print("\n--- Grid Lookup ---")
    print("Rows:", grid_lookup.count())

    grid_lookup.show(5, truncate=False)

    return grid_lookup


# =========================================================
# Validate GeoJSON
# =========================================================

def validate_geometry(grid_lookup):

    print("\n--- Geometry Validation ---")

    missing_geometry = (
        grid_lookup
        .filter(F.col("geometry").isNull())
        .count()
    )

    print("Geometry type: Polygon")
    print("Missing geometry:", missing_geometry)

    if missing_geometry > 0:
        raise ValueError(
            "Some grid cells have missing geometry."
        )

# =========================================================
# Calculate polygon centroid
# =========================================================

def centroid(geometry_json):

    if geometry_json is None:
        return None

    geometry = json.loads(geometry_json)

    coordinates = geometry["coordinates"][0]

    area = 0
    cx = 0
    cy = 0

    for i in range(len(coordinates) - 1):

        x1, y1 = coordinates[i]
        x2, y2 = coordinates[i + 1]

        cross = x1 * y2 - x2 * y1

        area += cross
        cx += (x1 + x2) * cross
        cy += (y1 + y2) * cross

    if area == 0:
        return (
            sum(p[0] for p in coordinates) / len(coordinates),
            sum(p[1] for p in coordinates) / len(coordinates)
        )

    return (
        cx / (3 * area),
        cy / (3 * area)
    )


centroid_schema = StructType([
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType())
])


centroid_udf = F.udf(
    centroid,
    centroid_schema
)


# =========================================================
# Validate join
# =========================================================

def validate_join(activity_df, enriched_df):

    before = (
        activity_df
        .select("grid_id")
        .distinct()
        .count()
    )

    after = (
        enriched_df
        .select("grid_id")
        .distinct()
        .count()
    )

    matched = (
        enriched_df
        .filter(F.col("geometry").isNotNull())
        .select("grid_id")
        .distinct()
        .count()
    )

    unmatched = (
        enriched_df
        .filter(F.col("geometry").isNull())
        .select("grid_id")
        .distinct()
    )

    unmatched_count = unmatched.count()

    coverage = (
        matched / before * 100
        if before > 0
        else 0
    )

    print("\n--- Grid Enrichment Report ---")
    print("Distinct grids before join:", before)
    print("Distinct grids after join :", after)
    print("Matched grids             :", matched)
    print("Missing geometry grids    :", unmatched_count)
    print(f"Coverage                  : {coverage:.2f}%")

    return unmatched, coverage


# =========================================================
# Main
# =========================================================

def main():

    spark = (
        SparkSession.builder
        .appName("SP4-Geospatial-Enrichment")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )

    try:

        # -------------------------------------------------
        # 1. Load SP3 activity
        # -------------------------------------------------

        activity_df = spark.read.parquet(
            str(SP3_PATH)
        )

        print("\n--- SP3 Input ---")
        activity_df.printSchema()

        # -------------------------------------------------
        # 2. Normalize grid_id
        # -------------------------------------------------

        activity_df = activity_df.withColumn(
            "grid_id",
            F.col("grid_id").cast("string")
        )

        # -------------------------------------------------
        # 3. Load GeoJSON lookup
        # -------------------------------------------------

        grid_lookup = load_grid_lookup(
            spark
        )

        # -------------------------------------------------
        # 4. Validate geometry
        # -------------------------------------------------

        validate_geometry(
            grid_lookup
        )

        # -------------------------------------------------
        # 5. Compare dataset sizes
        # -------------------------------------------------

        activity_grids = (
            activity_df
            .select("grid_id")
            .distinct()
            .count()
        )

        lookup_grids = grid_lookup.count()

        print("\n--- Dataset Size ---")
        print("Activity distinct grids:", activity_grids)
        print("Grid lookup rows:", lookup_grids)

        # -------------------------------------------------
        # 6. Standard join plan
        # -------------------------------------------------

        print("\n--- Standard Join Plan ---")

        (
            activity_df
            .join(
                grid_lookup,
                "grid_id",
                "left"
            )
            .explain()
        )

        # -------------------------------------------------
        # 7. Broadcast join plan
        # -------------------------------------------------

        print("\n--- Broadcast Join Plan ---")

        (
            activity_df
            .join(
                F.broadcast(grid_lookup),
                "grid_id",
                "left"
            )
            .explain()
        )

        # -------------------------------------------------
        # 8. Geospatial enrichment
        # -------------------------------------------------

        grid_activity_geo_df = (
            activity_df
            .join(
                F.broadcast(grid_lookup),
                "grid_id",
                "left"
            )
            .select(
                "timestamp",
                "grid_id",
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet",
                "total_activity",
                "geometry"
            )
        )

        # -------------------------------------------------
        # 9. Validate join
        # -------------------------------------------------

        unmatched_grids, coverage = validate_join(
            activity_df,
            grid_activity_geo_df
        )

        print("\n--- Unmatched Grid IDs ---")

        unmatched_grids.show(
            100,
            truncate=False
        )

        # -------------------------------------------------
        # 10. Add centroid
        # -------------------------------------------------

        grid_activity_geo_df = (
            grid_activity_geo_df
            .withColumn(
                "centroid",
                centroid_udf(
                    F.col("geometry")
                )
            )
            .withColumn(
                "centroid_longitude",
                F.col("centroid.longitude")
            )
            .withColumn(
                "centroid_latitude",
                F.col("centroid.latitude")
            )
            .drop("centroid")
        )

        # -------------------------------------------------
        # 11. Top high-activity grids
        # -------------------------------------------------

        top_grids = (
            grid_activity_geo_df
            .groupBy(
                "grid_id",
                "geometry",
                "centroid_longitude",
                "centroid_latitude"
            )
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
            .orderBy(
                F.desc("total_activity")
            )
            .limit(20)
        )

        print("\n--- Top 20 High-Activity Grids ---")

        top_grids.show(
            20,
            truncate=False
        )

        # -------------------------------------------------
        # 12. Save outputs
        # -------------------------------------------------

        SP4_PATH.mkdir(
            parents=True,
            exist_ok=True
        )

        grid_activity_geo_df.write.mode(
            "overwrite"
        ).parquet(
            str(
                SP4_PATH / "grid_activity_geo"
            )
        )

        unmatched_grids.write.mode(
            "overwrite"
        ).parquet(
            str(
                SP4_PATH / "unmatched_grid_ids"
            )
        )

        top_grids.write.mode(
            "overwrite"
        ).parquet(
            str(
                SP4_PATH / "top_high_activity_grids"
            )
        )

        # -------------------------------------------------
        # 13. Coverage report
        # -------------------------------------------------

        coverage_df = spark.createDataFrame([
            (
                activity_grids,
                lookup_grids,
                coverage
            )
        ], [
            "activity_grid_count",
            "lookup_grid_count",
            "enrichment_coverage_percentage"
        ])

        coverage_df.show()

        coverage_df.write.mode(
            "overwrite"
        ).parquet(
            str(
                SP4_PATH / "grid_enrichment_coverage"
            )
        )

        # -------------------------------------------------
        # Final
        # -------------------------------------------------

        print("\n✓ SP4 processing completed successfully.")

        print(
            "\nOutput:"
        )

        print(
            SP4_PATH / "grid_activity_geo"
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()