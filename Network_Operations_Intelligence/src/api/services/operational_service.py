import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import mysql.connector
from mysql.connector import MySQLConnection


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]

PIPELINE_STATUS_PATH = Path(
    os.getenv(
        "PIPELINE_STATUS_PATH",
        PROJECT_ROOT / "data" / "analytics" / "pipeline_status.json",
    )
)

POLYGON_REFERENCE = os.getenv(
    "GRID_POLYGON_REFERENCE",
    "data/reference/milano-grid.geojson",
)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", "root"),
        database=os.getenv("DB_NAME", "network_operations"),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    else:
        value = str(value).strip()

        if value.endswith("Z"):
            value = value[:-1] + "+00:00"

        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt


def _calculate_freshness(as_of: Optional[datetime]):
    if as_of is None:
        return None, "UNKNOWN"

    now = datetime.now(timezone.utc)

    age_seconds = max(
        0,
        (now - as_of).total_seconds(),
    )

    freshness_minutes = age_seconds / 60

    # Operational thresholds.
    if freshness_minutes <= 60:
        freshness_status = "FRESH"
    elif freshness_minutes <= 180:
        freshness_status = "STALE"
    else:
        freshness_status = "VERY_STALE"

    return freshness_minutes, freshness_status


def _normalise_task(task: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "status": str(
            task.get(
                "status",
                task.get("state", "UNKNOWN"),
            )
        ).upper(),
        "rows_in": int(task.get("rows_in", 0) or 0),
        "rows_rejected": int(
            task.get("rows_rejected", 0) or 0
        ),
        "nulls_handled": int(
            task.get("nulls_handled", 0) or 0
        ),
        "rows_published": int(
            task.get("rows_published", 0) or 0
        ),
    }


# ---------------------------------------------------------------------------
# Pipeline status
# ---------------------------------------------------------------------------

def read_pipeline_status() -> Dict[str, Any]:
    """
    Read the machine-readable pipeline status produced by DE7.

    API6 deliberately does not infer pipeline health from memory,
    previous API calls, or model knowledge.
    """

    if not PIPELINE_STATUS_PATH.exists():
        return {
            "healthy": False,
            "reasons": [
                "Pipeline status record is unavailable"
            ],
            "run_id": "UNKNOWN",
            "timestamp": datetime.now(timezone.utc),
            "tasks": {},
            "rows_in": 0,
            "rows_rejected": 0,
            "nulls_handled": 0,
            "rows_published": 0,
            "as_of": None,
            "freshness_minutes": None,
            "freshness_status": "UNKNOWN",
        }

    try:
        with PIPELINE_STATUS_PATH.open(
            "r",
            encoding="utf-8",
        ) as file:
            raw = json.load(file)

    except Exception as exc:
        return {
            "healthy": False,
            "reasons": [
                f"Unable to read pipeline status record: {exc}"
            ],
            "run_id": "UNKNOWN",
            "timestamp": datetime.now(timezone.utc),
            "tasks": {},
            "rows_in": 0,
            "rows_rejected": 0,
            "nulls_handled": 0,
            "rows_published": 0,
            "as_of": None,
            "freshness_minutes": None,
            "freshness_status": "UNKNOWN",
        }

    # Allow either:
    #
    # {
    #   "run_id": "...",
    #   ...
    # }
    #
    # or:
    #
    # {
    #   "last_run": {
    #       ...
    #   }
    # }
    #
    record = raw.get("last_run", raw)

    run_id = str(
        record.get(
            "run_id",
            record.get("dag_run_id", "UNKNOWN"),
        )
    )

    timestamp = _parse_datetime(
        record.get(
            "timestamp",
            record.get(
                "completed_at",
                record.get("end_time"),
            ),
        )
    )

    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    as_of = _parse_datetime(
        record.get(
            "as_of",
            record.get("current_as_of"),
        )
    )

    raw_tasks = record.get(
        "tasks",
        record.get("task_status", {}),
    )

    tasks = {
        name: _normalise_task(task)
        for name, task in raw_tasks.items()
    }

    reasons: List[str] = []

    overall_status = str(
        record.get(
            "status",
            record.get("state", ""),
        )
    ).upper()

    if overall_status in {
        "FAILED",
        "FAILURE",
        "UNHEALTHY",
    }:
        reasons.append(
            f"Pipeline run status is {overall_status}"
        )

    for task_name, task in tasks.items():
        task_status = task["status"]

        if task_status in {
            "FAILED",
            "FAILURE",
            "UPSTREAM_FAILED",
        }:
            reasons.append(
                f"Task '{task_name}' has status {task_status}"
            )

    freshness_minutes, freshness_status = (
        _calculate_freshness(as_of)
    )

    if freshness_status in {
        "STALE",
        "VERY_STALE",
    }:
        reasons.append(
            f"Analytics layer is {freshness_status.lower()}"
        )

    if as_of is None:
        reasons.append(
            "Analytics AS_OF timestamp is unavailable"
        )

    healthy = len(reasons) == 0

    return {
        "healthy": healthy,
        "reasons": reasons,
        "run_id": run_id,
        "timestamp": timestamp,
        "tasks": tasks,
        "rows_in": int(
            record.get("rows_in", 0) or 0
        ),
        "rows_rejected": int(
            record.get("rows_rejected", 0) or 0
        ),
        "nulls_handled": int(
            record.get("nulls_handled", 0) or 0
        ),
        "rows_published": int(
            record.get("rows_published", 0) or 0
        ),
        "as_of": as_of,
        "freshness_minutes": freshness_minutes,
        "freshness_status": freshness_status,
    }


# ---------------------------------------------------------------------------
# Grid location
# ---------------------------------------------------------------------------

def get_grid_location(grid_id: str) -> Optional[Dict[str, Any]]:
    """
    Return centroid information from dim_grid.

    The Polygon geometry itself is deliberately NOT returned.
    """

    connection = get_db_connection()

    try:
        cursor = connection.cursor(dictionary=True)

        query = """
            SELECT
                grid_id,
                centroid_latitude,
                centroid_longitude
            FROM dim_grid
            WHERE grid_id = %s
            LIMIT 1
        """

        cursor.execute(query, (grid_id,))
        row = cursor.fetchone()

        if row is None:
            return None

        return {
            "grid_id": row["grid_id"],
            "centroid_latitude": float(
                row["centroid_latitude"]
            ),
            "centroid_longitude": float(
                row["centroid_longitude"]
            ),
            "polygon_reference": POLYGON_REFERENCE,
        }

    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Optional neighbours endpoint
# ---------------------------------------------------------------------------

def _haversine_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    radius = 6371.0

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)

    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad)
        * math.cos(lat2_rad)
        * math.sin(delta_lon / 2) ** 2
    )

    return radius * 2 * math.asin(
        math.sqrt(a)
    )


def get_grid_neighbours(
    grid_id: str,
    limit: int = 5,
) -> Optional[Dict[str, Any]]:

    connection = get_db_connection()

    try:
        cursor = connection.cursor(dictionary=True)

        query = """
            SELECT
                grid_id,
                centroid_latitude,
                centroid_longitude
            FROM dim_grid
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        target = None

        for row in rows:
            if str(row["grid_id"]) == str(grid_id):
                target = row
                break

        if target is None:
            return None

        neighbours = []

        for row in rows:
            if str(row["grid_id"]) == str(grid_id):
                continue

            distance = _haversine_km(
                float(target["centroid_latitude"]),
                float(target["centroid_longitude"]),
                float(row["centroid_latitude"]),
                float(row["centroid_longitude"]),
            )

            neighbours.append(
                {
                    "grid_id": row["grid_id"],
                    "distance_km": round(distance, 3),
                }
            )

        neighbours.sort(
            key=lambda item: item["distance_km"]
        )

        return {
            "grid_id": grid_id,
            "neighbours": neighbours[:limit],
        }

    finally:
        connection.close()