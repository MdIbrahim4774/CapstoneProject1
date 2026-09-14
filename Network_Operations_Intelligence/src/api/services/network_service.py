"""
Network services.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from typing import Any

from mysql.connector import MySQLConnection

from src.db.queries.network_queries import (
    GET_MAX_TIMESTAMP,
    GET_TOTAL_ACTIVITY,
    GET_ACTIVE_GRIDS,
    GET_PEAK_HOUR,
    GET_TOP_GRID,
    GET_GRID_ACTIVITY,
    GET_HOTSPOTS,
    GET_ALERTS,
    GET_GRID_FEATURES,
    GET_LATEST_FEATURE_TIMESTAMP,
)


def _get_effective_as_of(
    connection: MySQLConnection,
    requested_as_of: datetime | None,
) -> datetime:
    """
    Resolve the effective reporting timestamp.

    If the caller supplies as_of, use it.
    Otherwise use the latest timestamp available in the warehouse.
    """

    if requested_as_of is not None:
        return requested_as_of

    cursor = connection.cursor(dictionary=True)

    try:
        cursor.execute(GET_MAX_TIMESTAMP)

        row = cursor.fetchone()

        if row is None or row["as_of"] is None:
            raise RuntimeError(
                "Analytics layer contains no timestamp data."
            )

        return row["as_of"]

    finally:
        cursor.close()


# ============================================================================
# API1 - Network Summary
# ============================================================================

def get_network_summary(
    connection: MySQLConnection,
    requested_as_of: datetime | None = None,
) -> dict:
    """
    Return the network summary for the effective reporting timestamp.
    """

    effective_as_of = _get_effective_as_of(
        connection,
        requested_as_of,
    )

    cursor = connection.cursor(dictionary=True)

    try:
        # ---------------------------------------------------------
        # 1. Total activity
        # ---------------------------------------------------------
        cursor.execute(
            GET_TOTAL_ACTIVITY,
            (effective_as_of,),
        )

        total_row = cursor.fetchone()

        if total_row is None:
            raise RuntimeError(
                "Unable to calculate total activity."
            )

        # ---------------------------------------------------------
        # 2. Active grids
        # ---------------------------------------------------------
        cursor.execute(
            GET_ACTIVE_GRIDS,
            (effective_as_of,),
        )

        active_row = cursor.fetchone()

        if active_row is None:
            raise RuntimeError(
                "Unable to calculate active grids."
            )

        # ---------------------------------------------------------
        # 3. Peak hour
        # ---------------------------------------------------------
        cursor.execute(
            GET_PEAK_HOUR,
            (effective_as_of,),
        )

        peak_row = cursor.fetchone()

        if peak_row is None:
            raise RuntimeError(
                "Unable to calculate peak hour."
            )

        # ---------------------------------------------------------
        # 4. Top grid
        # ---------------------------------------------------------
        cursor.execute(
            GET_TOP_GRID,
            (effective_as_of,),
        )

        top_grid_row = cursor.fetchone()

        if top_grid_row is None:
            raise RuntimeError(
                "Unable to calculate top grid."
            )

        return {
            "total_activity": float(
                total_row["total_activity"] or 0
            ),
            "active_grids": int(
                active_row["active_grids"] or 0
            ),
            "peak_hour": int(
                peak_row["hour"]
            ),
            "top_grid": str(
                top_grid_row["grid_id"]
            ),
            "as_of": effective_as_of,
        }

    finally:
        cursor.close()

# ============================================================================
# API2 - Grid Activity
# ============================================================================

def get_grid_activity(
    connection: MySQLConnection,
    grid_id: int,
    requested_date: str | None = None,
    requested_hour: int | None = None,
    requested_as_of: datetime | None = None,
) -> dict:
    """
    Return hourly activity for a grid over the effective 24-hour window.
    """

    effective_as_of = _get_effective_as_of(
        connection,
        requested_as_of,
    )

    query = GET_GRID_ACTIVITY

    params = [
        grid_id,
        effective_as_of - timedelta(hours=23),
        effective_as_of,
    ]

    if requested_date is not None:
        query = query.replace(
            "GROUP BY",
            "AND DATE(t.timestamp) = %s\nGROUP BY",
        )

        params.append(requested_date)

    if requested_hour is not None:
        query = query.replace(
            "GROUP BY",
            "AND t.hour = %s\nGROUP BY",
        )

        params.append(requested_hour)

    cursor = connection.cursor(dictionary=True)

    try:
        cursor.execute(
            query,
            tuple(params),
        )

        rows = cursor.fetchall()

        return {
            "grid_id": grid_id,
            "activity": rows,
            "as_of": effective_as_of,
        }

    finally:
        cursor.close()


# ============================================================================
# API3 - Hotspots
# ============================================================================

def get_hotspots(
    connection: MySQLConnection,
    limit: int = 20,
    requested_as_of: datetime | None = None,
) -> dict:
    """
    Return the highest-activity grids for the effective timestamp.
    """

    effective_as_of = _get_effective_as_of(
        connection,
        requested_as_of,
    )

    cursor = connection.cursor(dictionary=True)

    try:
        cursor.execute(
            GET_HOTSPOTS,
            (
                effective_as_of,
                limit,
            ),
        )

        rows = cursor.fetchall()

        hotspots = []

        for row in rows:
            hotspots.append(
                {
                    "grid_id": int(row["grid_id"]),
                    "timestamp": row["timestamp"],

                    "call_activity": float(
                        row["call_activity"] or 0
                    ),

                    "sms_activity": float(
                        row["sms_activity"] or 0
                    ),

                    "internet_activity": float(
                        row["internet_activity"] or 0
                    ),

                    "total_activity": float(
                        row["total_activity"] or 0
                    ),

                    "status": "HOTSPOT",

                    "reason": (
                        "High network activity detected."
                    ),

                    "source": "analytics",

                    "risk": None,
                }
            )

        return {
            "as_of": effective_as_of,
            "count": len(hotspots),
            "items": hotspots,
        }

    finally:
        cursor.close()

# ============================================================================
# API3 - Alerts
# ============================================================================

def get_alerts(
    connection: MySQLConnection,
    limit: int = 20,
    severity: str | None = None,
    requested_as_of: datetime | None = None,
) -> dict:
    effective_as_of = _get_effective_as_of(
        connection,
        requested_as_of,
    )

    cursor = connection.cursor(dictionary=True)

    try:
        cursor.execute(
            GET_ALERTS,
            (
                effective_as_of,
                limit,
            ),
        )

        rows = cursor.fetchall()

        alerts = []

        for row in rows:
            alerts.append({
                "grid_id": int(row["grid_id"]),
                "timestamp": row["timestamp"],
                "call_activity": float(row["call_activity"] or 0),
                "sms_activity": float(row["sms_activity"] or 0),
                "internet_activity": float(row["internet_activity"] or 0),
                "total_activity": float(row["total_activity"] or 0),
                "status": str(row["status"]),
                "severity": str(row["severity"]),
                "reason": str(row["reason"]),
                "source": "rule",
                "risk": None,
            })

        # Apply severity filtering only if the SQL query
        # does not already do it.
        if severity is not None:
            normalized = severity.upper()
            alerts = [
                alert
                for alert in alerts
                if alert["severity"].upper() == normalized
            ]

        return {
            "as_of": effective_as_of,
            "count": len(alerts),
            "items": alerts,
        }

    finally:
        cursor.close()


def _format_timestamp(value: Any) -> str:
    """
    Convert database datetime/date values into the stable API string format.
    """
    if isinstance(value, datetime):
        return value.isoformat()

    return str(value)


def get_grid_features(
    connection,
    grid_id: str,
    start_time: datetime,
    end_time: datetime,
):
    """
    Read ML2 features for a grid and time window.

    This function deliberately does NOT calculate any features.
    """

    cursor = connection.cursor(dictionary=True)

    try:
        # ---------------------------------------------------------
        # Read stored ML2 feature values
        # ---------------------------------------------------------
        cursor.execute(
            GET_GRID_FEATURES,
            (
                grid_id,
                start_time,
                end_time,
            ),
        )

        rows = cursor.fetchall()

        # ---------------------------------------------------------
        # Find latest feature timestamp for freshness calculation
        # ---------------------------------------------------------
        cursor.execute(GET_LATEST_FEATURE_TIMESTAMP)

        result = cursor.fetchone()

        if isinstance(result, dict):
            latest_timestamp = next(iter(result.values()))
        else:
            latest_timestamp = result[0] if result else None

        features = []

        for row in rows:
            feature_timestamp = row["feature_timestamp"]

            # -----------------------------------------------------
            # Freshness
            # -----------------------------------------------------
            if (
                latest_timestamp is not None
                and feature_timestamp == latest_timestamp
            ):
                freshness = "FRESH"
            else:
                freshness = "STALE"

            # -----------------------------------------------------
            # Data quality
            # -----------------------------------------------------
            feature_columns = [
                "avg_activity",
                "activity_growth",
                "active_hours",
                "peak_ratio",
                "variability",
                "internet_share",
            ]

            quality = "GOOD"

            if any(row[column] is None for column in feature_columns):
                quality = "DEGRADED"

            # -----------------------------------------------------
            # API response
            # -----------------------------------------------------
            features.append(
                {
                    "avg_activity": float(row["avg_activity"]),
                    "activity_growth": float(row["activity_growth"]),
                    "active_hours": int(row["active_hours"]),
                    "peak_ratio": float(row["peak_ratio"]),
                    "variability": float(row["variability"]),
                    "internet_share": float(row["internet_share"]),
                    "feature_timestamp": _format_timestamp(
                        feature_timestamp
                    ),
                    "data_quality": quality,
                    "feature_freshness": freshness,
                }
            )

        return {
            "grid_id": grid_id,
            "features": features,
        }

    finally:
        cursor.close()