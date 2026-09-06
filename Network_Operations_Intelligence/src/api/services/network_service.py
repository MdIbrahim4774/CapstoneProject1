"""
Network services.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from mysql.connector import MySQLConnection

from src.db.queries.network_queries import (
    GET_MAX_TIMESTAMP,
    GET_NETWORK_SUMMARY,
    GET_GRID_ACTIVITY,
    GET_HOTSPOTS,
    GET_ALERTS,
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
        cursor.execute(
            GET_NETWORK_SUMMARY,
            (effective_as_of,),
        )

        row = cursor.fetchone()

        if row is None:
            raise RuntimeError(
                "Unable to calculate network summary."
            )

        if row["peak_hour"] is None:
            raise RuntimeError(
                "Analytics layer contains no activity data."
            )

        if row["top_grid"] is None:
            raise RuntimeError(
                "Analytics layer contains no grid data."
            )

        return {
            "total_activity": float(
                row["total_activity"] or 0
            ),
            "active_grids": int(
                row["active_grids"] or 0
            ),
            "peak_hour": int(
                row["peak_hour"]
            ),
            "top_grid": str(
                row["top_grid"]
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