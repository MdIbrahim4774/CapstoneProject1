"""
Network summary service.

Contains business logic for API1.
"""

from __future__ import annotations

from datetime import datetime

from mysql.connector import MySQLConnection

from src.db.queries.network_queries import (
    GET_MAX_TIMESTAMP,
    GET_NETWORK_SUMMARY,
)


def _get_effective_as_of(
    connection: MySQLConnection,
    requested_as_of: datetime | None,
) -> datetime:
    """
    Resolve the effective reporting timestamp.

    Explicit as_of:
        use the supplied timestamp.

    Missing as_of:
        use MAX(timestamp) from the analytics layer.
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


def get_network_summary(
    connection: MySQLConnection,
    requested_as_of: datetime | None = None,
) -> dict:
    """
    Return network KPIs through the effective as_of timestamp.
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