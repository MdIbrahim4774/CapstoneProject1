"""
MySQL database connection.

Network Operations Intelligence
"""

from __future__ import annotations

from mysql.connector import MySQLConnection

from src.config.settings import get_settings


def get_connection() -> MySQLConnection:
    """
    Create a connection to the analytics warehouse.
    """

    settings = get_settings()

    connection = MySQLConnection(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_database,
    )

    if not connection.is_connected():
        raise RuntimeError(
            "Unable to connect to the analytics warehouse."
        )

    return connection