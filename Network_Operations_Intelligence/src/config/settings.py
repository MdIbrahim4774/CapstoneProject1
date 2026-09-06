"""
Application configuration.

Network Operations Intelligence
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    """

    mysql_host: str = 'localhost'
    mysql_port: int = 3306
    mysql_user: str = 'root'
    mysql_password: str = 'root'
    mysql_database: str = 'network_operations'

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    """
    Return cached application settings.
    """
    return Settings()