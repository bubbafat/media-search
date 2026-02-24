"""Application configuration (Pydantic Settings)."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """App config; load from env / .env."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database (PostgreSQL 16+)
    database_url: str = "postgresql+psycopg://localhost/media_search"

    # Logging
    log_level: str = "INFO"


_config: Config | None = None


def get_config() -> Config:
    """Return singleton config instance."""
    global _config
    if _config is None:
        _config = Config()
    return _config
