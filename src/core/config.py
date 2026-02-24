"""Application configuration (Pydantic v2). Load from worker_config.yml with optional env override."""

import os
import socket
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, field_validator


class Settings(BaseModel):
    """Worker config loaded from YAML. database_url may be overridden by env DATABASE_URL."""

    model_config = {"extra": "ignore"}

    database_url: str = "postgresql+psycopg://localhost/media_search"
    library_roots: dict[str, str] = {}
    worker_id: str | None = None
    log_level: str = "INFO"

    @field_validator("worker_id", mode="before")
    @classmethod
    def default_worker_id(cls, v: Any) -> str | None:
        if v is not None and v != "":
            return str(v)
        return None


_config: Settings | None = None


def _load_settings_from_yaml(path: str | Path, apply_env_override: bool = True) -> Settings:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as f:
        data = yaml.safe_load(f)
    if not data:
        data = {}
    # Allow env override for database_url only when loading default config (not when tests pass explicit path)
    if apply_env_override and os.environ.get("DATABASE_URL"):
        data["database_url"] = os.environ["DATABASE_URL"]
    settings = Settings.model_validate(data)
    if settings.worker_id is None or settings.worker_id == "":
        settings = settings.model_copy(update={"worker_id": socket.gethostname()})
    return settings


def get_config(config_path: str | Path | None = None) -> Settings:
    """Return singleton config. If config_path given, load from it. Else use cache or WORKER_CONFIG / worker_config.yml."""
    global _config
    if config_path is not None:
        _config = _load_settings_from_yaml(config_path, apply_env_override=False)
        return _config
    if _config is not None:
        return _config
    path = os.environ.get("WORKER_CONFIG") or "worker_config.yml"
    if Path(path).exists():
        _config = _load_settings_from_yaml(path)
    else:
        _config = Settings(worker_id=socket.gethostname())
        if os.environ.get("DATABASE_URL"):
            _config = _config.model_copy(update={"database_url": os.environ["DATABASE_URL"]})
    return _config


def reset_config() -> None:
    """Clear cached config (for tests)."""
    global _config
    _config = None
