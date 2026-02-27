"""Application configuration (Pydantic v2). Load from worker_config.yml with optional env override."""

import os
import socket
from pathlib import Path
from typing import Any, Mapping

import yaml
from pydantic import BaseModel, field_validator


DEFAULT_DATABASE_URL = "postgresql+psycopg2://localhost/media_search"
DEFAULT_CONFIG_ENV_VAR = "WORKER_CONFIG"
DEFAULT_CONFIG_FILENAME = "worker_config.yml"


class Settings(BaseModel):
    """
    Worker config loaded from YAML.

    By default, the database_url may be overridden by the DATABASE_URL environment variable
    when loading the default config (but not when an explicit config_path is provided).
    """

    model_config = {"extra": "ignore"}

    database_url: str = DEFAULT_DATABASE_URL
    data_dir: str = "./data"
    export_root_path: str | None = None
    library_roots: dict[str, str] = {}
    worker_id: str | None = None
    log_level: str = "INFO"
    forensics_dir: str = "/logs/forensics"

    @field_validator("worker_id", mode="before")
    @classmethod
    def default_worker_id(cls, v: Any) -> str | None:
        if v is not None and v != "":
            return str(v)
        return None


_config: Settings | None = None


class ConfigLoader:
    """
    Helper responsible for loading Settings from YAML and environment.

    - load_from_yaml(path, apply_env_override): read a YAML file and optionally apply env overrides.
    - load_default(): resolve the default config path from WORKER_CONFIG / worker_config.yml and
      apply DATABASE_URL and MEDIA_SEARCH_DATA_DIR overrides when present.
    """

    def __init__(self, env: Mapping[str, str] | None = None) -> None:
        self._env: Mapping[str, str] = env or os.environ

    def load_from_yaml(self, path: Path, apply_env_override: bool) -> Settings:
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path) as f:
            data = yaml.safe_load(f)
        if not data:
            data = {}
        if apply_env_override:
            if self._env.get("DATABASE_URL"):
                data["database_url"] = self._env["DATABASE_URL"]
            if self._env.get("MEDIA_SEARCH_DATA_DIR"):
                data["data_dir"] = self._env["MEDIA_SEARCH_DATA_DIR"]
            if self._env.get("EXPORT_ROOT_PATH"):
                data["export_root_path"] = self._env["EXPORT_ROOT_PATH"]
        settings = Settings.model_validate(data)
        if settings.worker_id is None or settings.worker_id == "":
            settings = settings.model_copy(update={"worker_id": socket.gethostname()})
        return settings

    def load_default(self) -> Settings:
        """
        Load the default Settings, using WORKER_CONFIG or worker_config.yml.

        When no explicit config_path is provided, DATABASE_URL and MEDIA_SEARCH_DATA_DIR (if set)
        override the YAML values or defaults.
        """
        path_str = self._env.get(DEFAULT_CONFIG_ENV_VAR) or DEFAULT_CONFIG_FILENAME
        path = Path(path_str)
        if path.exists():
            return self.load_from_yaml(path, apply_env_override=True)

        settings = Settings(worker_id=socket.gethostname())
        overrides: dict[str, str] = {}
        if self._env.get("DATABASE_URL"):
            overrides["database_url"] = self._env["DATABASE_URL"]
        if self._env.get("MEDIA_SEARCH_DATA_DIR"):
            overrides["data_dir"] = self._env["MEDIA_SEARCH_DATA_DIR"]
        if self._env.get("EXPORT_ROOT_PATH"):
            overrides["export_root_path"] = self._env["EXPORT_ROOT_PATH"]
        if overrides:
            settings = settings.model_copy(update=overrides)
        return settings


_loader = ConfigLoader()


def get_config(config_path: str | Path | None = None) -> Settings:
    """
    Return singleton config.

    - If config_path is given, load from it (without env overrides for database_url) and update the cache.
    - Otherwise, return the cached config if available, or load via ConfigLoader.load_default().
    """
    global _config
    if config_path is not None:
        _config = _loader.load_from_yaml(Path(config_path), apply_env_override=False)
        return _config
    if _config is not None:
        return _config
    _config = _loader.load_default()
    return _config


def reset_config() -> None:
    """Clear cached config (for tests)."""
    global _config
    _config = None
