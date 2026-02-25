"""Tests for config (YAML), path_resolver, and FlightLogger handler."""

import io
import logging
import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from src.core import config as config_module
from src.core.config import get_config, reset_config
from src.core.logging import FlightLogger, FLIGHT_LOG_CAPACITY
from src.core.path_resolver import _reset_session_factory_for_tests, resolve_path


def test_settings_loads_from_yaml(tmp_path):
    """Settings loads correctly from a sample YAML."""
    yaml_path = tmp_path / "worker_config.yml"
    yaml_path.write_text("""
database_url: postgresql://localhost/testdb
library_roots:
  nas-main: /mnt/nas/main
  lib2: /data/lib2
worker_id: my-worker-1
log_level: DEBUG
""")
    reset_config()
    cfg = get_config(config_path=yaml_path)
    assert cfg.database_url == "postgresql://localhost/testdb"
    assert cfg.library_roots == {"nas-main": "/mnt/nas/main", "lib2": "/data/lib2"}
    assert cfg.worker_id == "my-worker-1"
    assert cfg.log_level == "DEBUG"


def test_settings_worker_id_auto_from_hostname(tmp_path):
    """When worker_id is missing in YAML, it is set from hostname."""
    yaml_path = tmp_path / "worker_config.yml"
    yaml_path.write_text("""
database_url: postgresql://localhost/db
library_roots: {}
""")
    reset_config()
    cfg = get_config(config_path=yaml_path)
    assert cfg.worker_id is not None
    assert len(cfg.worker_id) > 0


@pytest.fixture(scope="module")
def path_resolver_db():
    """Postgres container with migrations and a library row (slug=mylib) for path_resolver tests."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        url = postgres.get_connection_url()
        prev = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = url
        config_module._config = None  # type: ignore[attr-defined]
        try:
            from alembic import command
            from alembic.config import Config

            alembic_cfg = Config("alembic.ini")
            alembic_cfg.set_main_option("script_location", "migrations")
            command.upgrade(alembic_cfg, "head")

            engine = create_engine(url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(
                    text(
                        "INSERT INTO library (slug, name, absolute_path, is_active, scan_status, sampling_limit) "
                        "VALUES ('mylib', 'My Lib', '/tmp/placeholder', true, 'idle', 100)"
                    )
                )
                conn.commit()
            yield url
        finally:
            if prev is not None:
                os.environ["DATABASE_URL"] = prev
            else:
                os.environ.pop("DATABASE_URL", None)
            config_module._config = None  # type: ignore[attr-defined]


def test_resolve_path_joins_mapped_slug_and_verifies_exists(tmp_path, path_resolver_db):
    """resolve_path returns absolute path for mapped slug when file exists; raises for unknown slug."""
    lib_root = tmp_path / "library_root"
    lib_root.mkdir()
    (lib_root / "some").mkdir()
    (lib_root / "some" / "rel.txt").write_text("ok")

    url = path_resolver_db
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE library SET absolute_path = :p WHERE slug = 'mylib'"),
            {"p": str(lib_root.resolve())},
        )
        conn.commit()

    yaml_path = tmp_path / "worker_config.yml"
    yaml_path.write_text(f"database_url: {url}\n")
    reset_config()
    get_config(config_path=yaml_path)
    _reset_session_factory_for_tests()

    result = resolve_path("mylib", "some/rel.txt")
    assert result == (lib_root / "some" / "rel.txt").resolve()
    assert result.exists()
    assert result.read_text() == "ok"

    with pytest.raises(ValueError, match="Unknown library slug"):
        resolve_path("unknown_slug", "x")


def test_resolve_path_raises_for_missing_file(tmp_path, path_resolver_db):
    """resolve_path raises FileNotFoundError when the resolved path does not exist."""
    lib_root = tmp_path / "library_root"
    lib_root.mkdir()
    url = path_resolver_db
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE library SET absolute_path = :p WHERE slug = 'mylib'"),
            {"p": str(lib_root.resolve())},
        )
        conn.commit()
    yaml_path = tmp_path / "worker_config.yml"
    yaml_path.write_text(f"database_url: {url}\n")
    reset_config()
    get_config(config_path=yaml_path)
    _reset_session_factory_for_tests()

    with pytest.raises(FileNotFoundError, match="Path does not exist"):
        resolve_path("mylib", "nonexistent.txt")


def test_resolve_path_rejects_traversal(tmp_path, path_resolver_db):
    """resolve_path raises ValueError when rel_path escapes library root."""
    lib_root = tmp_path / "library_root"
    lib_root.mkdir()
    url = path_resolver_db
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE library SET absolute_path = :p WHERE slug = 'mylib'"),
            {"p": str(lib_root.resolve())},
        )
        conn.commit()
    yaml_path = tmp_path / "worker_config.yml"
    yaml_path.write_text(f"database_url: {url}\n")
    reset_config()
    get_config(config_path=yaml_path)
    _reset_session_factory_for_tests()

    with pytest.raises(ValueError, match="escapes library root"):
        resolve_path("mylib", "../../etc/passwd")


def test_flight_logger_stores_all_levels_in_memory_until_dump(tmp_path):
    """FlightLogger stores all levels (DEBUG, INFO, WARNING, ERROR) in memory and does not write to disk until dump() is called."""
    forensics_dir = tmp_path / "logs" / "forensics"
    handler = FlightLogger(capacity=100, forensics_dir=forensics_dir)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)

    assert forensics_dir.exists() is False

    logging.debug("msg-debug")
    logging.info("msg-info")
    logging.warning("msg-warning")
    logging.error("msg-error")

    assert len(handler) == 4

    path = handler.dump("test-worker")
    assert Path(path).exists()
    content = Path(path).read_text()
    assert "msg-debug" in content
    assert "msg-info" in content
    assert "msg-warning" in content
    assert "msg-error" in content

    root.removeHandler(handler)


def test_flight_logger_gets_all_levels_console_only_above_config(tmp_path):
    """With root at DEBUG and console at INFO: DEBUG goes only to flight log; INFO goes to flight log and console."""
    forensics_dir = tmp_path / "logs" / "forensics"
    handler = FlightLogger(capacity=100, forensics_dir=forensics_dir)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(message)s"))
    stream = io.StringIO()
    console = logging.StreamHandler(stream)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)
    root.setLevel(logging.DEBUG)
    root.addHandler(handler)
    root.addHandler(console)

    logging.debug("only-in-flight")
    logging.info("in-flight-and-console")

    root.removeHandler(handler)
    root.removeHandler(console)

    assert len(handler) == 2
    path = handler.dump("routing-test")
    content = Path(path).read_text()
    assert "only-in-flight" in content
    assert "in-flight-and-console" in content
    console_out = stream.getvalue()
    assert "only-in-flight" not in console_out
    assert "in-flight-and-console" in console_out


def test_flight_logger_only_last_50000_after_60000_messages(tmp_path):
    """After 60,000 log lines, dump contains only the last 50,000 entries."""
    forensics_dir = tmp_path / "logs" / "forensics"
    handler = FlightLogger(capacity=FLIGHT_LOG_CAPACITY, forensics_dir=forensics_dir)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)

    for i in range(60_000):
        logging.info("line-%d", i)

    assert len(handler) == 50_000

    path = handler.dump("cap-test")
    lines = Path(path).read_text().strip().split("\n")
    assert len(lines) == 50_000
    # Oldest retained message should be line-10000 (the 10001st message, index 10000)
    assert "line-10000" in lines[0] or lines[0].endswith("line-10000")
    assert "line-59999" in lines[-1] or lines[-1].endswith("line-59999")

    root.removeHandler(handler)
