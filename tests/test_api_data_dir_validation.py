"""Tests for API data_dir mount validation."""

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = [pytest.mark.fast]


def _run_import_with_data_dir(data_dir: str) -> subprocess.CompletedProcess:
    """Run a subprocess that sets MEDIA_SEARCH_DATA_DIR and imports main."""
    script = f"""
import os
os.environ["MEDIA_SEARCH_DATA_DIR"] = {data_dir!r}
import src.api.main  # noqa: F401
"""
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parent.parent,
    )


def test_data_dir_cwd_raises_runtime_error():
    """Importing main with data_dir=. (cwd) raises RuntimeError."""
    result = _run_import_with_data_dir(".")
    assert result.returncode == 1
    assert "Unsafe data_dir" in (result.stdout + result.stderr)


def test_data_dir_root_raises_runtime_error():
    """Importing main with data_dir=/ raises RuntimeError."""
    result = _run_import_with_data_dir("/")
    assert result.returncode == 1
    assert "Unsafe data_dir" in (result.stdout + result.stderr)
