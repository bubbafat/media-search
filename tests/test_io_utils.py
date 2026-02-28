"""Tests for io_utils."""

from pathlib import Path

import pytest

from src.core.io_utils import file_non_empty

pytestmark = [pytest.mark.fast]


def test_file_non_empty_missing_returns_false(tmp_path):
    """file_non_empty returns False when path does not exist."""
    p = tmp_path / "nonexistent"
    assert file_non_empty(p) is False


def test_file_non_empty_zero_bytes_returns_false(tmp_path):
    """file_non_empty returns False when file exists but has 0 bytes."""
    p = tmp_path / "empty"
    p.write_bytes(b"")
    assert file_non_empty(p) is False


def test_file_non_empty_nonzero_returns_true(tmp_path):
    """file_non_empty returns True when file has at least min_bytes."""
    p = tmp_path / "one"
    p.write_bytes(b"x")
    assert file_non_empty(p) is True
    assert file_non_empty(p, min_bytes=1) is True


def test_file_non_empty_min_bytes(tmp_path):
    """file_non_empty respects min_bytes parameter."""
    p = tmp_path / "three"
    p.write_bytes(b"abc")
    assert file_non_empty(p, min_bytes=2) is True
    assert file_non_empty(p, min_bytes=3) is True
    assert file_non_empty(p, min_bytes=4) is False
