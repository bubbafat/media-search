"""Unit tests for sharpness module (no DB, no I/O beyond path for compute_sharpness)."""

from pathlib import Path

import numpy as np
import pytest

from src.metadata.sharpness import compute_sharpness, compute_sharpness_from_array

pytestmark = [pytest.mark.fast]


def test_compute_sharpness_from_array_solid_color_returns_low_value() -> None:
    """Solid-color array has zero Laplacian variance; result is in [0, 1] and close to 0."""
    arr = np.zeros((32, 32, 3), dtype=np.uint8)
    arr[:] = (128, 128, 128)
    score = compute_sharpness_from_array(arr)
    assert isinstance(score, float)
    assert 0.0 <= score <= 1.0
    assert score < 0.01


def test_compute_sharpness_from_array_checkerboard_higher_than_solid() -> None:
    """Checkerboard pattern has higher Laplacian variance than solid color."""
    solid = np.ones((32, 32, 3), dtype=np.uint8) * 128
    # 8x8 checkerboard
    check = np.zeros((32, 32, 3), dtype=np.uint8)
    for i in range(32):
        for j in range(32):
            check[i, j] = 255 if ((i // 8) + (j // 8)) % 2 == 0 else 0
    score_solid = compute_sharpness_from_array(solid)
    score_check = compute_sharpness_from_array(check)
    assert score_check > score_solid


def test_compute_sharpness_from_array_result_clamped_to_unit_interval() -> None:
    """Result is always in [0.0, 1.0] even for high-variance images."""
    # High-contrast fine pattern can yield variance > SHARPNESS_MAX_VARIANCE; should clamp to 1.0
    arr = np.zeros((64, 64, 3), dtype=np.uint8)
    for i in range(64):
        for j in range(64):
            arr[i, j] = 255 if (i + j) % 2 == 0 else 0
    score = compute_sharpness_from_array(arr)
    assert 0.0 <= score <= 1.0


def test_compute_sharpness_raises_value_error_for_nonexistent_path(tmp_path: Path) -> None:
    """compute_sharpness raises ValueError when the path does not exist."""
    missing = tmp_path / "does_not_exist.jpg"
    assert not missing.exists()
    with pytest.raises(ValueError, match="Cannot read image"):
        compute_sharpness(missing)
