"""Sharpness scoring via Laplacian variance (OpenCV)."""

from pathlib import Path

import cv2
import numpy as np

from src.core.config import get_config

SHARPNESS_MAX_VARIANCE = 1000.0


def compute_sharpness(image_path: Path) -> float:
    """
    Compute normalized sharpness score from a JPEG (or other image) file.

    Uses Laplacian variance; result is clamped to [0.0, 1.0].
    Raises ValueError if the file cannot be read.
    """
    img = cv2.imread(str(image_path))
    if img is None:
        raise ValueError(f"Cannot read image: {image_path}")
    return compute_sharpness_from_array(img)


def compute_sharpness_from_array(img_bgr: np.ndarray) -> float:
    """
    Compute normalized sharpness score from a BGR numpy array (e.g. from cv2.imread).

    Uses Laplacian variance; result is in [0.0, 1.0].
    """
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    laplacian = cv2.Laplacian(gray, cv2.CV_64F, ksize=3)
    variance = float(laplacian.var())
    max_variance = get_config().sharpness_max_variance
    normalized = min(1.0, variance / max_variance)
    return max(0.0, normalized)
