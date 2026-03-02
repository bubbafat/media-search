"""Unit tests for face_detection module. MediaPipe is mocked; no real detector calls."""

import numpy as np
import pytest
from unittest.mock import MagicMock, patch

from src.metadata import face_detection

pytestmark = [pytest.mark.fast]


def test_detect_faces_empty_detections_returns_false_zero() -> None:
    """When detector returns no detections, (False, 0) is returned."""
    with patch.object(face_detection, "get_detector") as mock_get:
        mock_detector = MagicMock()
        mock_detector.detect.return_value = MagicMock(detections=[])
        mock_get.return_value = mock_detector

        has_face, face_count = face_detection.detect_faces(
            np.zeros((100, 100, 3), dtype=np.uint8)
        )

        assert has_face is False
        assert face_count == 0


def test_detect_faces_two_detections_returns_true_two() -> None:
    """When detector returns 2 detections, (True, 2) is returned."""
    with patch.object(face_detection, "get_detector") as mock_get:
        mock_detector = MagicMock()
        mock_detector.detect.return_value = MagicMock(
            detections=[MagicMock(), MagicMock()]
        )
        mock_get.return_value = mock_detector

        has_face, face_count = face_detection.detect_faces(
            np.zeros((100, 100, 3), dtype=np.uint8)
        )

        assert has_face is True
        assert face_count == 2


def test_get_detector_singleton_initialized_once() -> None:
    """get_detector returns the same instance across multiple calls."""
    face_detection._detector = None
    try:
        d1 = face_detection.get_detector()
        d2 = face_detection.get_detector()
        assert d1 is d2
    except (AttributeError, OSError, Exception) as e:
        if "solutions" in str(e) or "model" in str(e).lower() or "download" in str(e).lower():
            pytest.skip(f"mediapipe face detector not available: {e}")
        raise
