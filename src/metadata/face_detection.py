"""Face detection via MediaPipe Face Detection (not Face Landmarker)."""

import os
from pathlib import Path
from typing import Any

import cv2
import numpy as np

# MediaPipe 0.10+ uses tasks.python.vision; older used solutions (removed in 0.10).
_FACE_DETECTOR_MODEL_URL = (
    "https://storage.googleapis.com/mediapipe-models/face_detector/blaze_face_short_range/float16/1/blaze_face_short_range.tflite"
)

_detector: Any = None


def _get_model_path() -> Path:
    """Return path to cached face detector model; download if missing."""
    cache_dir = Path(os.environ.get("XDG_CACHE_HOME", os.path.expanduser("~/.cache"))) / "media_search"
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / "blaze_face_short_range.tflite"
    if path.exists():
        return path
    import urllib.request

    urllib.request.urlretrieve(_FACE_DETECTOR_MODEL_URL, path)
    return path


def get_detector():
    """Return the singleton MediaPipe FaceDetection instance. Lazy-init with min_detection_confidence=0.5."""
    global _detector
    if _detector is None:
        from mediapipe.tasks.python.core import base_options as base_options_lib
        from mediapipe.tasks.python.vision import FaceDetector, FaceDetectorOptions
        from mediapipe.tasks.python.vision.core import vision_task_running_mode

        model_path = str(_get_model_path())
        base_options = base_options_lib.BaseOptions(model_asset_path=model_path)
        options = FaceDetectorOptions(
            base_options=base_options,
            running_mode=vision_task_running_mode.VisionTaskRunningMode.IMAGE,
            min_detection_confidence=0.5,
        )
        _detector = FaceDetector.create_from_options(options)
    return _detector


def detect_faces(image_bgr: np.ndarray) -> tuple[bool, int]:
    """
    Run face detection on a BGR image (e.g. from cv2.imread).

    Returns (has_face, face_count).
    """
    from mediapipe.tasks.python.vision.core import image as image_module

    rgb = cv2.cvtColor(image_bgr, cv2.COLOR_BGR2RGB)
    mp_image = image_module.Image(image_module.ImageFormat.SRGB, rgb)
    result = get_detector().detect(mp_image)
    detections = result.detections if result.detections else []
    face_count = len(detections)
    return (face_count > 0, face_count)
