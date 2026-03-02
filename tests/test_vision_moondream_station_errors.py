"""Error handling tests for Moondream Station vision analyzer."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from PIL import Image
from requests import exceptions as req_exc  # type: ignore[attr-defined]

from src.ai.vision_moondream_station import MoondreamStationAnalyzer, MoondreamUnavailableError


@pytest.mark.fast
def test_analyze_image_raises_friendly_unavailable_error_on_connection_error(tmp_path: Path):
    """Connection errors from Moondream Station are mapped to a friendly MoondreamUnavailableError."""
    # Create a tiny dummy image file on disk.
    img_path = tmp_path / "img.jpg"
    image = Image.new("RGB", (2, 2), color="white")
    image.save(img_path)

    analyzer = MoondreamStationAnalyzer()

    with patch.object(
        analyzer._session,  # type: ignore[attr-defined]
        "post",
        side_effect=req_exc.ConnectionError("connection refused"),
    ):
        with pytest.raises(MoondreamUnavailableError) as excinfo:
            analyzer.analyze_image(img_path)

    message = str(excinfo.value)
    assert "Could not connect to Moondream Station at" in message
    assert "Make sure Moondream Station is running" in message


@pytest.mark.fast
def test_analyze_image_raises_runtimeerror_on_other_request_exception(tmp_path: Path):
    """Other RequestException errors are mapped to a concise RuntimeError."""
    img_path = tmp_path / "img.jpg"
    image = Image.new("RGB", (2, 2), color="white")
    image.save(img_path)

    analyzer = MoondreamStationAnalyzer()

    with patch.object(
        analyzer._session,  # type: ignore[attr-defined]
        "post",
        side_effect=req_exc.HTTPError("500 Server Error"),
    ):
        with pytest.raises(RuntimeError) as excinfo:
            analyzer.analyze_image(img_path)

    message = str(excinfo.value)
    assert "Moondream Station returned an error while analyzing the image." in message

