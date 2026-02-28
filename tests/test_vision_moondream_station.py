"""Tests for Moondream Station vision analyzer (caption fallback, etc)."""

import pytest
from unittest.mock import MagicMock, patch

from src.ai.vision_moondream_station import _parse_tags


@pytest.mark.fast
def test_parse_tags_dedupes_preserving_order():
    """_parse_tags returns unique tags in first-seen order."""
    result = _parse_tags("a, b, a, c, b")
    assert result == ["a", "b", "c"]


@pytest.mark.fast
def test_analyze_image_falls_back_to_query_when_caption_raises_keyerror():
    """When caption() raises KeyError('caption'), analyze_image falls back to query for description."""
    pytest.importorskip("moondream")
    from pathlib import Path

    from src.ai.vision_moondream_station import MoondreamStationAnalyzer

    calls = []

    def mock_query(image, question, *, reasoning=False):
        calls.append(question)
        if "Describe" in question:
            return {"answer": "fallback description from query"}
        if "tags" in question.lower():
            return {"answer": "outdoor, snow"}
        if "Extract" in question or "text" in question.lower():
            return {"answer": "None"}
        return {"answer": "unknown"}

    with patch.dict("os.environ", {"MEDIASEARCH_MOONDREAM_STATION_ENDPOINT": "http://localhost:9999/v1"}):
        analyzer = MoondreamStationAnalyzer()
        mock_model = MagicMock()
        mock_model.caption.side_effect = KeyError("caption")
        mock_model.query.side_effect = mock_query
        analyzer._model = mock_model

    with patch("PIL.Image.open") as mock_open:
        mock_img = MagicMock()
        mock_img.mode = "RGB"
        mock_open.return_value = mock_img

        result = analyzer.analyze_image(Path("/tmp/test.jpg"))

    assert result.description == "fallback description from query"
    assert result.tags == ["outdoor", "snow"]
    assert result.ocr_text is None
    assert mock_model.caption.called
    assert mock_model.query.call_count == 3
    assert "Describe this image briefly" in calls[0]
