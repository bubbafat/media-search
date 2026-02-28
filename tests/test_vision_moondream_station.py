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
    """When _caption raises KeyError('caption'), analyze_image falls back to query for description."""
    from pathlib import Path

    from src.ai.vision_moondream_station import MoondreamStationAnalyzer

    post_calls = []

    def mock_post(path: str, json_payload: dict):
        post_calls.append((path, json_payload.get("question", json_payload.get("length", ""))))
        if path == "caption":
            # Non-standard response without "caption" key triggers KeyError in _caption
            return {}
        if path == "query":
            question = json_payload.get("question", "")
            if "Describe" in question:
                return {"answer": "fallback description from query"}
            if "tags" in question.lower():
                return {"answer": "outdoor, snow"}
            if "Extract" in question or "text" in question.lower():
                return {"answer": "None"}
            return {"answer": "unknown"}
        return {}

    with patch.dict("os.environ", {"MEDIASEARCH_MOONDREAM_STATION_ENDPOINT": "http://localhost:9999/v1"}):
        analyzer = MoondreamStationAnalyzer()
        with patch.object(analyzer, "_post", side_effect=mock_post):
            with patch("PIL.Image.open") as mock_open:
                mock_img = MagicMock()
                mock_img.mode = "RGB"
                mock_img.convert.return_value = mock_img
                mock_img.copy.return_value = mock_img
                mock_open.return_value.__enter__ = MagicMock(return_value=mock_img)
                mock_open.return_value.__exit__ = MagicMock(return_value=False)

                result = analyzer.analyze_image(Path("/tmp/test.jpg"))

    assert result.description == "fallback description from query"
    assert result.tags == ["outdoor", "snow"]
    assert result.ocr_text is None
    assert post_calls[0][0] == "caption"
    assert post_calls[1][0] == "query"
    assert "Describe this image briefly" in post_calls[1][1]
    assert len(post_calls) == 4  # caption + 3 query calls
