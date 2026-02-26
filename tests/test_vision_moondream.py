"""Tests for Moondream vision analyzer (tags parsing and dedup)."""

import pytest

from src.ai.vision_moondream import _parse_tags


@pytest.mark.fast
def test_parse_tags_dedupes_preserving_order():
    """_parse_tags returns unique tags in first-seen order."""
    result = _parse_tags("a, b, a, c, b")
    assert result == ["a", "b", "c"]
