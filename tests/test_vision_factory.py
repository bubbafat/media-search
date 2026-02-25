"""Tests for the vision analyzer factory (get_vision_analyzer)."""

import pytest

from src.ai.factory import get_vision_analyzer
from src.ai.vision_base import BaseVisionAnalyzer, MockVisionAnalyzer


def test_get_vision_analyzer_mock_returns_mock_analyzer():
    """get_vision_analyzer('mock') returns a MockVisionAnalyzer."""
    analyzer = get_vision_analyzer("mock")
    assert isinstance(analyzer, MockVisionAnalyzer)
    assert isinstance(analyzer, BaseVisionAnalyzer)
    assert analyzer.get_model_card().name == "mock-analyzer"
    assert analyzer.get_model_card().version == "1.0"


def test_get_vision_analyzer_moondream2_returns_moondream_analyzer():
    """get_vision_analyzer('moondream2') returns a MoondreamAnalyzer."""
    pytest.importorskip("torch")
    pytest.importorskip("transformers")
    from src.ai.vision_moondream import MoondreamAnalyzer

    try:
        analyzer = get_vision_analyzer("moondream2")
    except (ImportError, OSError) as e:
        pytest.skip(f"moondream2 model not loadable (missing deps or network): {e}")
    assert isinstance(analyzer, MoondreamAnalyzer)
    assert isinstance(analyzer, BaseVisionAnalyzer)
    assert analyzer.get_model_card().name == "moondream2"
    assert analyzer.get_model_card().version == "2025-01-09"


def test_get_vision_analyzer_unknown_raises():
    """get_vision_analyzer with unknown name raises ValueError."""
    with pytest.raises(ValueError, match=r"Unknown vision analyzer: unknown"):
        get_vision_analyzer("unknown")
