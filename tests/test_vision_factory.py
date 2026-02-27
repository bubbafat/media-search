"""Tests for the vision analyzer factory (get_vision_analyzer)."""

import sys
import warnings

import pytest

from src.ai.factory import get_vision_analyzer
from src.ai.vision_base import BaseVisionAnalyzer, MockVisionAnalyzer


@pytest.mark.fast
def test_get_vision_analyzer_mock_returns_mock_analyzer():
    """get_vision_analyzer('mock') returns a MockVisionAnalyzer."""
    analyzer = get_vision_analyzer("mock")
    assert isinstance(analyzer, MockVisionAnalyzer)
    assert isinstance(analyzer, BaseVisionAnalyzer)
    assert analyzer.get_model_card().name == "mock-analyzer"
    assert analyzer.get_model_card().version == "1.0"


@pytest.mark.ai
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


@pytest.mark.ai
def test_get_vision_analyzer_moondream3_returns_moondream3_analyzer():
    """get_vision_analyzer('moondream3') returns a Moondream3Analyzer."""
    pytest.importorskip("torch")
    pytest.importorskip("transformers")
    from src.ai.vision_moondream3 import Moondream3Analyzer

    try:
        analyzer = get_vision_analyzer("moondream3")
    except (ImportError, OSError) as e:
        pytest.skip(f"moondream3 model not loadable (missing deps or network): {e}")
    assert isinstance(analyzer, Moondream3Analyzer)
    assert isinstance(analyzer, BaseVisionAnalyzer)
    assert analyzer.get_model_card().name == "moondream3"
    assert analyzer.get_model_card().version == "preview"


@pytest.mark.ai
def test_get_vision_analyzer_moondream_station_returns_station_analyzer():
    """get_vision_analyzer('moondream-station') returns MoondreamStationAnalyzer when moondream is installed."""
    try:
        pytest.importorskip("moondream")
    except Exception:
        pytest.skip("moondream package not installed (install with: uv sync --extra station)")
    from src.ai.vision_moondream_station import MoondreamStationAnalyzer

    try:
        analyzer = get_vision_analyzer("moondream-station")
    except ValueError as e:
        pytest.skip(f"moondream-station not available: {e}")
    assert isinstance(analyzer, MoondreamStationAnalyzer)
    assert isinstance(analyzer, BaseVisionAnalyzer)
    assert analyzer.get_model_card().name == "moondream-station"
    assert analyzer.get_model_card().version == "local"


@pytest.mark.ai
def test_get_vision_analyzer_md3p_int4_returns_station_analyzer():
    """get_vision_analyzer('md3p-int4') returns MoondreamStationAnalyzer (alias for station)."""
    try:
        pytest.importorskip("moondream")
    except Exception:
        pytest.skip("moondream package not installed (install with: uv sync --extra station)")
    from src.ai.vision_moondream_station import MoondreamStationAnalyzer

    try:
        analyzer = get_vision_analyzer("md3p-int4")
    except ValueError as e:
        pytest.skip(f"md3p-int4 not available: {e}")
    assert isinstance(analyzer, MoondreamStationAnalyzer)
    assert analyzer.get_model_card().name == "moondream-station"


@pytest.mark.fast
def test_get_vision_analyzer_moondream_station_without_extra_raises_helpful_error():
    """When moondream is not installed, get_vision_analyzer('moondream-station') raises ValueError with install hint."""
    try:
        import moondream  # noqa: F401
        pytest.skip("moondream is installed; cannot test missing-extra path")
    except ImportError:
        pass
    with pytest.raises(ValueError, match=r"uv sync --extra station"):
        get_vision_analyzer("moondream-station")


@pytest.mark.ai
@pytest.mark.order(2)  # Run before other moondream2 test so warning is emitted and captured
@pytest.mark.skipif(sys.version_info < (3, 14), reason="torch.jit.script_method deprecation only emitted on Python 3.14+")
@pytest.mark.filterwarnings(
    "always:.*torch\\.jit\\.script_method.*Python 3\\.14.*:DeprecationWarning"
)
def test_moondream_torch_jit_deprecation_still_present():
    """Guard: fail when upstream removes this deprecation so we can remove the filter and this test."""
    pytest.importorskip("torch")
    pytest.importorskip("transformers")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        try:
            get_vision_analyzer("moondream2")
        except (ImportError, OSError):
            pytest.skip("Moondream2 not loadable in this environment")

        assert any(
            issubclass(item.category, DeprecationWarning)
            and "torch.jit.script_method" in str(item.message)
            and "Python 3.14" in str(item.message)
            for item in w
        ), "Upstream torch.jit.script_method deprecation warning is gone; remove the filter and this test."


@pytest.mark.fast
def test_get_vision_analyzer_unknown_raises():
    """get_vision_analyzer with unknown name raises ValueError."""
    with pytest.raises(ValueError, match=r"Unknown vision analyzer: unknown"):
        get_vision_analyzer("unknown")
