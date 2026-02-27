"""Factory for vision analyzers. Uses lazy imports so PyTorch is not loaded until needed."""

from src.ai.vision_base import BaseVisionAnalyzer


def get_vision_analyzer(analyzer_name: str) -> BaseVisionAnalyzer:
    """Return a vision analyzer by name. Imports are lazy to avoid loading PyTorch until requested."""
    if analyzer_name == "mock":
        from src.ai.vision_base import MockVisionAnalyzer

        return MockVisionAnalyzer()
    if analyzer_name == "moondream2":
        from src.ai.vision_moondream import MoondreamAnalyzer

        return MoondreamAnalyzer()
    if analyzer_name == "moondream3":
        from src.ai.vision_moondream3 import Moondream3Analyzer

        return Moondream3Analyzer()
    raise ValueError(f"Unknown vision analyzer: {analyzer_name}")
