"""Abstract base and mock implementation for vision analyzers."""

import time
from abc import ABC, abstractmethod
from pathlib import Path

from src.ai.schema import ModelCard, VisualAnalysis


class BaseVisionAnalyzer(ABC):
    """Abstract base for image analysis (description, tags, OCR)."""

    @abstractmethod
    def get_model_card(self) -> ModelCard:
        """Return model identity (name, version)."""
        ...

    @abstractmethod
    def analyze_image(self, image_path: Path) -> VisualAnalysis:
        """Analyze image at path; return description, tags, and optional OCR text."""
        ...


class MockVisionAnalyzer(BaseVisionAnalyzer):
    """Placeholder analyzer for testing and development."""

    def get_model_card(self) -> ModelCard:
        return ModelCard(name="mock-analyzer", version="1.0")

    def analyze_image(self, image_path: Path) -> VisualAnalysis:
        time.sleep(0.5)
        return VisualAnalysis(
            description="A placeholder description.",
            tags=["mock", "test"],
            ocr_text="MOCK TEXT",
        )
