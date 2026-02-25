"""Pydantic data contracts for AI models and visual analysis."""

from pydantic import BaseModel, Field


class ModelCard(BaseModel):
    """Metadata identifying an AI/vision model."""

    name: str
    version: str


class VisualAnalysis(BaseModel):
    """Result of running a vision model on an image."""

    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    ocr_text: str | None = None
