"""Vision analyzer that uses a local Moondream Station server (e.g. md3p-int4 via MLX).

This module is the only place that imports the moondream client. Install with:
  uv sync --extra station

Requires Moondream Station to be running (e.g. moondream-station) and optionally
switched to md3p-int4 for Apple Silicon. Set MEDIASEARCH_MOONDREAM_STATION_ENDPOINT
to override the default endpoint (default: http://localhost:2020/v1).
"""

import os
from pathlib import Path

from src.ai.schema import ModelCard, VisualAnalysis
from src.ai.vision_base import BaseVisionAnalyzer

DEFAULT_ENDPOINT = "http://localhost:2020/v1"
ENDPOINT_ENV = "MEDIASEARCH_MOONDREAM_STATION_ENDPOINT"


def _parse_tags(tags_str: str) -> list[str]:
    """Parse comma-separated tags with order-preserving deduplication."""
    return list(dict.fromkeys(t.strip() for t in tags_str.split(",") if t.strip()))


class MoondreamStationAnalyzer(BaseVisionAnalyzer):
    """Vision analyzer that calls a local Moondream Station server.

    Use md3p-int4 on Apple Silicon by running moondream-station and switching
    to that model. No MLX or model code in this codebase; all inference runs
    in the separate Moondream Station process.
    """

    def __init__(self) -> None:
        import moondream as md
        from PIL import Image

        self._md = md
        self._Image = Image
        endpoint = os.environ.get(ENDPOINT_ENV, DEFAULT_ENDPOINT).strip() or DEFAULT_ENDPOINT
        self._model = md.vl(endpoint=endpoint)

    def get_model_card(self) -> ModelCard:
        return ModelCard(name="moondream-station", version="local")

    def analyze_image(self, image_path: Path) -> VisualAnalysis:
        Image = self._Image
        image = Image.open(image_path)
        if image.mode != "RGB":
            image = image.convert("RGB")

        caption_out = self._model.caption(image, length="short")
        desc = caption_out["caption"] if isinstance(caption_out["caption"], str) else "".join(caption_out["caption"])

        tags_out = self._model.query(
            image,
            "Provide a comma-separated list of single-word tags for this image.",
            reasoning=False,
        )
        tags_str = tags_out["answer"] if isinstance(tags_out["answer"], str) else "".join(tags_out["answer"])

        ocr_out = self._model.query(
            image,
            "Extract all readable text. If there is no text, reply 'None'.",
            reasoning=False,
        )
        ocr_raw = ocr_out["answer"] if isinstance(ocr_out["answer"], str) else "".join(ocr_out["answer"])

        tags_list = _parse_tags(tags_str)
        ocr = ocr_raw.strip() if ocr_raw else None
        if ocr is not None and ocr.lower() == "none":
            ocr = None

        return VisualAnalysis(
            description=desc,
            tags=tags_list,
            ocr_text=ocr,
        )
