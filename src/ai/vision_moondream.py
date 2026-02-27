"""Moondream2 vision analyzer using the modern caption/query API."""

from pathlib import Path

from src.ai.schema import ModelCard, VisualAnalysis
from src.ai.vision_base import BaseVisionAnalyzer


def _parse_tags(tags_str: str) -> list[str]:
    """Parse comma-separated tags with order-preserving deduplication."""
    return list(dict.fromkeys(t.strip() for t in tags_str.split(",") if t.strip()))


class MoondreamAnalyzer(BaseVisionAnalyzer):
    """Vision analyzer using vikhyatk/moondream2 (revision 2025-01-09).

    This class keeps Pillow as a boundary type for model inputs: callers should
    prefer passing proxy file paths, but when an in-memory image object is used
    it is expected to be a `PIL.Image`.
    """

    def __init__(self) -> None:
        import torch
        from PIL import Image
        from transformers import AutoModelForCausalLM

        self._Image = Image
        self.device = (
            "cuda"
            if torch.cuda.is_available()
            else "mps"
            if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available()
            else "cpu"
        )
        dtype = torch.float16 if self.device == "mps" else torch.bfloat16
        self.model = AutoModelForCausalLM.from_pretrained(
            "vikhyatk/moondream2",
            revision="2025-01-09",
            trust_remote_code=True,
            device_map={"": self.device},
            dtype=dtype,
        )
        try:
            self.model = torch.compile(self.model)
        except Exception:
            pass  # fallback to eager mode (e.g. MPS compile often fails)

    def get_model_card(self) -> ModelCard:
        return ModelCard(name="moondream2", version="2025-01-09")

    def analyze_image(self, image_path: Path) -> VisualAnalysis:
        Image = self._Image
        image = Image.open(image_path)
        if image.mode != "RGB":
            image = image.convert("RGB")

        encoded = self.model.encode_image(image)
        desc = self.model.caption(encoded, length="normal")["caption"]
        tags_str = self.model.query(
            encoded, "Provide a comma-separated list of single-word tags for this image."
        )["answer"]
        ocr_raw = self.model.query(
            encoded, "Extract all readable text. If there is no text, reply 'None'."
        )["answer"]

        tags_list = _parse_tags(tags_str)
        ocr = ocr_raw.strip() if ocr_raw else None
        if ocr is not None and ocr.lower() == "none":
            ocr = None

        return VisualAnalysis(
            description=desc,
            tags=tags_list,
            ocr_text=ocr,
        )
