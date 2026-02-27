"""Moondream3 vision analyzer using moondream/moondream3-preview."""

from pathlib import Path

from src.ai.schema import ModelCard, VisualAnalysis
from src.ai.vision_base import BaseVisionAnalyzer


def _parse_tags(tags_str: str) -> list[str]:
    """Parse comma-separated tags with order-preserving deduplication."""
    return list(dict.fromkeys(t.strip() for t in tags_str.split(",") if t.strip()))


class Moondream3Analyzer(BaseVisionAnalyzer):
    """Vision analyzer using moondream/moondream3-preview.

    This class keeps Pillow as a boundary type for model inputs: callers should
    prefer passing proxy file paths, but when an in-memory image object is used
    it is expected to be a `PIL.Image`.
    """

    def __init__(self) -> None:
        import torch
        from PIL import Image
        from transformers import AutoModelForCausalLM

        self._Image = Image
        # FlexAttention (used by Moondream3) only supports CUDA, CPU, HPU; not MPS.
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        dtype = torch.bfloat16 if self.device == "cuda" else torch.float32
        self.model = AutoModelForCausalLM.from_pretrained(
            "moondream/moondream3-preview",
            trust_remote_code=True,
            device_map={"": self.device},
            dtype=dtype,
        )
        try:
            self.model.compile()
        except Exception:
            pass  # fallback to eager mode (e.g. MPS or CPU compile may fail)

    def get_model_card(self) -> ModelCard:
        return ModelCard(name="moondream3", version="preview")

    def analyze_image(self, image_path: Path) -> VisualAnalysis:
        Image = self._Image
        image = Image.open(image_path)
        if image.mode != "RGB":
            image = image.convert("RGB")

        encoded = self.model.encode_image(image)
        desc = self.model.caption(encoded, length="normal")["caption"]
        tags_str = self.model.query(
            image=encoded,
            question="Provide a comma-separated list of single-word tags for this image.",
            reasoning=False,
        )["answer"]
        ocr_raw = self.model.query(
            image=encoded,
            question="Extract all readable text. If there is no text, reply 'None'.",
            reasoning=False,
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
