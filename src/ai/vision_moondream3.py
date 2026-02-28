"""Moondream3 vision analyzer using moondream/moondream3-preview."""

from pathlib import Path

from src.ai.schema import ModelCard, VisualAnalysis
from src.ai.vision_base import BaseVisionAnalyzer


def _parse_tags(tags_str: str) -> list[str]:
    """Parse comma-separated tags with order-preserving deduplication."""
    return list(dict.fromkeys(t.strip() for t in tags_str.split(",") if t.strip()))


class Moondream3Analyzer(BaseVisionAnalyzer):
    """Vision analyzer using moondream/moondream3-preview (revision 1dae073c).

    This class keeps Pillow as a boundary type for model inputs: callers should
    prefer passing proxy file paths, but when an in-memory image object is used
    it is expected to be a `PIL.Image`.
    """

    def __init__(self) -> None:
        import torch
        from PIL import Image

        self._Image = Image
        # FlexAttention (used by Moondream3) only supports CUDA, CPU, HPU; not MPS.
        self.device = "cuda" if torch.cuda.is_available() else "cpu"

        # Moondream's vision.py has an MPS workaround that moves tensors to MPS after
        # CPU ops. When our model runs on CPU, this causes mixed CPU/MPS tensors and
        # "Passed CPU tensor to MPS op". Hide MPS before loading so vision uses the
        # standard adaptive_avg_pool2d path.
        if self.device == "cpu" and hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            _orig = torch.backends.mps.is_available
            torch.backends.mps.is_available = lambda: False
        else:
            _orig = None

        from transformers import AutoModelForCausalLM

        try:
            dtype = torch.bfloat16 if self.device == "cuda" else torch.float32
            self.model = AutoModelForCausalLM.from_pretrained(
                "moondream/moondream3-preview",
                revision="1dae073c11761588126e4bf439ff918ba77ac6c1",
                trust_remote_code=True,
                device_map={"": self.device},
                dtype=dtype,
            )
            try:
                self.model.compile()
            except Exception:
                pass  # fallback to eager mode (e.g. MPS or CPU compile may fail)
        finally:
            if _orig is not None:
                torch.backends.mps.is_available = _orig

    def get_model_card(self) -> ModelCard:
        return ModelCard(name="moondream3", version="1dae073c")

    def analyze_image(
        self,
        image_path: Path,
        mode: str = "full",
        max_tokens: int | None = None,
        should_flush_memory: bool = False,
    ) -> VisualAnalysis:
        Image = self._Image
        with Image.open(image_path) as img:
            image = img.convert("RGB") if img.mode != "RGB" else img.copy()

        encoded = self.model.encode_image(image)
        desc = self.model.caption(encoded, length="short")["caption"]
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

        result = VisualAnalysis(
            description=desc,
            tags=tags_list,
            ocr_text=ocr,
        )
        if should_flush_memory:
            import torch

            if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
                torch.mps.empty_cache()
                torch.mps.synchronize()
        return result
