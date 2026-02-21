"""
Moondream2 tagging for image keywords.
Uses transformers with MPS (Metal) on Apple Silicon.
"""

from __future__ import annotations

from PIL import Image

_model: object | None = None
_tokenizer: object | None = None

MODEL_ID = "vikhyatk/moondream2"
REVISION = "2025-01-09"


def _get_moondream() -> tuple[object, object]:
    """Load Moondream2 model and tokenizer (lazy, cached). Uses torch.float16 and MPS."""
    global _model, _tokenizer
    if _model is not None:
        return _model, _tokenizer
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer
    _model = AutoModelForCausalLM.from_pretrained(
        MODEL_ID,
        trust_remote_code=True,
        revision=REVISION,
        torch_dtype=torch.float16,
        device_map={"": "mps"},
    )
    _tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, revision=REVISION)
    return _model, _tokenizer


def get_image_tags(image: Image.Image) -> list[str]:
    """
    Generate descriptive keywords for a PIL Image using Moondream2.
    Pass a single-loaded image to avoid duplicate I/O.
    """
    model, tokenizer = _get_moondream()
    prompt = "List 10 descriptive keywords for this image, comma-separated."
    enc_image = model.encode_image(image)
    answer = model.answer_question(enc_image, prompt, tokenizer)
    return [tag.strip().lower() for tag in answer.split(",") if tag.strip()]
