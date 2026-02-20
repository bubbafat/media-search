"""Tests for _clip_mlx: weight mapping, sanitize, and optional full model load."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


# ---- Unit: weight name mapping (HF -> mlx-examples) ----
def test_map_weights_hf_to_mlx_renames_keys() -> None:
    """HF/mlx-community weight keys are renamed to mlx-examples format."""
    import mlx.core as mx
    from _clip_mlx import CLIPModel

    # Dummy arrays (shape doesn't matter for key mapping)
    def arr(shape: tuple[int, ...] = (1,)) -> mx.array:
        return mx.zeros(shape)

    weights = {
        "text_model.layers.0.attention.key_proj.weight": arr((64, 64)),
        "text_model.layers.0.attention.key_proj.bias": arr((64,)),
        "text_model.layers.0.linear1.weight": arr((128, 64)),
        "text_model.layers.0.ln1.weight": arr((64,)),
        "text_model.token_embedding.weight": arr((49408, 512)),
        "text_model.position_embedding": arr((77, 512)),
        "vision_model.layers.0.attention.out_proj.bias": arr((768,)),
        "vision_model.patch_embedding.weight": arr((768, 32, 3, 32)),
        "vision_model.class_embedding": arr((768,)),
        "vision_model.position_embedding": arr((50, 768)),
        "vision_model.pre_layernorm.weight": arr((768,)),
    }
    out = CLIPModel._map_weights_hf_to_mlx(weights)

    assert "text_model.encoder.layers.0.self_attn.k_proj.weight" in out
    assert "text_model.encoder.layers.0.self_attn.k_proj.bias" in out
    assert "text_model.encoder.layers.0.mlp.fc1.weight" in out
    assert "text_model.encoder.layers.0.layer_norm1.weight" in out
    assert "text_model.embeddings.token_embedding.weight" in out
    assert "text_model.embeddings.position_embedding.weight" in out
    assert "vision_model.encoder.layers.0.self_attn.out_proj.bias" in out
    assert "vision_model.embeddings.patch_embedding.weight" in out
    assert "vision_model.embeddings.class_embedding" in out
    assert "vision_model.embeddings.position_embedding.weight" in out
    assert "vision_model.pre_layrnorm.weight" in out


# ---- Unit: patch_embedding transpose in sanitize ----
def test_sanitize_patch_embedding_transpose() -> None:
    """sanitize() transposes patch_embedding.weight to (out, kH, kW, in)."""
    import mlx.core as mx
    from _clip_mlx import CLIPModel

    # (O, kH, C, kW) layout from mlx-community -> should become (O, kH, kW, C)
    w_ohc_kw = mx.zeros((768, 32, 3, 32))
    weights = {"vision_model.embeddings.patch_embedding.weight": w_ohc_kw}
    out = CLIPModel.sanitize(weights)
    assert out["vision_model.embeddings.patch_embedding.weight"].shape == (768, 32, 32, 3)

    # (O, C, kH, kW) layout -> should become (O, kH, kW, C)
    w_oc_hw = mx.zeros((768, 3, 32, 32))
    weights2 = {"vision_model.embeddings.patch_embedding.weight": w_oc_hw}
    out2 = CLIPModel.sanitize(weights2)
    assert out2["vision_model.embeddings.patch_embedding.weight"].shape == (768, 32, 32, 3)


def test_sanitize_drops_position_ids() -> None:
    """sanitize() drops position_ids keys."""
    import mlx.core as mx
    from _clip_mlx import CLIPModel

    weights = {"text_model.embeddings.position_ids": mx.zeros((1, 77))}
    out = CLIPModel.sanitize(weights)
    assert len(out) == 0


# ---- Integration: full load + embed (opt-in, needs network) ----
@pytest.mark.skipif(
    os.environ.get("MEDIASEARCH_TEST_CLIP_LOAD") != "1",
    reason="Set MEDIASEARCH_TEST_CLIP_LOAD=1 to run (downloads model, requires network)",
)
def test_load_clip_from_hf_and_embed(tmp_path: Path) -> None:
    """Load CLIP from HF and run one text and one image embedding (catches mapping/shape bugs)."""
    import mlx.core as mx
    from PIL import Image

    from _clip_mlx import load_clip_from_hf

    model, processor = load_clip_from_hf("mlx-community/clip-vit-base-patch32")
    # Use numpy so we don't require PyTorch
    inputs = processor(text=["a photo of a cat"], return_tensors="np", padding=True, truncation=True)
    input_ids = mx.array(inputs["input_ids"])
    text_feats = model.get_text_features(input_ids=input_ids)
    assert text_feats.shape == (1, 512)
    # Image embedding (tiny RGB image)
    img = Image.new("RGB", (224, 224), color="red")
    inputs_img = processor(images=img, return_tensors="np", padding=True)
    pv = inputs_img["pixel_values"]
    if len(pv.shape) == 4 and pv.shape[1] == 3:
        pv = pv.transpose(0, 2, 3, 1)
    pixel_values = mx.array(pv).astype(mx.float32)
    img_feats = model.get_image_features(pixel_values=pixel_values)
    assert img_feats.shape == (1, 512)
