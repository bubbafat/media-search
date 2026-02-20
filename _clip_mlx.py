# Copyright © 2023-2024 Apple Inc.
# CLIP model for MLX (from mlx-examples). Loads weights.npz for mlx-community/clip-vit-base-patch32.

from __future__ import annotations

import glob
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import mlx.core as mx
import mlx.nn as nn
from mlx.core import linalg as LA


@dataclass
class CLIPVisionOutput:
    pooler_output: mx.array
    last_hidden_state: mx.array
    hidden_states: Optional[mx.array]


@dataclass
class CLIPTextOutput:
    pooler_output: mx.array
    last_hidden_state: mx.array


@dataclass
class CLIPModelOutput:
    loss: Optional[mx.array]
    text_embeds: Optional[mx.array]
    image_embeds: Optional[mx.array]
    text_model_output: Optional[CLIPTextOutput]
    vision_model_output: Optional[CLIPVisionOutput]


@dataclass
class CLIPTextConfig:
    num_hidden_layers: int
    hidden_size: int
    intermediate_size: int
    num_attention_heads: int
    max_position_embeddings: int
    vocab_size: int
    layer_norm_eps: float


@dataclass
class CLIPVisionConfig:
    num_hidden_layers: int
    hidden_size: int
    intermediate_size: int
    num_attention_heads: int
    num_channels: int
    image_size: int
    patch_size: int
    layer_norm_eps: float


@dataclass
class CLIPConfig:
    text_config: CLIPTextConfig
    vision_config: CLIPVisionConfig
    projection_dim: int


def _quick_gelu(x: mx.array) -> mx.array:
    return x * mx.sigmoid(1.702 * x)


class Attention(nn.Module):
    def __init__(
        self,
        dims: int,
        num_heads: int,
        query_input_dims: Optional[int] = None,
        key_input_dims: Optional[int] = None,
        value_input_dims: Optional[int] = None,
        value_dims: Optional[int] = None,
        value_output_dims: Optional[int] = None,
        bias: bool = False,
    ):
        super().__init__()
        if (dims % num_heads) != 0:
            raise ValueError(f"dims % num_heads != 0 ({dims} % {num_heads})")
        query_input_dims = query_input_dims or dims
        key_input_dims = key_input_dims or dims
        value_input_dims = value_input_dims or key_input_dims
        value_dims = value_dims or dims
        value_output_dims = value_output_dims or dims
        self.num_heads = num_heads
        self.q_proj = nn.Linear(query_input_dims, dims, bias=bias)
        self.k_proj = nn.Linear(key_input_dims, dims, bias=bias)
        self.v_proj = nn.Linear(value_input_dims, value_dims, bias=bias)
        self.out_proj = nn.Linear(value_dims, value_output_dims, bias=bias)

    def __call__(self, queries: mx.array, keys: mx.array, values: mx.array, mask: Optional[mx.array] = None) -> mx.array:
        queries = self.q_proj(queries)
        keys = self.k_proj(keys)
        values = self.v_proj(values)
        B, L, D = queries.shape
        _, S, _ = keys.shape
        queries = queries.reshape(B, L, self.num_heads, -1).transpose(0, 2, 1, 3)
        keys = keys.reshape(B, S, self.num_heads, -1).transpose(0, 2, 3, 1)
        values = values.reshape(B, S, self.num_heads, -1).transpose(0, 2, 1, 3)
        scale = math.sqrt(1 / queries.shape[-1])
        scores = (queries * scale) @ keys
        if mask is not None:
            scores = scores + mask.astype(scores.dtype)
        scores = mx.softmax(scores, axis=-1)
        values_hat = (scores @ values).transpose(0, 2, 1, 3).reshape(B, L, -1)
        return self.out_proj(values_hat)


class MLP(nn.Module):
    def __init__(self, config: Any):
        super().__init__()
        self.fc1 = nn.Linear(config.hidden_size, config.intermediate_size)
        self.fc2 = nn.Linear(config.intermediate_size, config.hidden_size)

    def __call__(self, x: mx.array) -> mx.array:
        x = _quick_gelu(self.fc1(x))
        return self.fc2(x)


class EncoderLayer(nn.Module):
    def __init__(self, config: Any):
        super().__init__()
        self.embed_dim = config.hidden_size
        self.self_attn = Attention(config.hidden_size, config.num_attention_heads, bias=True)
        self.layer_norm1 = nn.LayerNorm(self.embed_dim, eps=config.layer_norm_eps)
        self.mlp = MLP(config)
        self.layer_norm2 = nn.LayerNorm(self.embed_dim, eps=config.layer_norm_eps)

    def __call__(self, x: mx.array, mask: Optional[mx.array] = None) -> mx.array:
        y = self.layer_norm1(x)
        y = self.self_attn(y, y, y, mask)
        x = x + y
        y = self.layer_norm2(x)
        y = self.mlp(y)
        return x + y


class Encoder(nn.Module):
    def __init__(self, config: Any):
        super().__init__()
        self.layers = [EncoderLayer(config) for _ in range(config.num_hidden_layers)]


class TextEmbeddings(nn.Module):
    def __init__(self, config: CLIPTextConfig):
        super().__init__()
        embed_dim = config.hidden_size
        self.token_embedding = nn.Embedding(config.vocab_size, embed_dim)
        self.position_embedding = nn.Embedding(config.max_position_embeddings, embed_dim)

    def __call__(self, x: mx.array) -> mx.array:
        embeddings = self.token_embedding(x)
        embeddings += self.position_embedding.weight[: x.shape[1]]
        return embeddings


class ClipTextModel(nn.Module):
    def __init__(self, config: CLIPTextConfig):
        super().__init__()
        self.embeddings = TextEmbeddings(config)
        self.encoder = Encoder(config)
        self.final_layer_norm = nn.LayerNorm(config.hidden_size)

    def __call__(self, x: mx.array) -> CLIPTextOutput:
        B, N = x.shape
        eot_tokens = mx.argmax(x, axis=-1)
        x = self.embeddings(x)
        mask = nn.MultiHeadAttention.create_additive_causal_mask(N, x.dtype)
        for layer in self.encoder.layers:
            x = layer(x, mask)
        last_hidden_state = self.final_layer_norm(x)
        pooler_output = last_hidden_state[mx.arange(B), eot_tokens]
        return CLIPTextOutput(pooler_output=pooler_output, last_hidden_state=last_hidden_state)


class VisionEmbeddings(nn.Module):
    def __init__(self, config: CLIPVisionConfig):
        super().__init__()
        self.config = config
        self.embed_dim = config.hidden_size
        self.class_embedding = mx.zeros((config.hidden_size,))
        self.patch_embedding = nn.Conv2d(
            config.num_channels,
            self.embed_dim,
            kernel_size=config.patch_size,
            stride=config.patch_size,
            bias=False,
        )
        self.num_patches = (config.image_size // config.patch_size) ** 2
        self.num_positions = self.num_patches + 1
        self.position_embedding = nn.Embedding(self.num_positions, self.embed_dim)

    def __call__(self, x: mx.array) -> mx.array:
        batch_size = x.shape[0]
        patch_embeddings = self.patch_embedding(x)
        patch_embeddings = mx.flatten(patch_embeddings, start_axis=1, end_axis=2)
        embed_dim = patch_embeddings.shape[-1]
        cls_embeddings = mx.broadcast_to(self.class_embedding, (batch_size, 1, embed_dim))
        embeddings = mx.concatenate((cls_embeddings, patch_embeddings), axis=1)
        embeddings += self.position_embedding.weight
        return embeddings


class ClipVisionModel(nn.Module):
    def __init__(self, config: CLIPVisionConfig):
        super().__init__()
        self.embeddings = VisionEmbeddings(config)
        self.pre_layrnorm = nn.LayerNorm(config.hidden_size)
        self.encoder = Encoder(config)
        self.post_layernorm = nn.LayerNorm(config.hidden_size)

    def __call__(
        self,
        x: mx.array,
        output_hidden_states: Optional[bool] = None,
    ) -> CLIPVisionOutput:
        x = self.embeddings(x)
        x = self.pre_layrnorm(x)
        encoder_states = (x,) if output_hidden_states else None
        for layer in self.encoder.layers:
            x = layer(x, mask=None)
            if output_hidden_states and encoder_states is not None:
                encoder_states = encoder_states + (x,)
        pooler_output = self.post_layernorm(x[:, 0, :])
        return CLIPVisionOutput(
            pooler_output=pooler_output,
            last_hidden_state=x,
            hidden_states=None,
        )


class CLIPModel(nn.Module):
    def __init__(self, config: CLIPConfig):
        super().__init__()
        self.text_model = ClipTextModel(config.text_config)
        self.vision_model = ClipVisionModel(config.vision_config)
        text_embed_dim = config.text_config.hidden_size
        vision_embed_dim = config.vision_config.hidden_size
        projection_dim = config.projection_dim
        self.visual_projection = nn.Linear(vision_embed_dim, projection_dim, bias=False)
        self.text_projection = nn.Linear(text_embed_dim, projection_dim, bias=False)
        self.logit_scale = mx.array(0.0)

    def get_text_features(self, input_ids: mx.array) -> mx.array:
        return self.text_projection(self.text_model(input_ids).pooler_output)

    def get_image_features(self, pixel_values: mx.array) -> mx.array:
        return self.visual_projection(self.vision_model(pixel_values).pooler_output)

    def __call__(
        self,
        input_ids: Optional[mx.array] = None,
        pixel_values: Optional[mx.array] = None,
        return_loss: bool = False,
    ) -> CLIPModelOutput:
        if input_ids is not None:
            text_model_output = self.text_model(input_ids)
            text_embeds = self.text_projection(text_model_output.pooler_output)
            text_embeds = text_embeds / LA.norm(text_embeds, axis=-1, keepdims=True)
        else:
            text_embeds = None
            text_model_output = None
        if pixel_values is not None:
            vision_model_output = self.vision_model(pixel_values)
            image_embeds = self.visual_projection(vision_model_output.pooler_output)
            image_embeds = image_embeds / LA.norm(image_embeds, axis=-1, keepdims=True)
        else:
            image_embeds = None
            vision_model_output = None
        return CLIPModelOutput(
            loss=None,
            text_embeds=text_embeds,
            image_embeds=image_embeds,
            vision_model_output=vision_model_output,
            text_model_output=text_model_output,
        )

    @staticmethod
    def _map_weights_hf_to_mlx(weights: dict[str, mx.array]) -> dict[str, mx.array]:
        """Map HuggingFace/mlx-community weight names to mlx-examples format."""
        out: dict[str, mx.array] = {}
        for k, v in weights.items():
            new_k = k
            new_k = new_k.replace("text_model.layers.", "text_model.encoder.layers.")
            new_k = new_k.replace("vision_model.layers.", "vision_model.encoder.layers.")
            new_k = new_k.replace(".attention.key_proj", ".self_attn.k_proj")
            new_k = new_k.replace(".attention.query_proj", ".self_attn.q_proj")
            new_k = new_k.replace(".attention.value_proj", ".self_attn.v_proj")
            new_k = new_k.replace(".attention.out_proj", ".self_attn.out_proj")
            new_k = new_k.replace(".linear1.", ".mlp.fc1.")
            new_k = new_k.replace(".linear2.", ".mlp.fc2.")
            # Layer norms in encoder: ln1/ln2 -> layer_norm1/layer_norm2
            new_k = new_k.replace(".ln1.", ".layer_norm1.")
            new_k = new_k.replace(".ln2.", ".layer_norm2.")
            # Text embeddings live under text_model.embeddings
            if new_k.startswith("text_model.token_embedding."):
                new_k = "text_model.embeddings." + new_k[len("text_model.") :]
            if new_k == "text_model.position_embedding":
                new_k = "text_model.embeddings.position_embedding.weight"
            # Vision embeddings and pre_layrnorm (typo: layrnorm in mlx-examples)
            if new_k == "vision_model.class_embedding":
                new_k = "vision_model.embeddings.class_embedding"
            if new_k.startswith("vision_model.patch_embedding."):
                new_k = new_k.replace("vision_model.patch_embedding.", "vision_model.embeddings.patch_embedding.", 1)
            if new_k == "vision_model.position_embedding":
                new_k = "vision_model.embeddings.position_embedding.weight"
            if new_k.startswith("vision_model.pre_layernorm."):
                new_k = new_k.replace("vision_model.pre_layernorm.", "vision_model.pre_layrnorm.", 1)
            out[new_k] = v
        return out

    @staticmethod
    def sanitize(weights: dict[str, mx.array]) -> dict[str, mx.array]:
        out: dict[str, mx.array] = {}
        for k, v in weights.items():
            if "position_ids" in k:
                continue
            if "patch_embedding.weight" in k:
                # Model expects (out, kH, kW, in). File may be (O, kH, C, kW) or (O, C, kH, kW).
                # If dims 2 and 3 differ (3 vs 32), file has (O, kH, C, kW) -> swap axes 2,3.
                if v.ndim == 4 and v.shape[2] != v.shape[3]:
                    out[k] = v.transpose((0, 1, 3, 2))
                elif v.ndim == 4:
                    out[k] = v.transpose((0, 2, 3, 1))
                else:
                    out[k] = v
            else:
                out[k] = v
        return out

    @classmethod
    def from_pretrained(cls, path: str | Path) -> "CLIPModel":
        path = Path(path)
        with open(path / "config.json", "r") as f:
            config = json.load(f)
        text_c = config["text_config"]
        text_config = CLIPTextConfig(
            num_hidden_layers=text_c["num_hidden_layers"],
            hidden_size=text_c["hidden_size"],
            intermediate_size=text_c["intermediate_size"],
            num_attention_heads=text_c["num_attention_heads"],
            max_position_embeddings=text_c["max_position_embeddings"],
            vocab_size=text_c["vocab_size"],
            layer_norm_eps=text_c["layer_norm_eps"],
        )
        vis_c = config["vision_config"]
        vision_config = CLIPVisionConfig(
            num_hidden_layers=vis_c["num_hidden_layers"],
            hidden_size=vis_c["hidden_size"],
            intermediate_size=vis_c["intermediate_size"],
            num_attention_heads=vis_c["num_attention_heads"],
            num_channels=3,
            image_size=vis_c["image_size"],
            patch_size=vis_c["patch_size"],
            layer_norm_eps=vis_c["layer_norm_eps"],
        )
        clip_config = CLIPConfig(
            text_config=text_config,
            vision_config=vision_config,
            projection_dim=config["projection_dim"],
        )
        model = cls(clip_config)
        weight_files = glob.glob(str(path / "*.safetensors"))
        if not weight_files:
            weight_files = glob.glob(str(path / "weights.npz"))
        if not weight_files:
            raise FileNotFoundError(f"No safetensors or weights.npz in {path}")
        weights: dict[str, mx.array] = {}
        for wf in weight_files:
            weights.update(mx.load(wf))
        weights = model._map_weights_hf_to_mlx(weights)
        weights = model.sanitize(weights)
        # Ensure patch_embedding.weight has layout (O, kH, kW, C) before load (in case key varied)
        for k, v in list(weights.items()):
            if "patch_embedding" in k and v.ndim == 4 and v.shape == (768, 32, 3, 32):
                weights[k] = v.transpose((0, 1, 3, 2))
        model.load_weights(list(weights.items()))
        return model


def load_clip_from_hf(repo_id: str) -> tuple[CLIPModel, Any]:
    """Download repo (with weights.npz) and load CLIP model + transformers processor. Returns (model, processor)."""
    from huggingface_hub import hf_hub_download, snapshot_download

    path = Path(repo_id)
    if not path.exists() or not path.is_dir():
        path = Path(
            snapshot_download(
                repo_id=repo_id,
                allow_patterns=["*.json", "*.npz", "*.txt", "*.model"],
            )
        )
    if not (path / "weights.npz").exists():
        hf_hub_download(repo_id, "weights.npz", local_dir=str(path))

    model = CLIPModel.from_pretrained(path)
    from transformers import AutoProcessor
    processor = AutoProcessor.from_pretrained(str(path))
    return model, processor
