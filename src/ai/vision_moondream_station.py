"""Vision analyzer that uses a local Moondream Station server (e.g. md3p-int4 via MLX).

Requires Moondream Station to be running (e.g. moondream-station) and optionally
switched to md3p-int4 for Apple Silicon. Set MEDIASEARCH_MOONDREAM_STATION_ENDPOINT
to override the default endpoint (default: http://localhost:2020/v1).

Uses a persistent requests.Session with connection pooling to avoid TCP socket
exhaustion when multiple workers hit the same Station.
"""

import base64
import os
from io import BytesIO
from pathlib import Path

import requests

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
        from PIL import Image

        self._Image = Image
        endpoint = os.environ.get(ENDPOINT_ENV, DEFAULT_ENDPOINT).strip() or DEFAULT_ENDPOINT
        self._endpoint = endpoint.rstrip("/")
        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=20)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def get_model_card(self) -> ModelCard:
        return ModelCard(name="moondream-station", version="local")

    def _encode_image(self, image) -> str:
        """Convert PIL Image to base64 data URL (same format as Moondream Station API)."""
        if image.mode != "RGB":
            image = image.convert("RGB")
        buffered = BytesIO()
        image.save(buffered, format="JPEG", quality=95)
        b64 = base64.b64encode(buffered.getvalue()).decode()
        return f"data:image/jpeg;base64,{b64}"

    def _post(self, path: str, json_payload: dict) -> dict:
        """POST JSON to Station endpoint and return parsed response."""
        url = f"{self._endpoint}/{path.lstrip('/')}"
        resp = self._session.post(
            url,
            json=json_payload,
            headers={"Content-Type": "application/json"},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()

    def _caption(self, image, length: str = "short") -> dict:
        """Call Station caption endpoint. Raises KeyError('caption') if response format is non-standard."""
        payload = {
            "image_url": self._encode_image(image),
            "length": length,
            "stream": False,
        }
        data = self._post("caption", payload)
        if "caption" not in data:
            raise KeyError("caption")
        return data

    def _query(self, image, question: str, *, reasoning: bool = False) -> dict:
        """Call Station query endpoint."""
        payload = {
            "image_url": self._encode_image(image),
            "question": question,
            "reasoning": reasoning,
            "stream": False,
        }
        return self._post("query", payload)

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

        try:
            caption_out = self._caption(image, length="short")
            cap = caption_out.get("caption") if isinstance(caption_out, dict) else None
            desc = cap if isinstance(cap, str) else ("".join(cap) if cap else "")
        except KeyError as e:
            if e.args == ("caption",):
                # Moondream Station (e.g. md3p-int4) may return non-standard caption format.
                # Fall back to query endpoint which returns {"answer": ...}.
                caption_out = self._query(
                    image,
                    "Describe this image briefly in one or two sentences.",
                    reasoning=False,
                )
                ans = caption_out.get("answer") if isinstance(caption_out, dict) else None
                desc = ans if isinstance(ans, str) else ("".join(ans) if ans else "")
            else:
                raise

        tags_out = self._query(
            image,
            "Provide a comma-separated list of single-word tags for this image.",
            reasoning=False,
        )
        ans_tags = tags_out.get("answer") if isinstance(tags_out, dict) else None
        tags_str = ans_tags if isinstance(ans_tags, str) else ("".join(ans_tags) if ans_tags else "")

        ocr_out = self._query(
            image,
            "Extract all readable text. If there is no text, reply 'None'.",
            reasoning=False,
        )
        ans_ocr = ocr_out.get("answer") if isinstance(ocr_out, dict) else None
        ocr_raw = ans_ocr if isinstance(ans_ocr, str) else ("".join(ans_ocr) if ans_ocr else "")

        tags_list = _parse_tags(tags_str)
        ocr = ocr_raw.strip() if ocr_raw else None
        if ocr is not None and ocr.lower() == "none":
            ocr = None

        return VisualAnalysis(
            description=desc,
            tags=tags_list,
            ocr_text=ocr,
        )
