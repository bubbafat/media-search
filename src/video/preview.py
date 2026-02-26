"""Build animated WebP preview from video scene representative frames."""

import logging
from pathlib import Path

from PIL import Image

from src.repository.video_scene_repo import VideoSceneRepository

_log = logging.getLogger(__name__)

PREVIEW_FILENAME = "preview.webp"
PREVIEW_SIZE = (320, 320)
PREVIEW_DURATION_MS = 400
MAX_FRAMES = 60


def build_preview_webp(
    asset_id: int,
    library_slug: str,
    scene_repo: VideoSceneRepository,
    data_dir: Path,
) -> Path | None:
    """
    Build an animated WebP from scene representative frames; save to the asset's scene folder.

    Returns the path of the written file, or None if no file was written (no scenes or no loadable frames).
    """
    scenes = scene_repo.list_scenes(asset_id)
    if not scenes:
        return None

    frames: list[Image.Image] = []
    for s in scenes:
        path = Path(s.rep_frame_path)
        if not path.exists():
            _log.warning("Scene frame missing, skipping: %s", path)
            continue
        try:
            img = Image.open(path)
            img.load()
            img = img.convert("RGB")
            thumb = img.copy()
            thumb.thumbnail(PREVIEW_SIZE, Image.Resampling.LANCZOS)
            # Pad to exact size if thumbnail didn't fill (e.g. portrait)
            if thumb.size != PREVIEW_SIZE:
                padded = Image.new("RGB", PREVIEW_SIZE, (0, 0, 0))
                padded.paste(thumb, (0, 0))
                thumb = padded
            frames.append(thumb)
        except (OSError, ValueError) as e:
            _log.warning("Could not load scene frame %s: %s", path, e)
            continue

    if not frames:
        return None

    # Cap frames for long videos
    if len(frames) > MAX_FRAMES:
        step = len(frames) / MAX_FRAMES
        indices = [int(i * step) for i in range(MAX_FRAMES)]
        frames = [frames[i] for i in indices]

    out_dir = data_dir / "video_scenes" / library_slug / str(asset_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / PREVIEW_FILENAME

    first = frames[0]
    rest = frames[1:]
    first.save(
        out_path,
        "WEBP",
        save_all=len(rest) > 0,
        append_images=rest if rest else None,
        duration=PREVIEW_DURATION_MS,
        loop=65535,  # Loop repeatedly (0 = infinite in spec; some viewers treat 0 as no loop)
    )
    return out_path
