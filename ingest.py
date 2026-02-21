#!/usr/bin/env python3
"""
Ingestion script: scan directory, tag images via Moondream2, embed via SigLIP, store in database.
Uses MediaDatabase (unified database layer) for storage; tagging and mediasearch for model inference.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from media_database import MediaDatabase
from mediasearch import ImageEmbedder
from tagging import get_image_tags

logger = logging.getLogger(__name__)


def get_file_type(path: Path) -> str:
    """Return file type category from extension."""
    ext = path.suffix.lower()
    if ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"):
        return "IMAGE"
    if ext in (".mp4", ".mov", ".avi", ".mkv", ".webm"):
        return "VIDEO"
    return "FILE"


def ingest_directory(
    db: MediaDatabase,
    embedder: ImageEmbedder,
    root: Path,
    extensions: frozenset[str],
) -> int:
    """
    Ingest images from directory.
    For each image: load once with PIL, generate SigLIP embedding, get Moondream tags,
    then store asset + tags + embedding in a single transaction.
    Returns count of successfully processed files.
    """
    root = root.resolve()
    paths = sorted(p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in extensions)
    total = len(paths)
    if total == 0:
        return 0

    count = 0
    for i, path in enumerate(paths, start=1):
        file_path = str(path)
        file_type = get_file_type(path)

        try:
            from PIL import Image

            image = Image.open(path).convert("RGB")

            # SigLIP 1152-dim embedding
            embedding = embedder.get_image_embedding_from_pil(image)

            # Moondream2 keywords (reuses same image, no extra I/O)
            tags = get_image_tags(image)
            print("Tags: %s", tags)

            # Single transaction: asset + tags + vector
            db.ingest_asset(file_path, file_type, tags, embedding)
            count += 1

        except Exception as e:
            logger.warning("Skipping %s: %s", path.name, e)
            continue

        if i % 10 == 0 or i == total:
            logger.info("Processing image %d/%d...", i, total)

    return count


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(
        description="Ingest images: tag with Moondream2, embed with SigLIP, store in database"
    )
    parser.add_argument("path", type=Path, help="Directory to scan")
    parser.add_argument("--db", type=Path, default=Path("mediasearch.db"), help="Database path")
    parser.add_argument(
        "--extensions",
        default=".jpg,.jpeg,.png,.gif,.webp",
        help="Comma-separated extensions (default: .jpg,.jpeg,.png,.gif,.webp)",
    )
    args = parser.parse_args()

    if not args.path.is_dir():
        logger.error("%s is not a directory", args.path)
        return 1

    exts = frozenset(e.strip().lower() for e in args.extensions.split(",") if e.strip())

    with MediaDatabase(args.db) as db:
        db.init_schema()
        embedder = ImageEmbedder()
        logger.info("Ingesting from %s...", args.path)
        count = ingest_directory(db, embedder, args.path, exts)
        logger.info("Done. Processed %d images.", count)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
