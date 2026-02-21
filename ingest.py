#!/usr/bin/env python3
"""
Ingestion script: scan directory, tag images via Moondream2, store in media.db.
Uses database.DatabaseManager for storage; tagging for model inference.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from database import DatabaseManager
from tagging import get_image_tags


def get_file_type(path: Path) -> str:
    """Return file type category from extension."""
    ext = path.suffix.lower()
    if ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"):
        return "IMAGE"
    if ext in (".mp4", ".mov", ".avi", ".mkv", ".webm"):
        return "VIDEO"
    return "FILE"


def ingest_directory(db: DatabaseManager, root: Path, extensions: frozenset[str]) -> int:
    """Ingest images from directory. Returns count of processed files."""
    root = root.resolve()
    count = 0
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in extensions:
            continue
        file_path = str(path)
        file_type = get_file_type(path)
        asset_id = db.add_asset(file_path, file_type)
        try:
            tags = get_image_tags(file_path)
            if tags:
                db.link_tags(asset_id, tags)
        except Exception as e:
            print(f"  Warning: tag failed for {path.name}: {e}")
        count += 1
        if count % 10 == 0:
            print(f"  Processed {count} files...")
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest images and tag them into media.db")
    parser.add_argument("path", type=Path, help="Directory to scan")
    parser.add_argument("--db", type=Path, default=Path("media.db"), help="Database path")
    parser.add_argument(
        "--extensions",
        default=".jpg,.jpeg,.png,.gif,.webp",
        help="Comma-separated extensions (default: .jpg,.jpeg,.png,.gif,.webp)",
    )
    args = parser.parse_args()

    if not args.path.is_dir():
        print(f"Error: {args.path} is not a directory")
        return 1

    exts = frozenset(e.strip().lower() for e in args.extensions.split(",") if e.strip())

    with DatabaseManager(args.db) as db:
        db.create_tables()
        print(f"Ingesting from {args.path}...")
        count = ingest_directory(db, args.path, exts)
        print(f"Done. Processed {count} files.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
