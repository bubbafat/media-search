#!/usr/bin/env python3
"""
MediaSearch Studio — Gradio Web UI for semantic and visual search.
Uses the same MediaDatabase and ImageEmbedder as the CLI (shared model, no double-load).
Optimized for M4 Mac Studio (GPU batching).

Run: uv run python app.py
Then open http://localhost:7860
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import gradio as gr

from mediasearch import (
    DEFAULT_DB_PATH,
    FileCrawler,
    ImageEmbedder,
    MediaDatabase,
    VideoThumbnailer,
    run_rebuild_with_progress,
)

# Shared resources (same process as CLI — model loaded once)
_db: MediaDatabase | None = None
_embedder: ImageEmbedder | None = None
_thumbnailer: VideoThumbnailer | None = None

# Result metadata for click-to-reveal: list of {"path", "display_path", "type", "capture_date", "lat", "lon"}
ResultMeta = dict[str, str | float | None]


def _db_instance() -> MediaDatabase:
    global _db
    if _db is None:
        _db = MediaDatabase(DEFAULT_DB_PATH)
        _db.init_schema()
    return _db


def _embedder_instance() -> ImageEmbedder:
    global _embedder
    if _embedder is None:
        _embedder = ImageEmbedder()
    return _embedder


def _thumbnailer_instance() -> VideoThumbnailer:
    global _thumbnailer
    if _thumbnailer is None:
        _thumbnailer = VideoThumbnailer()
    return _thumbnailer


def _asset_count() -> int:
    """Thread-safe: use a fresh connection so Gradio worker threads don't share the main-thread DB."""
    try:
        conn = sqlite3.connect(str(DEFAULT_DB_PATH))
        try:
            row = conn.execute("SELECT COUNT(*) FROM assets").fetchone()
            return row[0] if row else 0
        finally:
            conn.close()
    except sqlite3.OperationalError:
        return 0


def _search_results_to_gallery(
    db: MediaDatabase,
    thumbnailer: VideoThumbnailer,
    results: list[tuple[int, float]],
    k: int = 20,
) -> tuple[list[tuple[str | Path, str]], list[ResultMeta]]:
    """Convert search (asset_id, distance) list to gallery items and metadata for click-to-reveal."""
    conn = db.connect()
    gallery: list[tuple[str | Path, str]] = []
    meta_list: list[ResultMeta] = []
    for asset_id, distance in results[:k]:
        row = conn.execute(
            "SELECT path, hash, type, capture_date, lat, lon FROM assets WHERE id = ?",
            (asset_id,),
        ).fetchone()
        if not row:
            continue
        path_str = row["path"]
        path = Path(path_str)
        asset_type = row["type"]
        if asset_type == "VIDEO":
            thumb = thumbnailer.thumbnail_path(row["hash"])
            display_path = str(thumb) if thumb.exists() else path_str
        else:
            display_path = path_str
        if not Path(display_path).exists():
            continue
        gallery.append((display_path, f"{path.name} ({distance:.3f})"))
        meta_list.append({
            "path": path_str,
            "display_path": display_path,
            "type": asset_type,
            "capture_date": row["capture_date"],
            "lat": row["lat"],
            "lon": row["lon"],
        })
    return gallery, meta_list


def semantic_search(query: str) -> tuple[list[tuple[str | Path, str]], list[ResultMeta], str]:
    """Tab 1: natural language query → top 20 results."""
    if not query or not query.strip():
        return [], [], "Enter a search query."
    db = _db_instance()
    embedder = _embedder_instance()
    thumbnailer = _thumbnailer_instance()
    try:
        vec = embedder.get_text_embedding(query.strip())
    except Exception as e:
        return [], [], f"Embedding failed: {e}"
    results = db.search(vec, k=20)
    if not results:
        return [], [], "No results (index may be empty). Try indexing a directory first."
    gallery, meta_list = _search_results_to_gallery(db, thumbnailer, results, k=20)
    return gallery, meta_list, f"Found {len(gallery)} results."


def visual_similarity(image: str | None) -> tuple[list[tuple[str | Path, str]], list[ResultMeta], str]:
    """Tab 2: upload image → visually similar matches."""
    if not image or not image.strip():
        return [], [], "Upload an image to find similar media."
    path = Path(image.strip())
    if not path.is_file():
        return [], [], "Could not read uploaded image."
    db = _db_instance()
    embedder = _embedder_instance()
    thumbnailer = _thumbnailer_instance()
    try:
        vec = embedder.get_image_embedding(path)
    except Exception as e:
        return [], [], f"Embedding failed: {e}"
    results = db.search(vec, k=20)
    if not results:
        return [], [], "No results in index."
    gallery, meta_list = _search_results_to_gallery(db, thumbnailer, results, k=20)
    return gallery, meta_list, f"Found {len(gallery)} similar results."


def scan_and_index(path_text: str) -> Iterator[tuple[str, str]]:
    """Tab 3: scan directory and index with progress."""
    path = Path(path_text).expanduser().resolve()
    if not path.is_dir():
        yield "Invalid directory path.", get_status_text()
        return
    db = _db_instance()
    log_lines: list[str] = []
    for msg in run_rebuild_with_progress(db, path):
        log_lines.append(msg)
        yield "\n".join(log_lines), get_status_text(_asset_count())
    yield "\n".join(log_lines), get_status_text(_asset_count())


def clear_database() -> tuple[str, str]:
    """Tab 3: clear all assets and embeddings."""
    db = _db_instance()
    db.rebuild_schema()
    return "Database cleared.", get_status_text(0)


def get_status_text(count: int | None = None) -> str:
    if count is None:
        count = _asset_count()
    return f"**{count}** assets indexed"


def on_gallery_select(evt: gr.SelectData, meta_list: list[ResultMeta]) -> str:
    """Click to reveal: show path and metadata in accordion."""
    if not meta_list or evt.index is None:
        return "_No selection._"
    idx = evt.index if isinstance(evt.index, int) else evt.index[0]
    if idx < 0 or idx >= len(meta_list):
        return "_No selection._"
    m = meta_list[idx]
    path = m.get("path") or ""
    capture_date = m.get("capture_date") or "—"
    lat, lon = m.get("lat"), m.get("lon")
    if lat is not None and lon is not None:
        gps = f"{lat:.6f}, {lon:.6f}"
    else:
        gps = "—"
    return f"""**Path**  
`{path}`  

**Date**  
{capture_date}  

**GPS**  
{gps}"""


def build_ui() -> gr.Blocks:
    with gr.Blocks(title="MediaSearch Studio") as demo:
        # State for click-to-reveal (per-tab so each gallery has correct metadata)
        result_meta_semantic = gr.State([])  # type: ignore[var-annotated]
        result_meta_visual = gr.State([])  # type: ignore[var-annotated]

        # Header
        gr.Markdown("# MediaSearch Studio")
        status = gr.Markdown(get_status_text(), elem_classes=["status"])
        gr.Markdown("Semantic and visual search over your local media (JPG, ARW, MP4, MOV).")

        with gr.Tabs():
            # Tab 1: Semantic Search
            with gr.TabItem("Semantic Search"):
                search_in = gr.Textbox(
                    label="Search",
                    placeholder='e.g. "blue car in the snow"',
                    scale=9,
                )
                search_btn = gr.Button("Search", variant="primary", scale=1)
                search_status = gr.Markdown("")
                gallery = gr.Gallery(
                    label="Results",
                    columns=4,
                    rows=5,
                    object_fit="contain",
                    height="auto",
                    show_label=True,
                )
                with gr.Accordion("Click to reveal — file path & metadata", open=False):
                    reveal = gr.Markdown("_Click a result above to show path and metadata._")
                search_btn.click(
                    fn=semantic_search,
                    inputs=[search_in],
                    outputs=[gallery, result_meta_semantic, search_status],
                ).then(
                    fn=lambda: "_Click a result above to show path and metadata._",
                    inputs=None,
                    outputs=[reveal],
                )
                gallery.select(
                    fn=on_gallery_select,
                    inputs=[result_meta_semantic],
                    outputs=[reveal],
                )

            # Tab 2: Visual Similarity
            with gr.TabItem("Visual Similarity"):
                image_in = gr.Image(label="Reference image", type="filepath")
                vs_btn = gr.Button("Find similar", variant="primary")
                vs_status = gr.Markdown("")
                gallery_vs = gr.Gallery(
                    label="Similar results",
                    columns=4,
                    rows=5,
                    object_fit="contain",
                    height="auto",
                    show_label=True,
                )
                with gr.Accordion("Click to reveal — file path & metadata", open=False):
                    reveal_vs = gr.Markdown("_Click a result above to show path and metadata._")
                vs_btn.click(
                    fn=visual_similarity,
                    inputs=[image_in],
                    outputs=[gallery_vs, result_meta_visual, vs_status],
                ).then(
                    fn=lambda: "_Click a result above to show path and metadata._",
                    inputs=None,
                    outputs=[reveal_vs],
                )
                gallery_vs.select(
                    fn=on_gallery_select,
                    inputs=[result_meta_visual],
                    outputs=[reveal_vs],
                )

            # Tab 3: Library Management
            with gr.TabItem("Library Management"):
                path_in = gr.Textbox(
                    label="Directory path",
                    placeholder="/path/to/photos",
                    value="",
                )
                scan_btn = gr.Button("Scan & Index", variant="primary")
                progress_log = gr.Textbox(
                    label="Progress",
                    lines=10,
                    interactive=False,
                    placeholder="Click Scan & Index to start…",
                )
                clear_btn = gr.Button("Clear Database", variant="secondary")
                scan_btn.click(
                    fn=scan_and_index,
                    inputs=[path_in],
                    outputs=[progress_log, status],
                )
                clear_btn.click(
                    fn=clear_database,
                    inputs=None,
                    outputs=[progress_log, status],
                )

        # Refresh status when app loads
        demo.load(fn=lambda: get_status_text(), inputs=None, outputs=[status])

    return demo


def main() -> None:
    demo = build_ui()
    demo.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
        theme=gr.themes.Soft(primary_hue="slate"),
        css=".status { font-size: 0.95rem; color: var(--body-text-color-subdued); }",
    )


if __name__ == "__main__":
    main()
