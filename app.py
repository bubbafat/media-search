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
    RawThumbnailer,
    VideoThumbnailer,
    run_rebuild_with_progress,
)

# Shared resources (same process as CLI — model loaded once)
# THREAD-SAFETY: Gradio runs handlers in worker threads. _db_instance() uses a main-thread
# connection. Handlers that use _db_instance() may only call methods that use _fresh_connection()
# (search, fetch_asset_rows_by_ids, get_all_assets, get_vec_index_count). Handlers that need
# connect() (rebuild_schema, batch_upsert, etc.) must create a fresh MediaDatabase() in the
# worker. See tests/test_app_thread_safety.py.
_db: MediaDatabase | None = None
_embedder: ImageEmbedder | None = None
_thumbnailer: VideoThumbnailer | None = None
_raw_thumbnailer: RawThumbnailer | None = None

# Result metadata for click-to-reveal: list of {"path", "display_path", "type", "capture_date", "lat", "lon", "distance"}
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


def _raw_thumbnailer_instance() -> RawThumbnailer:
    global _raw_thumbnailer
    if _raw_thumbnailer is None:
        _raw_thumbnailer = RawThumbnailer()
    return _raw_thumbnailer


def _asset_count() -> int:
    """Thread-safe: use a fresh connection so Gradio worker threads don't share the main-thread DB."""
    try:
        conn = sqlite3.connect(str(DEFAULT_DB_PATH), timeout=30)
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
    raw_thumbnailer: RawThumbnailer,
    results: list[tuple[int, float]],
    k: int = 20,
) -> tuple[list[tuple[str | Path, str]], list[ResultMeta]]:
    """Convert search (asset_id, distance) list to gallery items and metadata for click-to-reveal."""
    pairs = results[:k]
    rows_with_dist = db.fetch_asset_rows_by_ids(pairs)
    gallery: list[tuple[str | Path, str]] = []
    meta_list: list[ResultMeta] = []
    for (row, distance) in rows_with_dist:
        if not row:
            continue
        path_str = row["path"]
        path = Path(path_str)
        asset_type = row["type"]
        if asset_type == "VIDEO":
            thumb = thumbnailer.thumbnail_path(row["hash"])
            display_path = str(thumb) if thumb.exists() else path_str
        elif asset_type == "RAW":
            raw_preview = raw_thumbnailer.ensure_thumbnail(path, row["hash"] or "")
            display_path = str(raw_preview) if raw_preview else path_str
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
            "distance": distance,
        })
    return gallery, meta_list


def _build_score_view(meta_list: list[ResultMeta]) -> str:
    """Build Score View debug text from result metadata. Warn if all distances > 0.9."""
    if not meta_list:
        return "_No results._"
    lines = []
    for i, m in enumerate(meta_list, 1):
        dist = m.get("distance")
        path = m.get("path") or ""
        name = Path(path).name if path else f"#{i}"
        lines.append(f"{i}. `{name}` → **{dist:.4f}**" if dist is not None else f"{i}. `{name}` → —")
    text = "\n".join(lines)
    distances = [m["distance"] for m in meta_list if m.get("distance") is not None]
    if distances and all(d > 0.9 for d in distances):
        text += "\n\n⚠️ **All distances > 0.9** — AI doesn't see a strong match. Possible vector normalization issue."
    return text


def semantic_search(query: str) -> tuple[list[tuple[str | Path, str]], list[ResultMeta], str, str]:
    """Tab 1: natural language query → top 20 results."""
    empty_score = "_Run a search to see raw distance scores._"
    if not query or not query.strip():
        return [], [], "Enter a search query.", empty_score
    db = _db_instance()
    embedder = _embedder_instance()
    thumbnailer = _thumbnailer_instance()
    try:
        vec = embedder.get_text_embedding(query.strip())
    except Exception as e:
        return [], [], f"Embedding failed: {e}", empty_score
    results = db.search(vec, k=20)
    if not results:
        return [], [], "No results (index may be empty). Try indexing a directory first.", empty_score
    raw_thumbnailer = _raw_thumbnailer_instance()
    gallery, meta_list = _search_results_to_gallery(db, thumbnailer, raw_thumbnailer, results, k=20)
    score_view = _build_score_view(meta_list)
    return gallery, meta_list, f"Found {len(gallery)} results.", score_view


def visual_similarity(image: str | None) -> tuple[list[tuple[str | Path, str]], list[ResultMeta], str, str]:
    """Tab 2: upload image → visually similar matches."""
    empty_score = "_Upload an image and click Find similar to see raw distance scores._"
    if not image or not image.strip():
        return [], [], "Upload an image to find similar media.", empty_score
    path = Path(image.strip())
    if not path.is_file():
        return [], [], "Could not read uploaded image.", empty_score
    db = _db_instance()
    embedder = _embedder_instance()
    thumbnailer = _thumbnailer_instance()
    try:
        vec = embedder.get_image_embedding(path)
    except Exception as e:
        return [], [], f"Embedding failed: {e}", empty_score
    results = db.search(vec, k=20)
    if not results:
        return [], [], "No results in index.", empty_score
    raw_thumbnailer = _raw_thumbnailer_instance()
    gallery, meta_list = _search_results_to_gallery(db, thumbnailer, raw_thumbnailer, results, k=20)
    score_view = _build_score_view(meta_list)
    return gallery, meta_list, f"Found {len(gallery)} similar results.", score_view


def scan_and_index(path_text: str) -> Iterator[tuple[str, str]]:
    """Tab 3: scan directory and index with progress."""
    path = Path(path_text).expanduser().resolve()
    if not path.is_dir():
        yield "Invalid directory path.", get_status_text()
        return
    # Use a fresh DB instance so the connection is created in this worker thread (thread-safe).
    db = MediaDatabase(DEFAULT_DB_PATH)
    db.init_schema()
    log_lines: list[str] = []
    for msg in run_rebuild_with_progress(db, path):
        log_lines.append(msg)
        yield "\n".join(log_lines), get_status_text(_asset_count())
    yield "\n".join(log_lines), get_status_text(_asset_count())


def clear_database() -> tuple[str, str]:
    """Tab 3: clear all assets and embeddings."""
    # Use a fresh DB instance so the connection is created in this worker thread (thread-safe).
    db = MediaDatabase(DEFAULT_DB_PATH)
    db.init_schema()
    db.rebuild_schema()
    return "Database cleared.", get_status_text(0)


def get_status_text(count: int | None = None) -> str:
    if count is None:
        count = _asset_count()
    return f"**{count}** assets indexed"


def _catalog_stats_text(
    assets_count: int,
    vec_count: int,
    missing_thumbnails: int,
) -> str:
    """Build Stats display markdown."""
    lines = [
        f"**Assets:** {assets_count}  |  **Vectors:** {vec_count}  |  **Missing thumbnails:** {missing_thumbnails}",
    ]
    if vec_count < assets_count and assets_count > 0:
        lines.append("\n⚠️ **Indexing Incomplete.** Run Scan & Index to embed remaining assets.")
    return "\n".join(lines)


def catalog_stats_load() -> str:
    """Load Stats for Catalog Browser. Thread-safe."""
    db = MediaDatabase(DEFAULT_DB_PATH)
    db.init_schema()
    assets_count = db.get_assets_count()
    vec_count = db.get_vec_index_count()
    thumbnailer = _thumbnailer_instance()
    raw_thumbnailer = _raw_thumbnailer_instance()
    thumb_assets = db.get_assets_for_thumb_check(limit=500)
    missing = 0
    for a in thumb_assets:
        h = a.get("hash") or ""
        t = a.get("type") or ""
        if t == "VIDEO":
            if not thumbnailer.thumbnail_path(h).exists():
                missing += 1
        elif t == "RAW":
            if not raw_thumbnailer.thumbnail_path(h).exists():
                missing += 1
    return _catalog_stats_text(assets_count, vec_count, missing)


def catalog_direct_load() -> tuple[list[list[str | None]], list[dict[str, object]]]:
    """Load Direct View: 100 rows (ID, Path, Type, Capture Date) and assets state. Thread-safe."""
    db = MediaDatabase(DEFAULT_DB_PATH)
    db.init_schema()
    assets = db.get_assets_with_id(limit=100)
    df_data: list[list[str | None]] = []
    for a in assets:
        df_data.append([
            str(a.get("id") or ""),
            a.get("path") or "",
            a.get("type") or "",
            str(a.get("capture_date") or ""),
        ])
    return df_data, assets


def validate_paths() -> str:
    """Check os.path.exists for first 10 asset paths. Reports Absolute Path Drift. Thread-safe."""
    db = MediaDatabase(DEFAULT_DB_PATH)
    db.init_schema()
    paths = db.get_first_paths(limit=10)
    if not paths:
        return "No assets in database."
    results: list[str] = []
    missing = 0
    for p in paths:
        exists = "✅" if Path(p).exists() else "❌"
        if not Path(p).exists():
            missing += 1
        results.append(f"{exists} `{p}`")
    report = "\n".join(results)
    if missing > 0:
        report += f"\n\n⚠️ **Absolute Path Drift:** {missing} of {len(paths)} sampled paths do not exist. Database may have been built on another drive/folder."
    return report


def on_direct_row_select(
    evt: gr.SelectData,
    assets: list[dict[str, object]],
) -> str | None:
    """Show preview for selected Direct View row (image, video thumb, or raw preview)."""
    if not assets or evt.index is None:
        return None
    row_idx = evt.index[0] if isinstance(evt.index, (list, tuple)) else evt.index
    if row_idx < 0 or row_idx >= len(assets):
        return None
    a = assets[row_idx]
    asset_type = a.get("type") or ""
    path_str = a.get("path") or ""
    if asset_type == "VIDEO":
        thumb = _thumbnailer_instance().thumbnail_path(a.get("hash") or "")
        return str(thumb) if thumb.exists() else (path_str if Path(path_str).is_file() else None)
    if asset_type == "RAW":
        raw_preview = _raw_thumbnailer_instance().ensure_thumbnail(Path(path_str), a.get("hash") or "")
        return str(raw_preview) if raw_preview else (path_str if Path(path_str).is_file() else None)
    return path_str if Path(path_str).is_file() else None


def on_catalog_row_select(
    evt: gr.SelectData,
    catalog_assets: list[dict[str, object]],
) -> str | None:
    """Show preview (image, video thumbnail, or raw embedded preview) for selected Dataframe row."""
    if not catalog_assets or evt.index is None:
        return None
    row_idx = evt.index[0] if isinstance(evt.index, (list, tuple)) else evt.index
    if row_idx < 0 or row_idx >= len(catalog_assets):
        return None
    a = catalog_assets[row_idx]
    asset_type = a.get("type") or ""
    path_str = a.get("path") or ""
    if asset_type == "VIDEO":
        thumb = _thumbnailer_instance().thumbnail_path(a.get("hash") or "")
        return str(thumb) if thumb.exists() else (path_str if Path(path_str).is_file() else None)
    if asset_type == "RAW":
        raw_preview = _raw_thumbnailer_instance().ensure_thumbnail(Path(path_str), a.get("hash") or "")
        return str(raw_preview) if raw_preview else (path_str if Path(path_str).is_file() else None)
    return path_str if Path(path_str).is_file() else None


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
                with gr.Accordion("Score View (debug)", open=False):
                    search_score_display = gr.Markdown("_Run a search to see raw distance scores._")
                search_btn.click(
                    fn=semantic_search,
                    inputs=[search_in],
                    outputs=[gallery, result_meta_semantic, search_status, search_score_display],
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
                with gr.Accordion("Score View (debug)", open=False):
                    vs_score_display = gr.Markdown("_Upload an image and click Find similar to see raw distance scores._")
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
                    outputs=[gallery_vs, result_meta_visual, vs_status, vs_score_display],
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

            # Tab 4: Catalog Browser (diagnostic)
            with gr.TabItem("Catalog Browser"):
                catalog_stats = gr.Markdown("")
                catalog_validate_btn = gr.Button("Validate Paths", variant="secondary")
                catalog_validate_report = gr.Markdown("")
                catalog_direct_assets = gr.State([])  # type: ignore[var-annotated]
                with gr.Row():
                    catalog_df = gr.Dataframe(
                        label="Direct View — last 100 assets (click row for preview)",
                        headers=["ID", "Path", "Type", "Capture Date"],
                        datatype=["str", "str", "str", "str"],
                    )
                    catalog_preview = gr.Image(label="Preview", type="filepath")
                catalog_refresh_btn = gr.Button("Refresh Catalog", variant="secondary")

                def catalog_refresh() -> tuple[str, list[list[str | None]], list[dict[str, object]]]:
                    stats = catalog_stats_load()
                    df_data, assets = catalog_direct_load()
                    return stats, df_data, assets

                catalog_refresh_btn.click(
                    fn=catalog_refresh,
                    inputs=None,
                    outputs=[catalog_stats, catalog_df, catalog_direct_assets],
                )
                catalog_validate_btn.click(
                    fn=validate_paths,
                    inputs=None,
                    outputs=[catalog_validate_report],
                )
                catalog_df.select(
                    fn=on_direct_row_select,
                    inputs=[catalog_direct_assets],
                    outputs=[catalog_preview],
                )

        # Eager-load CLIP in the main thread so Metal/MLX init happens before Gradio workers run.
        # If you run the app from a context where Metal isn't available, the first search will
        # show the full error (including "Original error: ..." from mediasearch).
        def load_model_and_status() -> tuple[str, str, list[list[str | None]], list[dict[str, object]]]:
            try:
                _embedder_instance().get_text_embedding("warmup")
            except Exception:
                pass  # First search will show the error; status still updates
            stats = catalog_stats_load()
            df_data, assets = catalog_direct_load()
            return get_status_text(), stats, df_data, assets

        demo.load(
            fn=load_model_and_status,
            inputs=None,
            outputs=[status, catalog_stats, catalog_df, catalog_direct_assets],
        )

    return demo


def main() -> None:
    # Serve local media files directly (no copy to cache). Critical on macOS to avoid Gradio copying 4K files.
    # ~ and /Volumes cover media; project dir covers .thumbnails/ for VIDEO/RAW previews.
    _project_root = Path(__file__).resolve().parent
    gr.set_static_paths(paths=[str(Path.home()), "/Volumes", str(_project_root)])
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
