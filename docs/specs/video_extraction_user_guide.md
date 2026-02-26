# Video Extraction & Scene Indexing: System Architecture

## 1. High-Level Objective
The goal of this pipeline is to transform raw video files into a searchable, semantic database of **Scenes**. Unlike simple frame-sampling, this system identifies logical camera cuts and "visual drifts" to group frames into meaningful segments, then uses AI (**Moondream2**) to describe the visual content.

---

## 2. The Extraction Engine (The "Inner Loop")
To process 1 hour of video in under 3 minutes, we avoid the overhead of spawning thousands of individual FFmpeg processes.

### Persistent Pipe Synchronization
We open a single, long-running FFmpeg pipe. Because FFmpeg provides pixel data on `stdout` and metadata on `stderr` asynchronously, we implement a strict **Pairing Contract**:

* **Low-Res Stream:** FFmpeg outputs raw RGB24 frames at 1 FPS, scaled to **480px width** (even height) to minimize memory churn. The scanner passes explicit width and height to the scale filter so Python and FFmpeg stay in sync.
* **Metadata Extraction:** We parse `pts_time` from the `showinfo` filter on `stderr`.
* **The Synchronized Queue:** A `pts_queue` ensures that every frame read from `stdout` is paired with its exact timestamp. 
* **The Heartbeat:** If PTS for the current frame is not received from stderr within 10 seconds, the system fails-fast (FFmpeg hung or stderr thread died).



---

## 3. Scene Segmentation Strategy
We use a **Composite Comparison Strategy** to decide when a "Scene" starts and ends.

* **pHash Drift (Perceptual):** We calculate a 256-bit `imagehash.phash`. A new scene is triggered if the Hamming distance between the current frame and the **Anchor Frame** (the first frame of the scene) exceeds 51 bits.
* **Temporal Ceiling:** To prevent infinite scenes in static or slow-moving shots, a new scene is forced every **30 seconds**.
* **Debounce Guard:** To prevent "jitter" from camera flashes or rapid movement, new scene triggers are ignored if they occur within **3 seconds** of the last cut (unless forced by the 30s ceiling).



---

## 4. Representative Frame Selection ("Best-So-Far")
For every open scene, we track the **Representative Frame** in real-time without storing every frame in memory.

* **Sharpness Scoring:** We calculate the **Laplacian Variance** of every frame.
* **Selection Logic:** We store the `bytes` and `PTS` of the sharpest frame found so far. We skip the first 2 frames of every scene to avoid transition motion blur or fade-ins.
* **High-Res Extraction:** Once a scene closes, we perform a **Targeted Seek** (`-ss [pts-0.5]`) and decode a 1-second window to extract that specific frame at **original resolution** for AI analysis.

---

## 5. Persistence & Deterministic Resume
The system is built on **PostgreSQL** and designed to survive crashes or manual interruptions.

1.  **`video_scenes`**: Stores the finalized metadata, AI descriptions, and paths to high-res thumbnails.
2.  **`video_active_state`**: Stores the "Checkpointer" for the currently open/unclosed scene (**Anchor Hash**, **Start Time**, **Current Best Sharpness**).

### The Resume Flow
When a job restarts, the system:
1.  Queries the DB for the `max(end_ts)`.
2.  Seeks the FFmpeg pipe to `max_end_ts - 2 seconds` (the overlap).
3.  Restores the `ActiveSceneState` to "prime" the segmenter with the exact anchor and best-frame data it had before the crash.
4.  Discards frames until `PTS >= max_end_ts`, then resumes processing seamlessly.

### Running the pipeline (Video worker)
The scene-indexing pipeline is driven by the **Video worker**, started via the CLI with `ai video`. The worker claims pending video assets from the database, runs the pipeline (scene detection, best-frame selection, optional AI analysis), and marks assets completed or poisoned. To support long-running videos safely, the worker **renews the asset lease** after each closed scene and supports **graceful shutdown**: on SIGINT/SIGTERM the pipeline is interrupted (per-frame check) and the asset is set back to pending so another worker can resume later.

After scene indexing completes, the system writes an animated **preview.webp** (WebP) in the asset’s scene folder (`data/video_scenes/<library_slug>/<asset_id>/preview.webp`) and sets **asset.preview_path** to that path (relative to data_dir). Previews are generated with **320px as the long-side resolution** and **preserve the original video aspect ratio** (no square padding). This file can be used as a hover preview in the UI (e.g. static thumbnail by default; on hover, the animated WebP plays without audio). The **asset.preview_path** column (relative to data_dir) is the single source of truth: a video "has a preview" if and only if it is set. When the scene index is cleared (e.g. reindex-videos or asset reindex), preview_path is set to NULL. The UI/API should use this field or a backend-derived URL, and must not infer preview location from library_slug/asset_id. To rebuild existing previews from scene JPEGs (no reindex), or to create previews for videos indexed before this feature, run:

```bash
uv run media-search ai video --repair
```

Use `--library <slug>` to limit repair to one library.

---

## 6. AI Analysis & Deduplication
The final stage involves feeding the high-res representative frame into **Moondream2**.

* **Semantic Merging:** We use `rapidfuzz` (token-ratio) to compare the generated description with the previous scene. If the descriptions are >85% similar (e.g., "A person cooking" vs "A man in a kitchen"), we merge them to keep the search index clean.
* **JSONB Storage:** All raw AI output and FFmpeg technical metadata are stored in a Postgres `jsonb` column for future-proofing and deep-data queries.

---

## 7. Performance Constraints
* **Resolution:** Low-res scanner operates at **480px** width.
* **Memory:** No raw frame copying between processes; hashing and comparison happen in the same process as the pipe reader.
* **Target:** 1 hour of 4K video indexed in **< 3 minutes** (excluding Moondream inference time).