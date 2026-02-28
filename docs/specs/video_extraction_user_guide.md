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
* **Final Scene Flush:** At EOF, the last open scene is closed and yielded with `keep_reason=forced`. When the video duration (from ffprobe) is known and exceeds the PTS of the last decoded frame, the final scene's `end_ts` is extended to the duration so the tail of the video (e.g. 2–5 seconds that fps=1 sampling may miss) remains searchable.

If you change these parameters (e.g. PHASH_THRESHOLD or DEBOUNCE_SEC in `scene_segmenter.py`), the system will automatically invalidate existing proxied videos and re-segment them on the next Video Proxy Worker pass. No manual reindex is needed.



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

### Running the pipeline (Video Proxy Worker and Video Worker)
Scene indexing is split into two stages. The **Video Proxy Worker** (CLI: `video-proxy`) claims **pending** video assets. It reads the source once and runs the **720p disposable pipeline**: transcodes to a temporary 720p H.264 file, extracts a thumbnail (frame at 0.0) from the temp, extracts a 10-second head-clip (stream copy) for UI preview, runs scene detection (pHash, temporal ceiling, best-frame selection) from the temp **without** vision analysis, persists scene bounds and representative frame paths to the database and disk, then deletes the temp file. It sets `video_preview_path` and updates the asset to **proxied**.

The **Video Worker** (CLI: `ai video`) claims **proxied** (or **analyzed_light**, see below) video assets that already have scene data. It runs **vision analysis only** on the existing representative frame images (e.g. Moondream), updates scene descriptions and metadata in the database, and marks the asset completed. It does not re-read the source video or generate the head-clip. This keeps GPU work separate from the single source read and avoids the "double-read penalty."

### Tiered Video Pipeline (Light → Full)
The Video Worker supports a two-pass tiered pipeline via `--mode`:
- **Light mode:** Claims `proxied` assets. Runs vision analysis for description and tags only (no OCR). Marks asset `analyzed_light`.
- **Full mode:** Claims `analyzed_light` assets. Adds OCR to scenes that already have descriptions. Merges OCR into existing metadata without overwriting Light tags or descriptions. Marks asset `completed`.

### Scene Index Truncation and Retries
The scene indexing pipeline verifies that indexing reached the actual end of the video. If the decoder stops early (e.g., hardware decoder error or premature EOF), `max(end_ts)` will be short of the video duration. In that case, `run_video_scene_indexing` raises a `ValueError` with message `"Video index truncated: indexed to Xs but duration is Ys; decoder may have stopped early."` The Video Proxy Worker treats this as a **retryable** failure: the asset is marked `failed` (not `poisoned`), and another worker may retry. On retry, the worker uses software decode (no hwaccel) if hardware decode previously failed; the duration check ensures partial runs are never marked successful.

### Strict Merge Policy
To prevent "Incomplete Scenes" and data loss, vision backfill uses a **Strict Merge** policy:
- **Fetch before save:** Before writing any vision data, the worker fetches the current scene metadata from the database. This avoids overwriting with stale in-memory data from a prior list.
- **Model version check:** Before processing a scene, the worker compares the asset's `analysis_model_id` or `tags_model_id` to the current effective model. If they differ (e.g., asset was analyzed by an older model), the full vision pass (mode=light) is re-run for that scene instead of merging. This prevents hybrid model metadata corruption and ensures the library converges on the latest model's output.
- **Deep merge:** When adding Full OCR to a scene, existing Light data (description, tags, top-level keys like `showinfo`) is preserved. Only `ocr_text` is added or updated—*provided* the asset's model IDs match the worker's effective model (see model version check).
- **Completion safety:** Before marking an asset `completed`, the worker verifies that *all* scenes have both a description and (if applicable) OCR. If any are missing, it runs the appropriate pass (Light for missing descriptions, Full for missing OCR) before final completion.

To support long-running videos, the Video Proxy Worker can be interrupted (SIGINT/SIGTERM); the asset is set back to pending so another worker can resume. The Video Worker supports graceful shutdown: on interrupt the asset is set back to proxied (or analyzed_light).

After the pipeline completes, the **preview image** shown on result and library cards is a **static scene frame**: for search results it is the best-match scene’s representative frame (from `video_scenes.rep_frame_path`); for the library browser it is the first scene’s representative frame. The UI loads these via the API’s `preview_url`; on desktop, hovering over a video card plays the head-clip; on mobile, tapping the preview toggles playback.

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