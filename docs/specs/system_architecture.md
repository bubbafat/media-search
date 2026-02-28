# MediaSearch v2: Deep-Dive Technical Specification

## System Vision & Executive Summary
MediaSearch v2 is a highly concurrent, distributed media discovery and AI-processing pipeline designed to index, analyze, and search massive media libraries (2,000,000+ assets and 10,000,000+ video frames). 

**The Problem:** Traditional media indexers degrade catastrophically at scale. They suffer from database locking during large deletions, they saturate local networks by repeatedly reading massive 50GB video files for different processing steps (the "Double-Read Penalty"), and they pollute pristine user storage with hidden files and sidecar metadata. 

**The Solution:** V2 is completely re-architected from the ground up to operate as a distributed system. It is built on three core philosophies:
1. **Source Immutability:** The user's original network-attached storage (NAS) is treated strictly as a read-only source. The system never pollutes the source with hidden files; all derivative work (thumbnails, proxies) lives on a fast, sharded local SSD cache.
2. **The Proxy Pipeline:** Network I/O is isolated from GPU-bound ML tasks. A heavy file is pulled across the network exactly once to generate a local proxy. Subsequent AI models (Moondream, CLIP, etc.) execute lightning-fast against the local proxy.
3. **Decentralized Scale:** There is no master dispatcher. The system relies on a pull-based queue using PostgreSQL's `FOR UPDATE SKIP LOCKED`, allowing workers to scale horizontally across multiple machines with zero race conditions.

By strictly tracking AI data provenance and utilizing soft-delete/chunked-hard-delete patterns, MediaSearch v2 ensures that the database remains highly responsive, and re-processing assets with future AI models is seamless and fast.

## 1. Core Architectural Mandates
### 1.1 Database Engine
- **Strict Requirement:** PostgreSQL 16.0 or higher.
- **Dialect:** Use only `sqlalchemy.dialects.postgresql` features for non-standard types. 
- **Concurrency:** Implement the **SKIP LOCKED** pattern for all task acquisitions. This is non-negotiable for distributed scaling to prevent race conditions.
- **Provenance:** Every metadata entry (tags, descriptions) must be explicitly associated with an `AIModel` record via its primary key. This allows the system to identify which assets need re-processing when a model is upgraded or changed.

### 1.2 Task Orchestration (The State Machine)
- **Pull-Based Logic:** Workers determine their own work by querying the `assets` table. There is no central dispatcher.
- **Lease Mechanism:** A "Claim" consists of an atomic update setting `status='processing'`, assigning a `worker_id`, and setting a `lease_expires_at` timestamp.
- **Recovery:** Any asset with `status='processing'` and `lease_expires_at < now()` is considered "Abandoned" and must be eligible for re-claiming by any healthy worker.

---

## 2. Detailed Database Schema

### 2.1 `libraries` Table
- `slug` (String, PK): URL-safe unique identifier (e.g., `nas-main`). Acts as the strict primary key to prevent duplication even in soft-deleted states.
- `name` (String): Human-readable name.
- `absolute_path` (String): The physical local or network mount path. Workers query this at runtime rather than relying on static configuration files.
- `deleted_at` (DateTime | None): Timestamp for soft-deletion. If NOT NULL, the library and its assets are considered "in the trash" and hidden from standard worker queries.
- `is_active` (Boolean): The master "Pause" switch.
- `scan_status` (Enum): `idle`, `scan_req`, `scanning`.
- `target_tagger_id` (FK): Links to `AIModel`. Defines the "Goal" state for assets. If NULL, the effective target is the system default AI model (stored in `system_metadata` and used at claim/repair time).
- `sampling_limit` (Integer): The hard cap on frames extracted per video (Default: 100).
- `sampling_policy` (JSONB): Configuration for the extraction strategy.

### 2.2 `assets` Table
- `id` (UUID or BigInt, PK): Primary identifier.
- `library_id` (FK): Reference to the parent library (`slug`).
- `rel_path` (String): Path relative to the library root. 
- **Indexing:** A **Composite Unique Index** on `(library_id, rel_path)` is mandatory.
- `type` (Enum): `image`, `video`.
- `mtime` (Float): Unix timestamp of last filesystem modification. Used for "Dirty Checks" during fast scans.
- `size` (BigInt): File size in bytes.
- `status` (Enum): `pending`, `proxied`, `extracting`, `analyzing`, `completed`, `failed`, `poisoned`.
- `tags_model_id` (FK): Records which AI model produced the *current* data.
- `retry_count` (Integer): Incremented on failure/poison; reset to 0 on success (proxied, analyzed_light, completed). If > 5, mark as `poisoned`.
- `lease_expires_at` (DateTime): Dead-man's switch for worker failure recovery.
- `preview_path` (String, nullable): **Deprecated.** Previously held the path to an animated WebP preview; the UI now uses a static scene frame as the preview image (first scene for library, best-match scene for search), derived from `video_scenes.rep_frame_path` via the API. The column remains; the Video Worker no longer sets it.
- `video_preview_path` (String, nullable): For video assets, path **relative to data_dir** to the 10-second head-clip MP4 (e.g. `video_clips/{library_slug}/{asset_id}/head_clip.mp4`). Used for hover/tap preview playback in the UI. Set when the Video Worker generates the head clip; NULL until then.
- `segmentation_version` (Integer, nullable): For video assets, encodes the `PHASH_THRESHOLD` and `DEBOUNCE_SEC` parameters used when scene indexing completed. When these parameters change, the Video Proxy Worker invalidates proxied videos (clears scene data, resets status to `pending`) so they are re-segmented. NULL for legacy assets or before indexing.

### 2.3 `video_frames` Table
- `id` (UUID or BigInt, PK): Primary identifier.
- `asset_id` (FK): Reference to parent video `Asset`.
- `timestamp_ms` (Integer): Precise temporal offset in milliseconds.
- `is_keyframe` (Boolean): True if extracted from a native I-Frame (Keyframe).
- `search_vector` (TSVector): PostgreSQL Full-Text Search index for keywords/descriptions.
- `tags_model_id` (FK): Provenance for this specific frame's analysis.

### 2.4 Video scene persistence (resumable indexing)
Scene-based video indexing (pHash + temporal ceiling + best-frame selection) is persisted so the process is **resumable** after a crash and metadata is searchable.

- **`video_scenes` Table:** One row per closed scene.
  - `id` (int, PK), `asset_id` (FK → asset.id), `start_ts`, `end_ts` (float seconds), `description` (text, nullable), `metadata` (JSONB, nullable), `sharpness_score`, `rep_frame_path` (path relative to data_dir; e.g. video_scenes/{library_slug}/{asset_id}/{start}_{end}.jpg), `keep_reason` (enum: `phash`, `temporal`, `forced`).
  - Index on `(asset_id, end_ts)` for resume queries.
- **`video_active_state` Table:** One row per asset currently being indexed (the "open" scene state).
  - `asset_id` (PK/FK → asset.id), `anchor_phash`, `scene_start_ts`, `current_best_pts`, `current_best_sharpness`.
  - Updated via **UPSERT** (`INSERT ... ON CONFLICT (asset_id) DO UPDATE`) so there are no orphaned state rows if the orchestrator is invoked multiple times for the same asset.

**Resume semantics:** (1) On startup, query `video_scenes` for `max(end_ts)` for the target `asset_id`. (2) **Seek:** Initialize the frame scanner at `max(max_end_ts - 2.0, 0)` (FFmpeg input seek). (3) **Catch-up:** Consume frames and discard until scanner PTS ≥ `max_end_ts`. (4) **State restore:** Load `video_active_state` and re-initialize the segmenter with `anchor_phash` and `scene_start_ts`. **Atomicity:** When a scene is closed, one transaction inserts into `video_scenes` and either UPSERTs or deletes `video_active_state`; on EOF the active state row is deleted.

**Segmentation versioning:** `asset.segmentation_version` tracks the `PHASH_THRESHOLD` and `DEBOUNCE_SEC` values used when scene indexing completed. If these parameters change (e.g. in code), the Video Proxy Worker detects the mismatch, clears scene data for affected assets, resets them to `pending`, and re-segments on the next pass. Legacy assets with NULL `segmentation_version` are not invalidated.

### 2.5 `worker_status` Table
- `worker_id` (String, PK): Unique identifier for the worker instance (e.g., hostname + UUID).
- `hostname` (String): Hostname of the machine the worker runs on (indexed for local-aware queries).
- `last_seen_at` (DateTime): Heartbeat timestamp.
- `state` (Enum): `idle`, `processing`, `paused`, `offline`.
- `command` (Enum): `none`, `pause`, `resume`, `shutdown`, `forensic_dump`.

---

## 3. Worker Node Architecture & Conceptual Roles

### 3.1 The Decentralized Actor Model
MediaSearch v2 abandons the traditional monolithic server model (where a central API handles file uploads, database writes, and AI processing). Instead, it uses a **Decentralized Worker Model**. 

Workers are autonomous, infinitely scalable background processes. There is no central "Master" node dispatching tasks. Instead, workers are completely stateless and pull work dynamically from the PostgreSQL `assets` table using atomic `SKIP LOCKED` queries. This allows you to run specialized workers on hardware suited for their specific task (e.g., I/O workers on a cheap NAS bridge, AI workers on a massive GPU rig).

### 3.2 Enumeration of Standard Worker Types
The pipeline is divided into specialized, isolated worker types to prevent hardware bottlenecks:

1. **The Scanner Worker (I/O & DB Bound):** - **Role:** The Discovery Engine. 
   - **Action:** Rapidly traverses the user's read-only network storage (NAS). It does *not* open or read media files. It only reads filesystem metadata (`os.stat`) to detect new or modified files and inserts them into the database with a `pending` status.

2. **The Image Proxy Worker (Network I/O & CPU Bound):**
   - **Role:** Pre-Processor & Cache Builder for **images**.
   - **Action:** Claims `pending` **image** assets. It pulls the original image files (e.g. 50MB RAW) across the network *exactly once*. For **RAW/DNG** formats it prefers **rawpy** (LibRaw) embedded preview when `use_previews` is true, then libvips thumbnail or full decode; **Pillow is not used for RAW** to avoid full-frame decode and high memory. Standard raster formats (JPEG/PNG/WebP/TIFF/BMP) are processed via **libvips (pyvips)** shrink-on-load thumbnails with `access=\"sequential\"` to bound memory.
   - **Cascade Resize Pipeline:** From the decoded representation (full-res or preview), it first generates a medium-size WebP **proxy** (max 768×768) and then generates the UI **thumbnail** (max 320×320 JPEG) from that proxy image, never upscaling:
     - If the source image is smaller than the proxy target in both dimensions, the proxy is saved at the source resolution (re-encoded only).
     - If the proxy image is smaller than the thumbnail target in both dimensions, the thumbnail is saved at the proxy resolution.
     - This ensures that icon-sized inputs (e.g., 32×32) remain 32×32 for both proxy and thumbnail while large images incur only one downscale from source to proxy, then a second, cheaper downscale from proxy to thumbnail.
   - Internally, the proxy worker operates primarily on `pyvips.Image` instances for decode/resize/encode; `PIL.Image` is only used at explicit compatibility boundaries (e.g. EXIF fix for non-RAW, or after rawpy preview). Raster formats remain libvips-first; RAW never uses Pillow.
   - After writing both derivatives to the sharded local SSD cache, it updates the asset status to `proxied`.

3. **The Video Proxy Worker (Network I/O & CPU Bound):**
   - **Role:** Pre-Processor for **videos**.
   - **Action:** Claims `pending` **video** assets. It reads the source file once and runs a **720p disposable pipeline**: transcodes to a temporary 720p H.264 file, extracts a thumbnail (frame at 0.0) from the temp, extracts a 10-second head-clip (stream copy) for UI preview, runs scene indexing (pHash, temporal ceiling, best-frame selection) from the temp with **no** vision analysis, persists scene bounds and representative frame paths, then deletes the temp file. It sets `video_preview_path` and updates the asset status to `proxied`. All derivatives (thumbnail, head-clip, scene rep frames) are produced from a single read of the source via the 720p temp.

4. **The ML / AI Worker (GPU Bound):**
   - **Role:** The Intelligence Engine.
   - **Action:** Claims `proxied` **image** assets. It never touches the network or the user's NAS. It strictly reads the lightweight local proxies from the SSD, runs them through local LLMs/Vision Models (e.g., Moondream, CLIP), extracts tags/embeddings, and updates the asset to `completed`.

5. **The Video Worker (GPU Bound, vision-only):**
   - **Role:** Vision backfill for **video** scene rep frames.
   - **Action:** Claims `proxied` **video** assets that already have scene bounds and representative frames (persisted by the Video Proxy Worker). It runs vision analysis (e.g. Moondream) only on the existing rep frame images, updates scene descriptions and metadata, and marks the asset completed. It does **not** re-read the source video or generate the head-clip (Video Proxy Worker already set `video_preview_path`).

6. **The Garbage Collector Worker (Disk & DB Bound):**
   - **Role:** The Janitor.
   - **Action:** Wakes up periodically to clean up the system. It executes chunked hard-deletions on databases for "emptied trash" libraries, and safely deletes orphaned physical proxy files from the local SSD to prevent disk bloat.

### 3.3 BaseWorker Framework & Lifecycle (Implementation) ###
Every worker must implement a non-blocking `run_loop` that manages its own lifecycle.
- **The Heartbeat:** A background thread or async task must update `worker_status.last_seen_at` and `worker_status.stats` (JSONB) every 15 seconds.
- **Signal Hook (`handle_signal`):**
    - `pause`: Transition to `paused` state. Finish the current asset being processed, then stop claiming new tasks. Poll for `resume`.
    - `resume`: Transition back to `idle` and resume the task claim cycle.
    - `shutdown`: Finish current asset, update state to `offline`, and terminate the process gracefully.
- **Priority:** OS signals (SIGINT/SIGTERM) must trigger the same graceful `shutdown` sequence to ensure DB consistency.

---

## 4. The 100-Chunk Extraction Algorithm

When a Video Worker processes an asset, it must follow this exact sequence:
1. **Segmenting:** Divide the total video duration by the `library.sampling_limit` value into `N` equal temporal windows (segments).
2. **Keyframe Probing:** Use `ffprobe` to identify the timestamps of all I-Frames (keyframes) within each segment.
3. **Selection Logic (Per Window):**
    - **Case 0 I-Frames:** Extract the frame at the absolute temporal center of the window.
    - **Case 1 I-Frame:** Extract this keyframe.
    - **Case >1 I-Frames:** - Extract all I-Frames in the window downscaled to 32x32 pixels.
        - Calculate a **Pixel-Wise Mean Frame** (average color value per pixel across all extracted I-frames in the segment).
        - Select the I-Frame with the highest **Structural Similarity (SSIM)** or lowest **Mean Squared Error (MSE)** compared to that Mean Frame.
        - *Rationale:* This identifies the most "consistent" or "representative" frame, discarding black/white/blur transitions.

**Foundation:** Frame extraction is built on a `VideoScanner` (persistent FFmpeg pipe with synchronized PTS from stderr), yielding `(frame_bytes, pts)` for indexing. Optionally, `SceneSegmenter` wraps `VideoScanner` to perform semantic scene segmentation (pHash drift, 30s temporal ceiling, 3s debounce) and best-frame selection (Laplacian sharpness, skipping the first two frames per scene), yielding one representative frame per scene. Scene results are persisted to `video_scenes` with deterministic resume via `video_active_state` (see §2.4).

---

## 5. Observability & The "Black Box" Flight Log

### 5.1 The Logging I/O Problem
In a high-throughput distributed system processing millions of assets and frames, standard disk-based logging is a critical anti-pattern. Writing `DEBUG` or `INFO` statements to a log file for every database transaction, network claim, or AI inference will quickly burn out SSDs (I/O exhaustion), saturate system resources, and generate terabytes of useless noise. 

### 5.2 The "Flight Log" Concept
To solve this, MediaSearch v2 utilizes a **"Black Box" Flight Log** architecture. Instead of continuously writing to disk, every worker maintains a high-fidelity, circular in-memory buffer. 

It records everything the worker does in real-time. If the worker remains healthy, the oldest logs naturally fall off the end of the buffer into oblivion. The system assumes that *successful* processing does not need to be permanently memorialized.

The data is only materialized to physical storage when a critical failure occurs (a crash) or when an administrator explicitly requests a diagnostic snapshot. This guarantees that when an error happens, developers have the exact contextual history leading up to the crash, without paying the I/O tax during normal operations.

### 5.3 Implementation Directives
- **Type:** In-memory `collections.deque` (Thread-safe circular buffer).
- **Capacity:** Exactly 50,000 entries per worker.
- **Policy:** - No standard disk logging for `DEBUG`/`INFO` levels to preserve SSD IOPS and prevent log bloat.
  - **The Triggered Dump:** On an unhandled exception (crash) or upon receiving the `forensic_dump` command via the `worker_status` table, the worker must instantly flush the entire in-memory buffer to a physical file at `/logs/forensics/{worker_id}_{timestamp}.log`.

---

## 6. Verification & Testing
- **Testcontainers:** Every integration test must utilize a `postgres:16-alpine` container via the `testcontainers-python` library.
- **Migration Enforcement:** - Every Alembic migration must have a test verifying `upgrade head` and `downgrade base`.
    - Verification tests must explicitly assert the presence of the `Asset` composite index and the `TSVECTOR` column type in the resulting schema.

---

## 7. Data Locality & The Proxy Pipeline
To ensure high performance, UI responsiveness, and absolute security of the user's source media, MediaSearch v2 strictly enforces a **Proxy Architecture**. 

- **Read-Only Source:** The user's original media directories (e.g., network NAS mounts) must be treated as **Read-Only**. Workers must never write hidden files, thumbnails, or metadata sidecars to the source directories.
- **Local Application Cache:** All derivative files must be written to a dedicated, high-speed local storage directory (e.g., `/data`). To prevent filesystem limits, all cached files must be sharded by asset ID (e.g., `/data/thumbnails/{asset_id % 1000}/`).

### 7.1 The Three-Stage Processing State Machine
Processing heavy media over a network requires separating I/O-bound tasks from GPU-bound tasks to prevent "Double-Read Penalties."
1. **Stage 1 (Discovery):** Scanner finds a file on the NAS. Inserts DB row as `pending`.
2. **Stage 2 (Proxy Generation):** For **images**, the Image Proxy Worker claims `pending` assets, reads the source once, and uses libvips shrink-on-load thumbnails (with sequential access) to generate an AI-optimized WebP Proxy (max 768px) and a UI Thumbnail (max 320px JPEG) on the local SSD, then sets status to `proxied`. For **videos**, the Video Proxy Worker claims `pending` assets, reads the source once into a temporary 720p H.264 file, generates thumbnail, 10-second head-clip (stream copy), and scene index (pHash, rep frames) from that temp, then deletes the temp and sets status to `proxied` and `video_preview_path`.
   - For RAW/DNG images, the proxy stage prefers **rawpy** (LibRaw) embedded preview when previews are enabled; if rawpy is unavailable or has no embedded thumb, it falls back to libvips thumbnail or full decode. Pillow is not used for RAW. Advanced users can disable previews via configuration/CLI (`--ignore-previews`) when they require a true RAW rendering; then a full-resolution libvips decode is used before generating the proxy/thumbnail cascade.
3. **Stage 3 (AI Extraction):** The ML Worker claims `proxied` **image** assets and reads only the local SSD proxy. The Video Worker claims `proxied` **video** assets and runs vision analysis only on the already-persisted scene rep frame images (no source read). Both update the DB to `completed`.

---

## 8. CLI Tooling & Data Lifecycle (Soft vs. Hard Deletion)
MediaSearch v2 relies on a synchronous Typer CLI (`src/cli.py`) for system administration, allowing developers and users to bypass background workers for immediate execution.

### 8.1 Administrative Commands
- `library add <name> <path>`: Enforces primary key uniqueness on the generated slug.
- `scan <slug>`: A one-shot synchronous execution of the ScannerWorker, bypassing the daemon loop for immediate discovery.
- `asset list <slug>`: Terminal-native querying of discovered files and their current pipeline status.

### 8.2 The Soft-Delete Pattern
Deleting libraries with millions of associated assets and video frames via a standard `DELETE` cascade will lock the database.
- **Soft Deletion (`library remove`):** Sets `deleted_at = now()` on the Library. All standard Repositories automatically append `WHERE deleted_at IS NULL` to queries, instantly hiding the data without locking tables.
- **Collision Prevention:** The system must actively prevent the creation of a new library if its generated slug matches a soft-deleted library in the trash, prompting the user to either restore the old library or choose a different name.

### 8.3 Chunked Hard Deletion ("Garbage Collection")
When a library is permanently deleted (`trash empty`), the repository must execute a **Chunked Deletion Loop**:
1. Iterate over the library's assets, deleting them in batches of 5,000 using `WHERE id IN (SELECT id ... LIMIT 5000)`.
2. Commit the transaction after every batch to release DB locks and allow concurrent UI/Worker queries.
3. Once all assets and frames are purged, physically delete the `Library` row.
4. Physical local proxy files (thumbnails) orphaned by this process are eventually swept by a separate background Garbage Collector worker.
