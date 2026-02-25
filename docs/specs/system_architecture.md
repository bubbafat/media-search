# MediaSearch v2: Deep-Dive Technical Specification

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
- `target_tagger_id` (FK): Links to `AIModel`. Defines the "Goal" state for assets.
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
- `retry_count` (Integer): Incremented on every claim attempt. If > 5, mark as `poisoned`.
- `lease_expires_at` (DateTime): Dead-man's switch for worker failure recovery.

### 2.3 `video_frames` Table
- `id` (UUID or BigInt, PK): Primary identifier.
- `asset_id` (FK): Reference to parent video `Asset`.
- `timestamp_ms` (Integer): Precise temporal offset in milliseconds.
- `is_keyframe` (Boolean): True if extracted from a native I-Frame (Keyframe).
- `search_vector` (TSVector): PostgreSQL Full-Text Search index for keywords/descriptions.
- `tags_model_id` (FK): Provenance for this specific frame's analysis.

### 2.4 `worker_status` Table
- `worker_id` (String, PK): Unique identifier for the worker instance (e.g., hostname + UUID).
- `last_seen_at` (DateTime): Heartbeat timestamp.
- `state` (Enum): `idle`, `processing`, `paused`, `offline`.
- `command` (Enum): `none`, `pause`, `resume`, `shutdown`, `forensic_dump`.

---

## 3. BaseWorker Framework & Lifecycle

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

---

## 5. Observability (The Flight Log)
- **Type:** In-memory `collections.deque`.
- **Capacity:** Exactly 50,000 entries.
- **Policy:** - No standard disk logging for DEBUG/INFO levels to preserve SSD IOPS and prevent log bloat.
    - **Forced Dump:** On unhandled exception or receiving the `forensic_dump` command, the worker must write the entire buffer to `/logs/forensics/{worker_id}_{timestamp}.log`.

---

## 6. Verification & Testing
- **Testcontainers:** Every integration test must utilize a `postgres:16-alpine` container via the `testcontainers-python` library.



## 7. Data Locality & The Proxy Pipeline
To ensure high performance, UI responsiveness, and absolute security of the user's source media, MediaSearch v2 strictly enforces a **Proxy Architecture**. 

- **Read-Only Source:** The user's original media directories (e.g., network NAS mounts) must be treated as **Read-Only**. Workers must never write hidden files, thumbnails, or metadata sidecars to the source directories.
- **Local Application Cache:** All derivative files must be written to a dedicated, high-speed local storage directory (e.g., `/data`). To prevent filesystem limits, all cached files must be sharded by asset ID (e.g., `/data/thumbnails/{asset_id % 1000}/`).



### 7.1 The Three-Stage Processing State Machine
Processing heavy media over a network requires separating I/O-bound tasks from GPU-bound tasks to prevent "Double-Read Penalties."
1. **Stage 1 (Discovery):** Scanner finds a file on the NAS. Inserts DB row as `pending`.
2. **Stage 2 (Proxy Generation):** A Proxy/Thumbnail worker claims the `pending` asset. It reads the heavy source file across the network *exactly once*. It generates a small UI Thumbnail (`320px`) and an AI-optimized Proxy (`1024px`) on the local SSD. It updates the DB to `proxied`.
3. **Stage 3 (AI Extraction):** The ML Worker claims the `proxied` asset. It reads *only* the local SSD proxy, bypassing the network entirely. It updates the DB to `completed`.

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
- **Migration Enforcement:** - Every Alembic migration must have a test verifying `upgrade head` and `downgrade base`.
    - Verification tests must explicitly assert the presence of the `Asset` composite index and the `TSVECTOR` column type in the resulting schema.
