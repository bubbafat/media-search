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
- `slug` (String, PK): URL-safe unique identifier (e.g., `nas-main`).
- `name` (String): Human-readable name.
- `is_active` (Boolean): The master "Pause" switch. If `false`, no assets in this library may be claimed by any worker.
- `scan_status` (Enum): `idle`, `scan_requested`, `scanning`.
- `target_tagger_id` (FK): Links to `AIModel`. Defines the "Goal" state for assets within this library.
- `sampling_limit` (Integer): The hard cap on frames extracted per video (Default: 100).
- `sampling_policy` (JSONB): Configuration for the extraction strategy (e.g., threshold for mean-frame analysis).

### 2.2 `assets` Table
- `id` (UUID or BigInt, PK): Primary identifier.
- `library_id` (FK): Reference to the parent library.
- `rel_path` (String): Path relative to the library root. 
- **Indexing:** A **Composite Unique Index** on `(library_id, rel_path)` is mandatory to ensure scanner lookup performance and data integrity.
- `type` (Enum): `image`, `video`.
- `mtime` (Float): Unix timestamp of last filesystem modification.
- `size` (BigInt): File size in bytes.
- `status` (Enum): `pending`, `extracting`, `analyzing`, `completed`, `failed`, `poisoned`.
- `tags_model_id` (FK): Records which AI model produced the *current* data.
- `retry_count` (Integer): Incremented on every claim attempt. If `retry_count > 5`, the asset must be marked `poisoned` and ignored.
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
- **Migration Enforcement:** - Every Alembic migration must have a test verifying `upgrade head` and `downgrade base`.
    - Verification tests must explicitly assert the presence of the `Asset` composite index and the `TSVECTOR` column type in the resulting schema.