# MediaSearch v2 — Code Review Report

**Audit date:** 2025-02-25  
**Sources of truth:** `.cursorrules`, `docs/specs/system_architecture.md`

---

## 🔴 Critical Violations

### 1. Migration verification does not assert TSVECTOR

**Spec (Section 6):** *"Verification tests must explicitly assert the presence of the Asset composite index and the **TSVECTOR column type** in the resulting schema."*

**Finding:** `tests/test_migrations.py` asserts the composite unique index on `asset (library_id, rel_path)` but **does not assert** that the `videoframe.search_vector` column is of type `tsvector`.

**Location:** `tests/test_migrations.py` — add an assertion after upgrade (e.g. query `information_schema.columns` or `pg_attribute` for `videoframe.search_vector` and confirm type is `tsvector`).

---

### 2. Spec table name mismatch: `worker_status` vs `workerstatus`

**Spec (Section 2.4):** *"`worker_status` Table"* — the document uses the name `worker_status` (with underscore).

**Finding:** The implementation uses table name `workerstatus` (no underscore): `WorkerStatus.__tablename__ = "workerstatus"` in `src/models/entities.py` and migrations create/drop `"workerstatus"`. This is a **spec/code naming drift**. If the spec is the contract for external tooling or docs, this can cause confusion.

**Location:** `src/models/entities.py` (line 101), migrations `001` and any references. Either align the spec to `workerstatus` or rename the table to `worker_status` (would require a new migration).

---

### 3. Future asset-claiming must use FOR UPDATE SKIP LOCKED

**Spec (Section 1.1):** *"Implement the **SKIP LOCKED** pattern for **all task acquisitions**."*  
**Spec (Section 1.2):** Asset claim is an atomic update (status, worker_id, lease_expires_at).

**Finding:** Today only **library** claiming is implemented (`claim_library_for_scanning` in `asset_repo.py`), and it correctly uses `FOR UPDATE SKIP LOCKED`. There is no asset-claiming path yet (no extractor/analyzer workers). When you add workers that claim **assets** (e.g. set `status='processing'`, `worker_id`, `lease_expires_at`), that claim **must** be implemented with `SELECT ... FOR UPDATE SKIP LOCKED` (or equivalent atomic update) to avoid race conditions. No current bug, but a **mandatory constraint** for future work.

---

## 🟡 Code Smells & Refactoring Targets

### 4. ScanStatus enum vs spec

**Spec (Section 2.1):** Library `scan_status` values include `full_scan_requested`, `fast_scan_requested`, `scanning`.  
**Code:** `ScanStatus` in `src/models/entities.py` has `idle`, `scan_req`, `scanning` (single “scan requested” value).

**Impact:** Intentional simplification is fine, but the spec should be updated to match, or the code extended to support full vs fast scan if product needs it.

**Location:** `src/models/entities.py` (ScanStatus), `src/repository/asset_repo.py` (uses `'scan_req'` in SQL).

---

### 5. Forensic dump path: relative vs absolute

**Spec (Section 5):** *"write the entire buffer to **/logs/forensics/**{worker_id}_{timestamp}.log"* (absolute path).  
**Code:** `src/core/logging.py` uses `DEFAULT_FORENSICS_DIR = "logs/forensics"` (relative to CWD).

**Impact:** Relative path is more portable but can write to unexpected directories depending on process CWD. Consider making this configurable or aligning with spec (e.g. config-driven forensics dir).

**Location:** `src/core/logging.py` (line 12).

---

### 6. Testcontainers image: `postgres:16` vs `postgres:16-alpine`

**Spec (Section 6):** *"Every integration test must utilize a **postgres:16-alpine** container."*  
**Code:** `tests/conftest.py`, `tests/test_migrations.py`, `tests/test_ui_api.py`, `tests/test_system_guard.py` use `PostgresContainer("postgres:16")`.

**Impact:** Functional difference is small; spec compliance and consistency suggest either changing tests to `postgres:16-alpine` or updating the spec to `postgres:16`.

---

### 7. Deprecated `datetime.utcnow`

**Finding:** `src/models/entities.py` line 105 uses `default_factory=datetime.utcnow` for `WorkerStatus.last_seen_at`. `datetime.utcnow` is deprecated in Python 3.12+ in favor of timezone-aware `datetime.now(timezone.utc)`.

**Location:** `src/models/entities.py`. Prefer e.g. `lambda: datetime.now(timezone.utc)` (and add `from datetime import timezone` if missing).

---

### 8. Missing `docs/specs/requirements.md`

**`.cursorrules`:** *"The broad project requirements are in **docs/specs/requirements.md** — refer to this document when making plans."*

**Finding:** The file `docs/specs/requirements.md` does not exist. Either add it or update `.cursorrules` to point to the actual requirements source.

---

### 9. UIRepository and raw SQL vs ORM mix

**Finding:** `UIRepository.get_library_stats()` uses raw `text("SELECT COUNT(*) ...")` while `get_worker_fleet()` uses `select(WorkerStatusEntity)`. Both are inside the repository layer (acceptable per “no ORM in business logic”). The mix is a style/consistency smell; consider using either raw SQL or ORM consistently per repo for readability.

**Location:** `src/repository/ui_repo.py`.

---

## 🟢 Architectural Wins

- **No DDL in workers or API:** No `SQLModel.metadata.create_all()` or Alembic imports in `src/workers/` or `src/api/`. Only the string *"Run migrations (alembic upgrade head)"* appears in a user-facing error in `base.py`.
- **FOR UPDATE SKIP LOCKED for library claiming:** `AssetRepository.claim_library_for_scanning()` uses `FOR UPDATE SKIP LOCKED` and atomic update of `scan_status` to `'scanning'` within the same transaction.
- **Atomic upsert and dirty detection:** `AssetRepository.upsert_asset()` uses `INSERT ... ON CONFLICT (library_id, rel_path) DO UPDATE` and only resets `status` to `'pending'` and clears `tags_model_id` when `mtime` or `size` differ (`CASE WHEN asset.mtime IS DISTINCT FROM EXCLUDED.mtime OR asset.size IS DISTINCT FROM EXCLUDED.size`). Unchanged files keep existing status.
- **Session management:** All repositories use a `_session_scope()` context manager and close sessions in `finally`, avoiding connection leaks: `AssetRepository`, `WorkerRepository`, `UIRepository`, `SystemMetadataRepository`.
- **BaseWorker pre-flight:** `BaseWorker._check_compatibility()` reads `system_metadata.schema_version` and raises `RuntimeError` if missing or not equal to `REQUIRED_SCHEMA_VERSION` before the worker does work.
- **Signal handling:** BaseWorker implements `handle_signal(pause|resume|shutdown)`; run loop polls `get_command()`, calls `handle_signal`, and clears command. SIGINT/SIGTERM set `should_exit` for graceful shutdown; state is set to `offline` in `finally`.
- **Scanner respects pause/shutdown:** `ScannerWorker.process_task()` passes `should_stop = lambda: self.should_exit or self._state == WorkerState.paused` into `_scan_dir()`; the recursive loop checks `should_stop()` at entry and after each stats update, so the `os.scandir` walk exits and library is returned to idle in `finally`.
- **Flight logger:** In-memory `collections.deque` with capacity 50,000; no disk logging for DEBUG/INFO by default; dump to disk only on `forensic_dump` or (per spec) on unhandled exception. Implemented in `src/core/logging.py` with `FlightLogger` and `setup_logging()`.
- **Logging not print:** No `print()` in `src/`; scanner and base worker use `logging.info` / `logging.debug` / `logging.error`.
- **UI decoupling:** Nothing in `src/api/` imports from `src/workers/`. Dashboard uses only `UIRepository` and `SystemMetadataRepository`.
- **Library paths:** Library roots are resolved only via `path_resolver.get_library_root(library_slug)` which reads from `get_config().library_roots`; no hardcoded filesystem paths for library roots in business code.
- **Composite unique index:** Migration `002_asset_library_rel_path_unique.py` enforces a unique index on `asset (library_id, rel_path)`; `tests/test_migrations.py` asserts its existence and that it is UNIQUE.
- **Repository pattern:** Database access is encapsulated in repository classes; workers and API use repos only, not raw ORM in business logic.
- **BaseWorker hook:** ScannerWorker inherits from BaseWorker and implements the expected lifecycle; signal handling is centralized in BaseWorker.

---

## Action Plan (Prioritized)

| Priority | Item | File(s) | Action |
|----------|------|---------|--------|
| 1 | TSVECTOR assertion in migration test | `tests/test_migrations.py` | Add assertion that `videoframe.search_vector` column type is `tsvector` after `upgrade head`. |
| 2 | Resolve worker_status vs workerstatus | `docs/specs/system_architecture.md` or `src/models/entities.py` + new migration | Either update spec to `workerstatus` or add migration to rename table to `worker_status` and update entities. |
| 3 | Document or implement asset-claim SKIP LOCKED | Future asset-claiming code | When adding asset-claiming, use `FOR UPDATE SKIP LOCKED` (or equivalent) and document in architecture spec. |
| 4 | Fix deprecated datetime.utcnow | `src/models/entities.py` | Replace with `datetime.now(timezone.utc)` (or equivalent). |
| 5 | Add or fix requirements reference | `docs/specs/requirements.md` or `.cursorrules` | Create `docs/specs/requirements.md` or point .cursorrules at the real requirements doc. |
| 6 | Forensic dump path | `src/core/logging.py` / config | Make forensics directory configurable or align with spec (`/logs/forensics` or config). |
| 7 | ScanStatus vs spec | `docs/specs/system_architecture.md` or `src/models/entities.py` | Align spec with single `scan_req` or add full_scan_requested / fast_scan_requested. |
| 8 | Testcontainers image | `tests/conftest.py`, `tests/test_migrations.py`, etc. | Switch to `postgres:16-alpine` or update spec to `postgres:16`. |
| 9 | UIRepository consistency | `src/repository/ui_repo.py` | Optional: standardize on raw SQL or ORM for readability. |

---

*Report generated from a full pass over `src/`, `tests/`, and `migrations/` against the stated directives and spec.*
