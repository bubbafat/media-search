# NLE Companion: Search & Indexing Architecture Upgrade
# Staged Implementation Plan

[cite_start]**Document Purpose:** This outlines the staged upgrade from PostgreSQL Full-Text Search to a Quickwit-backed, Multi-Index discovery layer[cite: 4, 5, 23, 24]. 

**Execution Strategy (JIT Prompting):** Stages must be executed linearly. [cite_start]Do not start Stage N+1 until Stage N passes verification[cite: 10]. Instead of hardcoding all prompt code here, the executing engineer or AI agent must use a Just-In-Time (JIT) prompting strategy: read the intent of the stage, verify the current codebase state, and write the specific code for that stage, adhering strictly to the constraints below.

---

### [cite_start]STAGE 1: Infrastructure Initialization [cite: 39]
* [cite_start]**Intent:** Add the Quickwit search engine to the deployment stack[cite: 39].
* [cite_start]**Actions:** Update `docker-compose.yml` to include the `quickwit` service and `quickwit_data` volume[cite: 44, 45]. [cite_start]Do not modify the existing `postgres` service[cite: 52].
* [cite_start]**Success Criteria:** `curl http://127.0.0.1:7280/health/livez` returns `{"status":"success"}`[cite: 82].

### [cite_start]STAGE 2: PostgreSQL Schema (Policy & Outbox) [cite: 87]
* **Intent:** Establish the control plane for Multi-Index routing and the Outbox queue for split-brain mutation prevention.
* [cite_start]**Actions:** * Create Alembic migration[cite: 93].
    * [cite_start]Add `library_model_policy` table[cite: 94]. (Note: Update schema from original plan to support string-based `active_index_name` instead of integer IDs to support the Multi-Index pattern).
    * Add `search_sync_queue` table (Columns: `id`, `asset_id`, `action`, `created_at`).
    * Add PostgreSQL Triggers to `asset` and `video_scenes` tables to write `UPSERT` and `DELETE` actions to `search_sync_queue` automatically.
* [cite_start]**Success Criteria:** Alembic upgrades successfully to head[cite: 130]; trigger functions are verified in the database schema.

### [cite_start]STAGE 3: Data Access Layer (Repositories) [cite: 138]
* [cite_start]**Intent:** Create the Python SQLModel entities and repositories for the new tables before wiring them into the application[cite: 142].
* **Actions:** * Update `src/models/entities.py`.
    * Create `LibraryModelPolicyRepository` and `SearchSyncQueueRepository`.
    * Write tests to ensure CRUD operations and queue claiming (`FOR UPDATE SKIP LOCKED`) work correctly.
* **Success Criteria:** `pytest` passes for new repository unit tests.

### [cite_start]STAGE 4: Quickwit Schema Definition [cite: 244]
* [cite_start]**Intent:** Define the precise JSON schema template for Quickwit indexes[cite: 246].
* [cite_start]**Actions:** * Create `quickwit/media_scenes_index_template.json`[cite: 257, 258]. 
    * **Constraint Checklist:** Ensure *every* UI facet is included (e.g., `is_favorite`, `offline_ready`, `preview_ready`, `library_slug`). 
    * Ensure the JSON acts as a template so the backend can inject dynamic `index_id`s (e.g., `media_scenes_model_1`) at runtime.
* [cite_start]**Success Criteria:** Successfully POST a compiled version of the template to Quickwit and verify `GET /api/v1/indexes/{index_name}` returns the configuration[cite: 359].

### [cite_start]STAGE 5: Multi-Index Search Repository [cite: 363]
* [cite_start]**Intent:** Implement `QuickwitSearchRepository`[cite: 369].
* **Actions:** * Implement `search()`. 
    * **Crucial Deviation from Old Plan:** Do *not* filter by `model_id` or `is_active`. Instead, build the request to route to a comma-separated list of active index names retrieved from the policy table.
* **Success Criteria:** Manually indexing a document and executing a search via the repository class returns the document correctly.

### [cite_start]STAGE 6: The Event-Driven Sync Worker [cite: 427]
* **Intent:** Replace the sequential tracker with an Outbox queue consumer.
* [cite_start]**Actions:** * Implement `SearchSyncWorker`[cite: 433].
    * [cite_start]Instead of `last_synced_asset_id`[cite: 440], the worker drains rows from `search_sync_queue`.
    * If action is `DELETE`, send HTTP DELETE to Quickwit.
    * If action is `UPSERT`, send HTTP DELETE (to clear old versions), fetch fresh data from Postgres, and POST to Quickwit.
* **Success Criteria:** Triggering an asset update in Postgres successfully reflects in Quickwit after a worker run.

### [cite_start]STAGE 7: API Cutover & Fallback Safeguards [cite: 462]
* [cite_start]**Intent:** Wire the Fast API endpoints to use Quickwit, with explicit, stateless, loud-fallback safety mechanisms[cite: 468].
* **Actions:** * Update `/api/search` (or `/api/scenes`) to use the new repository.
    * **Stateless Routing:** Do NOT use `@lru_cache` for the repository injection. Query Postgres for the active policy on every request.
    * **No Silent Failures:** The `try/except` block falling back to PostgreSQL MUST log a `CRITICAL` or `ERROR` traceback before falling back.
* **Success Criteria:** Live API requests hit Quickwit. Disabling the Quickwit container forces the API to log an explicit error and seamlessly fall back to Postgres FTS.