# MediaSearch CLI User Guide

The MediaSearch admin CLI is a Typer-based tool for library management, trash handling, asset listing, and one-shot scanning. Use it for system administration and immediate execution without running background workers.

**How to run:** From the project root with [uv](https://docs.astral.sh/uv/):

```bash
uv run media-search
uv run media-search --help
```

---

## Command tree


| Group / Command | Description                                                                                               |
| --------------- | --------------------------------------------------------------------------------------------------------- |
| `library`       | Add, remove, restore, and list libraries                                                                  |
| `trash`         | Manage soft-deleted libraries (list, empty one, empty all)                                                |
| `asset`         | List assets, show one asset, list video scenes (list, show, scenes)                                        |
| `search`        | Full-text search over asset visual analysis (vibe or OCR)                                                 |
| `scan`          | Run a one-shot scan for a library (no daemon)                                                             |
| `proxy`         | Start the proxy worker (thumbnails and proxies for pending assets)                                        |
| `ai`            | Manage AI models and workers (default model, start AI worker, start video worker, list/add/remove models) |


---

## Supported file formats

The scanner discovers the following file types under library roots. All discovered image types are eligible for the proxy and AI pipeline.

**Video:** `.mp4`, `.mkv`, `.mov`

**Images (raster):** `.jpg`, `.jpeg`, `.png`, `.webp`, `.bmp`, `.tif`, `.tiff`

**Images (camera RAW and DNG):** Canon (`.cr2`, `.cr3`, `.crw`), Nikon (`.nef`, `.nrw`), Sony (`.arw`, `.sr2`, `.srf`), Fujifilm (`.raf`), Olympus (`.orf`), Panasonic/Lumix (`.rw2`, `.raw`), Leica (`.rwl`), and Adobe Digital Negative (`.dng`).

RAW and DNG files are opened for proxy generation using libvips when Pillow cannot read them. Full support for all RAW formats depends on the system libvips being built with libraw.

---

## library

### library add name path

Add a new library. The slug is generated from the name (URL-safe). If the generated slug matches a soft-deleted library, the command fails with an error; restore or permanently delete the old library first, or use a different name.


| Argument | Description                                                          |
| -------- | -------------------------------------------------------------------- |
| `name`   | Display name for the library                                         |
| `path`   | Absolute or relative path to the library root (resolved to absolute) |


**Example:**

```bash
uv run media-search library add "My NAS" /mnt/nas/photos
```

---

### library remove slug

Soft-delete a library: set `deleted_at` so the library and its assets are hidden from normal queries. The library moves to the trash and can be restored or permanently deleted later.


| Argument | Description                 |
| -------- | --------------------------- |
| `slug`   | Library slug to soft-delete |


**Example:**

```bash
uv run media-search library remove nas-main
```

---

### library restore slug

Restore a soft-deleted library by clearing `deleted_at`. The library and its assets become visible again.


| Argument | Description                        |
| -------- | ---------------------------------- |
| `slug`   | Library slug to restore from trash |


**Example:**

```bash
uv run media-search library restore nas-main
```

---

### library list

List libraries in a table: slug, name, path, deleted_at. Paths are truncated for display. By default only non-deleted libraries are shown.


| Option              | Description                                |
| ------------------- | ------------------------------------------ |
| `--include-deleted` | Include soft-deleted libraries in the list |


**Example:**

```bash
uv run media-search library list
uv run media-search library list --include-deleted
```

---

## trash

### trash list

List libraries in the trash. Output is a Rich table with columns: Slug, Name, Deleted At.

**Example:**

```bash
uv run media-search trash list
```

---

### trash empty slug

Permanently delete a single trashed library and all its assets. Uses chunked deletion to avoid long DB locks. Cannot be undone. Prompts for confirmation unless `--force` is used.


| Argument | Description                        |
| -------- | ---------------------------------- |
| `slug`   | Library slug to permanently delete |



| Option    | Description              |
| --------- | ------------------------ |
| `--force` | Skip confirmation prompt |


**Example:**

```bash
uv run media-search trash empty nas-old
uv run media-search trash empty nas-old --force
```

---

### trash empty-all

Permanently delete all trashed libraries and their assets. Cannot be undone. Prompts for confirmation unless `--force` is used.

With `--verbose` / `-v`, prints progress (e.g. `Emptying 1/3: slug`) before each library.


| Option            | Description                         |
| ----------------- | ----------------------------------- |
| `--force`         | Skip confirmation prompt            |
| `--verbose`, `-v` | Print progress (Emptying 1/N: slug) |


**Example:**

```bash
uv run media-search trash empty-all
uv run media-search trash empty-all --force
uv run media-search trash empty-all --force --verbose
```

---

## asset

### asset list library_slug

List discovered assets for a library. Output is a Rich table: ID, Rel Path, Type, Status, Size (KB). A summary line reports how many assets are shown and the total (e.g. "Showing 50 of 213 assets for library 'disneyland'.").


| Argument       | Description                     |
| -------------- | ------------------------------- |
| `library_slug` | Library slug to list assets for |



| Option     | Description                                    |
| ---------- | ---------------------------------------------- |
| `--limit`  | Maximum number of assets to show (default: 50) |
| `--status` | Filter by status (e.g. `pending`, `completed`) |


Valid status values: `pending`, `processing`, `proxied`, `extracting`, `analyzing`, `completed`, `failed`, `poisoned`.

Exits with an error if the library is not found or is soft-deleted.

**Example:**

```bash
uv run media-search asset list nas-main
uv run media-search asset list nas-main --limit 100 --status pending
```

---

### asset show library_slug rel_path

Show one asset by library slug and relative path (as shown in `asset list`). By default prints a minimal summary: id, library_id, rel_path, type, status, and size (KB). With `--metadata`, prints the full asset record as JSON, including `visual_analysis` (description, tags, and extracted text in `ocr_text`).

Exits with code 1 if the library is not found or soft-deleted, or if the asset is not found.


| Argument       | Description                                   |
| -------------- | --------------------------------------------- |
| `library_slug` | Library slug                                  |
| `rel_path`     | Relative path of the asset within the library |



| Option       | Description                                                                 |
| ------------ | --------------------------------------------------------------------------- |
| `--metadata` | Dump full asset record as JSON (including visual_analysis / extracted text) |


**Example:**

```bash
uv run media-search asset show nas-main photos/2024/IMG_001.jpg
uv run media-search asset show nas-main photos/2024/IMG_001.jpg --metadata
```

---

### asset scenes library_slug rel_path

List video scenes for a video asset. Data comes from the `video_scenes` table (written by the Video worker). By default prints a summary table: scene index, start/end time (seconds), keep reason, and description (truncated). With `--metadata`, prints a JSON array of full scene records including the `metadata` JSONB (e.g. moondream description, tags, showinfo).

Exits with code 1 if the library is not found or soft-deleted, the asset is not found, or the asset is not a video. If there are no scenes indexed, prints "No scenes indexed for this asset." and exits 0.


| Argument       | Description                                        |
| -------------- | -------------------------------------------------- |
| `library_slug` | Library slug                                       |
| `rel_path`     | Relative path of the video asset within the library |


| Option       | Description                                                                                  |
| ------------ | -------------------------------------------------------------------------------------------- |
| `--metadata` | Output full scene records as JSON (including per-scene metadata: moondream description/tags). |


**Example:**

```bash
uv run media-search asset scenes nas-main video/clip.mp4
uv run media-search asset scenes nas-main video/clip.mp4 --metadata
```

---

## search

### search [query]

Full-text search over asset `visual_analysis` (AI description, tags, and extracted text). By default the query is applied to the whole JSON (vibe search). With `--ocr`, the query is applied only to the extracted OCR text. Only one search path is used per run: either global or OCR, not both.

When a query is provided, results are ordered by **relevance** (best match first) and limited by `--limit`. Without a query, results are ordered by asset modification time (newest first).

Results are shown in a Rich table: **Library**, **Relative Path**, **Type**, **Status**, **Confidence**. The **Confidence** column shows match strength as a percentage of the top result’s relevance score (100% for the best match). It is color-coded: green for high (>80%), yellow for medium (>50%), red for lower (≤50%). When there is no search query or when scores are not comparable, confidence is shown as "—".

If no assets match, a yellow message is printed.


| Argument | Description                                                                               |
| -------- | ----------------------------------------------------------------------------------------- |
| `query`  | Search string (optional; if omitted, returns assets subject to `--library` and `--limit`) |



| Option      | Description                                                               |
| ----------- | ------------------------------------------------------------------------- |
| `--ocr`     | Search only within extracted OCR text instead of the full visual analysis |
| `--library` | Filter results to this library slug only                                  |
| `--limit`   | Maximum number of results (default 50)                                    |


**Example:**

```bash
uv run media-search search "man in blue shirt"
uv run media-search search "hamburger" --ocr
uv run media-search search "beach" --library nas-main --limit 20
```

---

## scan

### scan slug

Run a one-shot scan for the given library. Does not start the scanner worker daemon; it runs the scanner logic once and exits. Useful for immediate discovery or testing. The library’s scan status is set so a running scanner worker would also pick up work.

Exits with code 1 if the library is not found or is soft-deleted; the message suggests using `library list` to see valid slugs.

With `--verbose` / `-v`, progress is printed every 100 files (e.g. `Scanner: files_processed=100`). Total is shown only at the end.


| Argument | Description               |
| -------- | ------------------------- |
| `slug`   | Library slug to scan once |



| Option            | Description                                       |
| ----------------- | ------------------------------------------------- |
| `--verbose`, `-v` | Enable DEBUG logging and progress every 100 files |


**Example:**

```bash
uv run media-search scan nas-main
uv run media-search scan nas-main --verbose
```

---

## proxy

### proxy

Start the proxy worker. It runs until interrupted (Ctrl+C). The worker claims pending assets, generates thumbnails and proxy images on local storage, and updates their status to proxied (or poisoned on error). Worker ID is auto-generated from hostname and a short UUID unless overridden.

When `--library` is provided, the command exits with code 1 if the library is not found or is soft-deleted (same message as `scan`).

With `--verbose` / `-v`, each proxied asset is printed with a running count (e.g. `Proxied asset 123 (photo.jpg) 5/200`). The path shown is the relative path within the library (rel_path only), so you can copy-paste it into commands like `asset show <library_slug> <rel_path>`. The total is the number of pending **image** (proxyable) assets at start, so videos and other non-proxyable assets are not included in the denominator. When there is no work, the worker logs that it is entering polling mode and at what interval (e.g. every 5s), and logs "Checking for work..." each time it wakes to poll, so you can see it is waiting rather than stuck.

With `--repair`, before the main loop the worker runs a one-time check: it finds assets that are supposed to have proxy and thumbnail files (status proxied, completed, etc.) but are missing them on disk (e.g. after deleting the data directory), sets their status to pending, then runs the normal loop so they are regenerated. Combine with `--library` to repair only one library.


| Option            | Description                                                                                     |
| ----------------- | ----------------------------------------------------------------------------------------------- |
| `--heartbeat`     | Heartbeat interval in seconds (default: 15.0)                                                   |
| `--worker-name`   | Force a specific worker ID; defaults to auto-generated                                          |
| `--library`       | Limit to this library slug only (optional)                                                      |
| `--verbose`, `-v` | Print progress (each asset and N/total)                                                         |
| `--repair`        | Check for missing proxy/thumbnail files and set those assets to pending so they are regenerated |


**Example:**

```bash
uv run media-search proxy
uv run media-search proxy --heartbeat 10
uv run media-search proxy --worker-name my-proxy-1
uv run media-search proxy --library disneyland
uv run media-search proxy --library disneyland --verbose
uv run media-search proxy --library disneyland --repair
```

---

## ai

The `ai` group manages AI/vision models and the AI worker. Models are registered by name and version; the AI worker claims proxied assets, runs vision analysis (e.g. description, tags, OCR), and marks assets completed (or poisoned on error).

**Default model:** A system-wide default AI model can be set with `ai default set`. Each library may override this via its target tagger (library default). The effective default for a library is the library’s target tagger if set, otherwise the system default. When you start the AI worker without `--analyzer`, it uses the effective default for the selected library (or the system default if no library is specified). The worker only claims assets whose effective target model matches the worker’s model. After a fresh install and running migrations, the system default is **moondream2** (version 2025-01-09), seeded by migration. To use a different model as the system default, ensure it is registered (`ai add <name> <version>` if needed), then run `ai default set <name> [version]`; use `ai default show` to confirm.

### ai default set name [version]

Set the system default AI model. The model is resolved by name and optional version; if version is omitted, the latest registered version for that name (by id) is used. Setting `mock` (or `mock-analyzer`) as default is rejected unless `MEDIASEARCH_ALLOW_MOCK_DEFAULT=1` (for tests only).


| Argument  | Description                                      |
| --------- | ------------------------------------------------ |
| `name`    | Model name (e.g. moondream2)                     |
| `version` | Optional; if omitted, latest by id for that name |


**Example:**

```bash
uv run media-search ai default set moondream2
uv run media-search ai default set moondream2 2025-01-09
```

---

### ai default show

Print the current system default AI model (id, name, version), or a message if none is set.

**Example:**

```bash
uv run media-search ai default show
```

---

### ai start

Start the AI worker. It runs until interrupted (Ctrl+C). The worker claims proxied image assets, runs the configured vision analyzer on their local proxy files, saves visual analysis to the asset, and sets status to completed (or poisoned on failure). Worker ID is auto-generated from hostname and a short UUID unless overridden.

When `--library` is provided, the command exits with code 1 if the library is not found or is soft-deleted.

When `--analyzer` is omitted, the worker uses the effective default: if `--library` was given, the library’s target tagger or (if null) the system default; otherwise the system default. If no default is set or the resolved model is `mock`, the command exits with an error (unless `MEDIASEARCH_ALLOW_MOCK_DEFAULT=1` in tests).

With `--repair`, before the main loop the worker runs a one-time repair pass: it finds assets that are in status completed or analyzing but whose library’s effective target model differs from the model that produced their current analysis, sets their status to proxied so they are re-claimed and re-analyzed. Use with `--library` to repair only that library.


| Option            | Description                                                                                |
| ----------------- | ------------------------------------------------------------------------------------------ |
| `--heartbeat`     | Heartbeat interval in seconds (default: 15.0)                                              |
| `--worker-name`   | Force a specific worker ID; defaults to auto-generated                                     |
| `--library`       | Limit to this library slug only (optional)                                                 |
| `--verbose`, `-v` | Print progress for each completed asset                                                    |
| `--analyzer`      | AI model to use (e.g. mock, moondream2). If omitted, uses library or system default        |
| `--repair`        | Set assets that need re-analysis (effective model changed) to proxied before the main loop |


**Analyzers:** `mock` is a placeholder for development and tests. `moondream2` uses the Moondream2 vision model (vikhyatk/moondream2, revision 2025-01-09) for description, tags, and OCR; it requires PyTorch and sufficient GPU/CPU memory. When using `moondream2`, the first image in a run may be slower than subsequent ones if the runtime uses model compilation (e.g. torch.compile).

**Example:**

```bash
uv run media-search ai start
uv run media-search ai start --library nas-main --verbose
uv run media-search ai start --analyzer moondream2
uv run media-search ai start --library nas-main --repair
```

---

### ai video

Start the Video worker. It runs until interrupted (Ctrl+C). The worker claims **pending** video assets (`.mp4`, `.mkv`, `.mov`), runs the scene-indexing pipeline (scene detection, best-frame selection, optional vision analysis on representative frames), renews the asset lease after each closed scene, and supports graceful shutdown (on SIGINT/SIGTERM the pipeline is interrupted and the asset is set back to pending so another worker can resume). Worker ID is auto-generated as `video-<hostname>-<short-uuid>` unless overridden.

When `--library` is provided, the command exits with code 1 if the library is not found or is soft-deleted. Model resolution (effective default, mock rejection) matches `ai start`. The same vision analyzer is used for optional per-scene description/tags (e.g. mock, moondream2).


| Option            | Description                                                                                                 |
| ----------------- | ----------------------------------------------------------------------------------------------------------- |
| `--heartbeat`     | Heartbeat interval in seconds (default: 15.0).                                                              |
| `--worker-name`   | Force a specific worker ID.                                                                                 |
| `--library`       | Limit to this library slug only.                                                                            |
| `--verbose`, `-v` | Print progress for each completed asset.                                                                    |
| `--analyzer`      | AI model to use for scene descriptions (e.g. mock, moondream2). If omitted, uses library or system default. |


**Examples:**

```bash
uv run media-search ai video
uv run media-search ai video --library nas-main --verbose
uv run media-search ai video --analyzer moondream2
```

---

### ai list

List all registered AI models in a Rich table: ID, Name, Version. Models are created when the AI worker starts (from its analyzer’s model card) or via `ai add`.

**Example:**

```bash
uv run media-search ai list
```

---

### ai add name version

Register an AI model by name and version. Useful for pre-registering models or when using a custom analyzer.


| Argument  | Description   |
| --------- | ------------- |
| `name`    | Model name    |
| `version` | Model version |


**Example:**

```bash
uv run media-search ai add moondream 1.0
```

---

### ai remove name

Remove an AI model by name (all versions with that name are removed). Prompts for confirmation unless `--force` is used. **Fails with an error** if any asset references the model (e.g. has been analyzed by it); you must re-process or clear those assets before removing the model.


| Argument | Description          |
| -------- | -------------------- |
| `name`   | Model name to remove |



| Option    | Description              |
| --------- | ------------------------ |
| `--force` | Skip confirmation prompt |


**Example:**

```bash
uv run media-search ai remove mock-analyzer
uv run media-search ai remove mock-analyzer --force
```

---

## End-to-end example

This section walks through a complete workflow using placeholder values. Use a real path and library name for your own run. The slug is derived from the name (e.g. `"Example Library"` → `example-library`).

**1. Create a new library**

```bash
uv run media-search library add "Example Library" /path/to/media
```

Note the slug (e.g. `example-library`) for the next steps.

**2. Scan the library**

Run a one-shot scan to discover files (no scanner daemon).

```bash
uv run media-search scan example-library
```

**3. Show library and asset details**

```bash
uv run media-search library list
uv run media-search asset list example-library --limit 100
```

**4. Create proxies**

Start the proxy worker for this library. It runs until interrupted (Ctrl+C) and processes pending assets. For the example, run it until at least some assets show status `proxied`, then stop.

```bash
uv run media-search proxy --library example-library --verbose
```

**5. Show proxy details**

List assets again to see `proxied` status; show one asset by path.

```bash
uv run media-search asset list example-library
uv run media-search asset show example-library photos/2024/IMG_001.jpg
```

**6. Create image text**

Start the AI worker to run vision analysis on proxied image assets. It runs until interrupted. When `--analyzer` is omitted, the worker uses the system default (moondream2 out of the box). Use `ai default show` to check the current default; for testing you can pass `--analyzer mock` if allowed by your environment.

```bash
uv run media-search ai start --library example-library --verbose
```

Stop after some image assets reach status `completed`.

**7. Show image text details**

View full asset record including `visual_analysis` (description, tags, `ocr_text`).

```bash
uv run media-search asset show example-library photos/2024/IMG_001.jpg --metadata
```

**8. Perform a query that finds 2+ images**

Search uses full-text over asset `visual_analysis`. Use a query that matches at least two completed image assets (e.g. a word from their descriptions or OCR).

```bash
uv run media-search search "person" --library example-library --limit 10
```

Results show Library, Relative Path, Type (`image`), Status, and Confidence.

**9. Process videos under the library**

Run the Video worker to index video assets (scene detection, best-frame selection, optional AI descriptions). Use `ai video`; with `--library example-library` the worker only claims pending videos from that library. The worker runs until interrupted (Ctrl+C). Video scene data is stored in the database (`video_scenes`) by the pipeline; the steps below show how search and asset show behave with respect to videos.

```bash
uv run media-search ai video --library example-library --verbose
```

**10. Show video details (including text if possible)**

Show a video asset. Video assets do not receive `visual_analysis` from the AI worker (only image assets do). Scene-level descriptions and text are stored in the database and can be viewed with **`asset scenes <library> <rel_path>`** (summary table) or **`asset scenes ... --metadata`** (full JSON including per-scene metadata).

```bash
uv run media-search asset show example-library videos/clip.mp4
uv run media-search asset show example-library videos/clip.mp4 --metadata
uv run media-search asset scenes example-library videos/clip.mp4
uv run media-search asset scenes example-library videos/clip.mp4 --metadata
```

**11. Repeat the previous query to show how video results are not incorporated**

Run the same search again. Results are unchanged: search only queries `Asset.visual_analysis`, so video scene or frame data is not included in search results.

```bash
uv run media-search search "person" --library example-library --limit 10
```

**12. Limit search to images only or videos only**

The CLI does not currently support filtering search results by asset type. When supported, the intended usage would be along the lines of `--type image` or `--type video` to restrict results to images only or videos only.

---

## Running tests

Tests are run via `test.sh` from the project root. Tests are categorized as **fast** (no DB, no AI), **slow** (need Postgres testcontainer), or **ai** (need real AI, e.g. moondream). Migration tests are separate and run only with `--all`.


| Invocation         | What runs                                       |
| ------------------ | ----------------------------------------------- |
| `./test.sh`        | Default: fast + slow (no ai, no migration)      |
| `./test.sh --fast` | Fast only (unit tests, mocks, no DB)            |
| `./test.sh --slow` | Slow only (tests that need Postgres)            |
| `./test.sh --ai`   | AI only (tests that load/use moondream)         |
| `./test.sh --all`  | Everything: fast, slow, ai, and migration tests |


Extra arguments are passed to pytest. Examples: `./test.sh --fast tests/test_storage.py`, `./test.sh tests/test_vision_factory.py -k mock`.

---

## Conventions

- **uv:** Use `uv run media-search` (or the installed `media-search` entry point) so the correct environment is used.
- **Destructive commands:** `trash empty` and `trash empty-all` prompt for confirmation unless `--force` is given.
- **Exit codes:** `0` on success; `1` on error (e.g. library not found, invalid status, slug collision on add).

