# MediaSearch CLI User Guide

The MediaSearch admin CLI is a Typer-based tool for library management, trash handling, asset listing, and one-shot scanning. Use it for system administration and immediate execution without running background workers.

**How to run:** From the project root with [uv](https://docs.astral.sh/uv/):

```bash
uv run media-search
uv run media-search --help
```

---

## Command tree

| Group / Command | Description |
|-----------------|-------------|
| `library`       | Add, remove, restore, and list libraries |
| `trash`         | Manage soft-deleted libraries (list, empty one, empty all) |
| `asset`         | List discovered assets for a library |
| `scan`          | Run a one-shot scan for a library (no daemon) |
| `proxy`         | Start the proxy worker (thumbnails and proxies for pending assets) |
| `ai`            | Manage AI models and workers (start worker, list/add/remove models) |

---

## library

### library add \<name\> \<path\>

Add a new library. The slug is generated from the name (URL-safe). If the generated slug matches a soft-deleted library, the command fails with an error; restore or permanently delete the old library first, or use a different name.

| Argument | Description |
|----------|-------------|
| `name`   | Display name for the library |
| `path`   | Absolute or relative path to the library root (resolved to absolute) |

**Example:**

```bash
uv run media-search library add "My NAS" /mnt/nas/photos
```

---

### library remove \<slug\>

Soft-delete a library: set `deleted_at` so the library and its assets are hidden from normal queries. The library moves to the trash and can be restored or permanently deleted later.

| Argument | Description |
|----------|-------------|
| `slug`   | Library slug to soft-delete |

**Example:**

```bash
uv run media-search library remove nas-main
```

---

### library restore \<slug\>

Restore a soft-deleted library by clearing `deleted_at`. The library and its assets become visible again.

| Argument | Description |
|----------|-------------|
| `slug`   | Library slug to restore from trash |

**Example:**

```bash
uv run media-search library restore nas-main
```

---

### library list

List libraries in a table: slug, name, path, deleted_at. Paths are truncated for display. By default only non-deleted libraries are shown.

| Option | Description |
|--------|-------------|
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

### trash empty \<slug\>

Permanently delete a single trashed library and all its assets. Uses chunked deletion to avoid long DB locks. Cannot be undone. Prompts for confirmation unless `--force` is used.

| Argument | Description |
|----------|-------------|
| `slug`   | Library slug to permanently delete |

| Option | Description |
|--------|-------------|
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

| Option | Description |
|--------|-------------|
| `--force` | Skip confirmation prompt |
| `--verbose`, `-v` | Print progress (Emptying 1/N: slug) |

**Example:**

```bash
uv run media-search trash empty-all
uv run media-search trash empty-all --force
uv run media-search trash empty-all --force --verbose
```

---

## asset

### asset list \<library_slug\>

List discovered assets for a library. Output is a Rich table: ID, Rel Path, Type, Status, Size (KB). A summary line reports how many assets are shown and the total (e.g. "Showing 50 of 213 assets for library 'disneyland'.").

| Argument | Description |
|----------|-------------|
| `library_slug` | Library slug to list assets for |

| Option | Description |
|--------|-------------|
| `--limit` | Maximum number of assets to show (default: 50) |
| `--status` | Filter by status (e.g. `pending`, `completed`) |

Valid status values: `pending`, `processing`, `proxied`, `extracting`, `analyzing`, `completed`, `failed`, `poisoned`.

Exits with an error if the library is not found or is soft-deleted.

**Example:**

```bash
uv run media-search asset list nas-main
uv run media-search asset list nas-main --limit 100 --status pending
```

---

### asset show \<library_slug\> \<rel_path\>

Show one asset by library slug and relative path (as shown in `asset list`). By default prints a minimal summary: id, library_id, rel_path, type, status, and size (KB). With `--metadata`, prints the full asset record as JSON, including `visual_analysis` (description, tags, and extracted text in `ocr_text`).

Exits with code 1 if the library is not found or soft-deleted, or if the asset is not found.

| Argument | Description |
|----------|-------------|
| `library_slug` | Library slug |
| `rel_path` | Relative path of the asset within the library |

| Option | Description |
|--------|-------------|
| `--metadata` | Dump full asset record as JSON (including visual_analysis / extracted text) |

**Example:**

```bash
uv run media-search asset show nas-main photos/2024/IMG_001.jpg
uv run media-search asset show nas-main photos/2024/IMG_001.jpg --metadata
```

---

## scan

### scan \<slug\>

Run a one-shot scan for the given library. Does not start the scanner worker daemon; it runs the scanner logic once and exits. Useful for immediate discovery or testing. The library’s scan status is set so a running scanner worker would also pick up work.

Exits with code 1 if the library is not found or is soft-deleted; the message suggests using `library list` to see valid slugs.

With `--verbose` / `-v`, progress is printed every 100 files (e.g. `Scanner: files_processed=100`). Total is shown only at the end.

| Argument | Description |
|----------|-------------|
| `slug`   | Library slug to scan once |

| Option | Description |
|--------|-------------|
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

With `--verbose` / `-v`, each proxied asset is printed with a running count (e.g. `Proxied asset 123 (disneyland/photo.jpg) 5/200`). Total is the pending count at start.

With `--repair`, before the main loop the worker runs a one-time check: it finds assets that are supposed to have proxy and thumbnail files (status proxied, completed, etc.) but are missing them on disk (e.g. after deleting the data directory), sets their status to pending, then runs the normal loop so they are regenerated. Combine with `--library` to repair only one library.

| Option | Description |
|--------|-------------|
| `--heartbeat` | Heartbeat interval in seconds (default: 15.0) |
| `--worker-name` | Force a specific worker ID; defaults to auto-generated |
| `--library` | Limit to this library slug only (optional) |
| `--verbose`, `-v` | Print progress (each asset and N/total) |
| `--repair` | Check for missing proxy/thumbnail files and set those assets to pending so they are regenerated |

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

### ai start

Start the AI worker. It runs until interrupted (Ctrl+C). The worker claims proxied image assets, runs the configured vision analyzer on their local proxy files, saves visual analysis to the asset, and sets status to completed (or poisoned on failure). Worker ID is auto-generated from hostname and a short UUID unless overridden.

When `--library` is provided, the command exits with code 1 if the library is not found or is soft-deleted.

| Option | Description |
|--------|-------------|
| `--heartbeat` | Heartbeat interval in seconds (default: 15.0) |
| `--worker-name` | Force a specific worker ID; defaults to auto-generated |
| `--library` | Limit to this library slug only (optional) |
| `--verbose`, `-v` | Print progress for each completed asset |
| `--analyzer` | Which AI model to use: `mock` (default) or `moondream2` |

**Analyzers:** `mock` is a placeholder for development and tests. `moondream2` uses the Moondream2 vision model (vikhyatk/moondream2, revision 2025-01-09) for description, tags, and OCR; it requires PyTorch and sufficient GPU/CPU memory.

**Example:**

```bash
uv run media-search ai start
uv run media-search ai start --library nas-main --verbose
uv run media-search ai start --analyzer moondream2
```

---

### ai list

List all registered AI models in a Rich table: ID, Name, Version. Models are created when the AI worker starts (from its analyzer’s model card) or via `ai add`.

**Example:**

```bash
uv run media-search ai list
```

---

### ai add \<name\> \<version\>

Register an AI model by name and version. Useful for pre-registering models or when using a custom analyzer.

| Argument | Description |
|----------|-------------|
| `name`   | Model name |
| `version` | Model version |

**Example:**

```bash
uv run media-search ai add moondream 1.0
```

---

### ai remove \<name\>

Remove an AI model by name (all versions with that name are removed). Prompts for confirmation unless `--force` is used. **Fails with an error** if any asset references the model (e.g. has been analyzed by it); you must re-process or clear those assets before removing the model.

| Argument | Description |
|----------|-------------|
| `name`   | Model name to remove |

| Option | Description |
|--------|-------------|
| `--force` | Skip confirmation prompt |

**Example:**

```bash
uv run media-search ai remove mock-analyzer
uv run media-search ai remove mock-analyzer --force
```

---

## Conventions

- **uv:** Use `uv run media-search` (or the installed `media-search` entry point) so the correct environment is used.
- **Destructive commands:** `trash empty` and `trash empty-all` prompt for confirmation unless `--force` is given.
- **Exit codes:** `0` on success; `1` on error (e.g. library not found, invalid status, slug collision on add).
