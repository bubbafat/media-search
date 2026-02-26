# MediaSearch UI User Guide

The MediaSearch web UI is a **Search-First dashboard** (Mission Control + Search). It provides:

- A fast search box with a Semantic vs OCR toggle.
- A responsive results grid (thumbnails, animated WebP previews on hover for videos).
- A collapsible System Status section showing worker health and stats.

---

## How to run

Start the FastAPI application with uvicorn (from the project root, using uv):

```bash
uv run uvicorn src.api.main:app --reload
```

Default URL: **http://127.0.0.1:8000**

**Requirements:** A running PostgreSQL instance and applied migrations (`alembic upgrade head`). The app reads from the same database as the CLI and workers.

---

## Dashboard (GET /dashboard)

The main page is **GET /dashboard**. It returns server-rendered HTML (Jinja2) and uses a small JSON API for search results.

### Header

- **System Version** — Schema version from `system_metadata` (e.g. `V1`). Shown as “V{schema_version}”.
- **DB Status** — Connection status: **Connected** (green) or **Error** (red), depending on whether a simple DB check succeeds.

### Search

- **Mode toggle** — Semantic (full-text on analysis text) vs OCR (full-text on OCR text).
- **Search input** — Enter triggers search; typing also triggers debounced search.
- **Results grid** — Responsive grid (`grid-cols-2 md:grid-cols-4 lg:grid-cols-6`):
  - **Images** show static thumbnails.
  - **Videos** show static thumbnails by default and swap to animated WebP previews on hover (when available).
  - **Match density** is shown as a subtle bar along the bottom of video cards.
  - **Jump badge** shows the best match timestamp (MM:SS) when available.

### System Status

The “System Status” section shows workers from `worker_status`:

- **worker_id** — Unique identifier (e.g. hostname + UUID).
- **state** — `idle`, `processing`, `paused`, or `offline`.
- **files_processed** — Derived from the worker’s `stats` JSONB if present.

If no workers are registered, the table shows: “No workers registered.”

---

## Media URLs (derivatives only)

The UI never writes to source libraries and only loads **derivatives from `data_dir`**:

- **Static mount**: `GET /media/...` serves files rooted at `data_dir`.
- **Thumbnails**: `/media/{library_slug}/thumbnails/{asset_id % 1000}/{asset_id}.jpg`
- **Animated previews** (videos): `/media/{asset.preview_path}` (when `asset.preview_path` is set).

---

## Tech stack

The dashboard is built with:

- **FastAPI** — Web framework and dependency injection.
- **Jinja2** — Server-side HTML templates.
- **Alpine.js** — Lightweight interactivity (search, hover previews, modal).
- **DaisyUI** — UI component classes.
- **Tailwind CSS** — Via DaisyUI’s base.

Templates live under `src/api/templates/` (e.g. `dashboard.html`).

---

## Scope and limitations

- **Single page:** Only the dashboard exists. There are no other pages or routes for the web UI.
- **Read-only:** The dashboard only displays data; it does not create, update, or delete libraries or assets. Use the [CLI](cli_user_guide.md) for those operations.
- **Derivatives only:** The UI only loads thumbnails and previews from `data_dir` via `/media/...` (no source library access).
