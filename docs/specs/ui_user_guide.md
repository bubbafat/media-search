# MediaSearch UI User Guide

The MediaSearch web UI is **Mission Control**: a single-page dashboard for system status. It shows schema version, database connectivity, the worker fleet, and aggregate library stats. There is no asset browser, library CRUD, or search UI yet—only this dashboard.

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

The only route is **GET /dashboard**. It returns server-rendered HTML (Jinja2); no separate JSON API is used for the dashboard data.

### Header

- **System Version** — Schema version from `system_metadata` (e.g. `V1`). Shown as “V{schema_version}”.
- **DB Status** — Connection status: **Connected** (green) or **Error** (red), depending on whether a simple DB check succeeds.

### Worker Fleet

A grid of cards, one per registered worker in `worker_status`:

- **Worker ID** — Unique identifier (e.g. hostname + UUID).
- **State** — Current worker state: `idle`, `processing`, `paused`, or `offline`.
- **Version** — Schema version (same for all workers in the current implementation).

If no workers are registered, the section shows: “No workers registered.”

### Library Stats

Two aggregate counts from the `asset` table:

- **Total Assets** — Total number of assets across all libraries.
- **Pending Assets** — Number of assets with status `pending`.

---

## Tech stack

The dashboard is built with:

- **FastAPI** — Web framework and dependency injection.
- **Jinja2** — Server-side HTML templates.
- **HTMX** — For optional dynamic behavior (included in the template).
- **DaisyUI** — UI component classes.
- **Tailwind CSS** — Via DaisyUI’s base.

Templates live under `src/api/templates/` (e.g. `dashboard.html`).

---

## Scope and limitations

- **Single page:** Only the dashboard exists. There are no other pages or routes for the web UI.
- **Read-only:** The dashboard only displays data; it does not create, update, or delete libraries or assets. Use the [CLI](cli_user_guide.md) for those operations.
- **No asset browser or search:** Browsing assets, searching, or viewing thumbnails/frames is not implemented in the UI.
