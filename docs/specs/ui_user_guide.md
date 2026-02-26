# MediaSearch UI User Guide

The MediaSearch web UI provides:

- **Search** — A fast search box with Semantic vs OCR toggle.
- Optional **tag filter**: open `/dashboard?tag=Disneyland` or `/dashboard/tag/Disneyland` to see all assets with that tag (no text query).
- A responsive results grid (thumbnails, animated WebP previews on hover for videos).
- **Detail modal**: Click any result to open a pop-up with the thumbnail on the left and, on the right, **description**, **tags** (as clickable chiclets that navigate to that tag’s results), and **OCR text**.
- **Library Browser** — Select a library and browse all media with infinite scroll, same layouts and detail modal as search.
- A collapsible System Status section showing worker health and stats.

---

## How to run

Start the FastAPI application with uvicorn (from the project root, using uv):

```bash
uv run uvicorn src.api.main:app --reload
```

Default URL: **http://127.0.0.1:8000**

**Requirements:** A running PostgreSQL instance and applied migrations (`alembic upgrade head`). The app reads from the same database as the CLI and workers.

### Building the dashboard CSS

Dashboard styles (Tailwind + DaisyUI) are built from source so that layout utilities (masonry, bento, list, filmstrip) are included. From the project root:

```bash
npm install
npm run build:css
```

This writes `static/css/app.css`. The built file is committed so the app works without Node; re-run `build:css` when you change dashboard markup or add new Tailwind/DaisyUI classes.

---

## Dashboard (GET /dashboard and GET /dashboard/tag/{tag})

The main page is **GET /dashboard**. It returns server-rendered HTML (Jinja2) and uses a small JSON API for search results. You can pass an initial tag filter with **GET /dashboard?tag=...** or **GET /dashboard/tag/{tag}** (e.g. `/dashboard/tag/Disneyland`); the dashboard then runs a tag-only search and shows a “Tag: …” chip that can be cleared.

### Header

- **System Version** — Schema version from `system_metadata` (e.g. `V1`). Shown as “V{schema_version}”.
- **DB Status** — Connection status: **Connected** (green) or **Error** (red), depending on whether a simple DB check succeeds.

### Search

- **Mode toggle** — Semantic (full-text on analysis text) vs OCR (full-text on OCR text).
- **Search input** — Search runs only when you press **Enter** or click the **Search** button (no search-while-typing).
- **Results** — Each result shows library name, filename, and **Match %** (relevance). For videos, a **Jump** badge shows the best match timestamp (MM:SS) and a density bar along the bottom of the card. Images and videos both show a Match percentage (e.g. 100% for images, or scene density for videos). **Click a result** to open the **detail modal**: thumbnail (and video preview) on the left; on the right, **description**, **tags** (click a tag to see all assets with that tag), and **OCR text**. For video results with a best-match timestamp, the modal shows a playable 10-second clip for verification (extracted on demand).

### Layout selector

When results are shown, you can switch how they are displayed:

- **Masonry** — Multi-column flow with variable-height cards (columns adapt to viewport).
- **Bento** — First result is large; remaining results in a dense grid.
- **List** — Single column of horizontal rows (thumbnail left, library · filename and Match % on the right).
- **Filmstrip** — One horizontal scrollable strip per library; each library has a section heading and its results in a strip.

The chosen layout is persisted in browser storage and restored on the next visit.

### System Status

The “System Status” section shows workers from `worker_status`:

- **worker_id** — Unique identifier (e.g. hostname + UUID).
- **state** — `idle`, `processing`, `paused`, or `offline`.
- **files_processed** — Derived from the worker’s `stats` JSONB if present.

If no workers are registered, the table shows: “No workers registered.”

---

## Library Browser (GET /library and GET /library/{slug})

The Library Browser page lets you select a library and browse all media in that library. It uses the same result grid (masonry, bento, list, filmstrip), detail modal, and type filter as the search dashboard.

### Navigation

Use the header links **Search** and **Library** to switch between the search dashboard and library browser.

### Library selector

Select a library from the dropdown. When a library is chosen, assets load automatically. You can bookmark or share a URL like `/library/nas-main` to open a specific library directly.

### Sort options

- **Name** — Alphabetical by path/filename.
- **Date** — Modification time (best proxy for media creation).
- **Size** — File size in bytes.
- **Added** — When indexed into the system.
- **Type** — Group images/videos, then by name.

You can toggle sort order (ascending/descending). Sort and order are persisted in browser storage.

### Infinite scroll

As you scroll near the bottom of the grid, more assets load automatically (about 50 per page). This lets you browse hundreds or thousands of items without manual pagination.

### Layout and detail modal

Same as the dashboard: Masonry, Bento, List, or Filmstrip layout; click any result to open the detail modal with description, tags, and OCR text.

---

## Media URLs (derivatives only)

The UI never writes to source libraries and only loads **derivatives from `data_dir`**:

- **Static mount**: `GET /media/...` serves files rooted at `data_dir`.
- **Thumbnails**: `/media/{library_slug}/thumbnails/{asset_id % 1000}/{asset_id}.jpg`
- **Animated previews** (videos): `/media/{asset.preview_path}` (when `asset.preview_path` is set).
- **Video clips** (search hit verification): `/api/asset/{asset_id}/clip?ts=...` lazy-extracts and redirects to `/media/video_clips/{library_id}/{asset_id}/clip_{ts}.mp4`.

---

## Tech stack

The dashboard is built with:

- **FastAPI** — Web framework and dependency injection.
- **Jinja2** — Server-side HTML templates.
- **Alpine.js** — Lightweight interactivity (search, hover previews, modal).
- **DaisyUI** — UI component classes.
- **Tailwind CSS + DaisyUI** — Built from source (`npm run build:css`); see “Building the dashboard CSS” above.

Templates live under `src/api/templates/` (e.g. `dashboard.html`).

---

## Scope and limitations

- **Search vs Library:** The search dashboard (`/dashboard`) and library browser (`/library`) are separate pages; both share the same result layouts and detail modal.
- **Read-only:** The dashboard only displays data; it does not create, update, or delete libraries or assets. Use the [CLI](cli_user_guide.md) for those operations.
- **Derivatives only:** The UI only loads thumbnails and previews from `data_dir` via `/media/...` (no source library access).
