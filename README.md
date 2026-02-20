# MediaSearch

Local-first semantic search for media files (JPG, ARW, MP4/MOV). Uses SQLite + sqlite-vec and Apple MLX.

## Testing

### Automated tests (pytest)

Install dev dependencies and run the test suite:

```bash
uv sync --extra dev
uv run pytest
```

- **FileCrawler** and helpers are always tested (no special SQLite required).
- **MediaDatabase** tests are skipped if your Python’s SQLite doesn’t support `load_extension` (common on macOS unless you use e.g. Homebrew Python).

### Manual CLI test

Use a small directory so rebuild/update run quickly:

```bash
# Create a test directory with a few “media” files
mkdir -p /tmp/ms-test
touch /tmp/ms-test/photo.jpg /tmp/ms-test/vacation.MOV

# Use a separate DB so you don’t touch mediasearch.db
uv run python mediasearch.py --db /tmp/ms-test/mediasearch.db rebuild --path /tmp/ms-test

# Incremental update
uv run python mediasearch.py --db /tmp/ms-test/mediasearch.db update --path /tmp/ms-test

# Query (placeholder embedding until MLX is wired)
uv run python mediasearch.py --db /tmp/ms-test/mediasearch.db query "beach sunset"
```

**Note:** `rebuild` and `update` need a Python build where SQLite has `enable_load_extension` (e.g. `brew install python` and recreate the venv with that interpreter). Otherwise you’ll see a clear error from the code.
