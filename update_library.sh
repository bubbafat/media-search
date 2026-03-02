#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <library_slug>" >&2
  exit 1
fi

LIBRARY_SLUG="$1"

uv sync --extra station

# Start Postgres, set DATABASE_URL, wait for ready, run migrations (reusable)
. ./pg.sh

echo "=== MediaSearch update: starting incremental pipeline for library '${LIBRARY_SLUG}' ==="

run_scan() {
  echo "--- Re-scanning library '${LIBRARY_SLUG}' for new/changed media ---"
  uv run media-search scan "${LIBRARY_SLUG}"
}

has_assets_with_status() {
  local status="$1"
  local output

  if ! output=$(uv run media-search asset list "${LIBRARY_SLUG}" --status "${status}" --limit 1); then
    echo "Error: failed to list assets with status '${status}' for library '${LIBRARY_SLUG}'." >&2
    exit 1
  fi

  if echo "${output}" | grep -q "Showing 0 of 0 assets"; then
    return 1
  fi

  return 0
}

repair_and_drain_proxies() {
  echo "--- Repairing and building image/video proxies for '${LIBRARY_SLUG}' ---"

  # Initial repair pass to mark missing/broken proxies as pending.
  uv run media-search proxy --library "${LIBRARY_SLUG}" --repair --once
  uv run media-search video-proxy --library "${LIBRARY_SLUG}" --repair --once

  # Continue processing any pending assets (new/changed or repaired).
  while has_assets_with_status "pending"; do
    uv run media-search proxy --library "${LIBRARY_SLUG}" --once
    uv run media-search video-proxy --library "${LIBRARY_SLUG}" --once

    if ! has_assets_with_status "pending"; then
      break
    fi
  done
}

repair_and_drain_ai() {
  echo "--- Repairing and running AI analysis for '${LIBRARY_SLUG}' ---"

  # Initial repair pass for image AI (e.g. target model changes).
  uv run media-search ai start --library "${LIBRARY_SLUG}" --repair --once

  # Pass 1: drain proxied -> analyzed_light (light mode: fast tags/desc, no OCR).
  while has_assets_with_status "proxied"; do
    uv run media-search ai start --library "${LIBRARY_SLUG}" --mode light --once
    uv run media-search ai video --library "${LIBRARY_SLUG}" --mode light --once

    if ! has_assets_with_status "proxied"; then
      break
    fi
  done

  # Pass 2: drain analyzed_light -> completed (full mode: OCR merge).
  while has_assets_with_status "analyzed_light"; do
    uv run media-search ai start --library "${LIBRARY_SLUG}" --mode full --once
    uv run media-search ai video --library "${LIBRARY_SLUG}" --mode full --once

    if ! has_assets_with_status "analyzed_light"; then
      break
    fi
  done
}

drain_search_sync() {
  echo "--- Syncing search index for '${LIBRARY_SLUG}' ---"
  local output exitcode
  while true; do
    set +e
    output=$(uv run media-search search-sync --library "${LIBRARY_SLUG}" --once 2>&1)
    exitcode=$?
    set -e
    echo "$output"
    if [ $exitcode -ne 0 ]; then
      exit $exitcode
    fi
    if echo "$output" | grep -q "No more assets"; then
      break
    fi
    if ! echo "$output" | grep -q "Batch complete"; then
      break
    fi
  done
}

run_scan
repair_and_drain_proxies
repair_and_drain_ai
drain_search_sync

echo "=== MediaSearch update: completed incremental pipeline for library '${LIBRARY_SLUG}' ==="

