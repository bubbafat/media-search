#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <library_slug>" >&2
  exit 1
fi

LIBRARY_SLUG="$1"

echo "=== MediaSearch build: starting full pipeline for library '${LIBRARY_SLUG}' ==="

uv sync --extra station

# Start Postgres, set DATABASE_URL, wait for ready, run migrations (reusable)
. ./pg.sh

run_scan() {
  echo "--- Scanning library '${LIBRARY_SLUG}' ---"
  uv run media-search scan "${LIBRARY_SLUG}" --verbose
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

drain_proxies() {
  echo "--- Building image and video proxies for '${LIBRARY_SLUG}' ---"

  # Repair pass: mark assets with missing/broken proxies as pending.
  uv run media-search proxy --library "${LIBRARY_SLUG}" --repair --once --verbose
  uv run media-search video-proxy --library "${LIBRARY_SLUG}" --repair --once --verbose

  while has_assets_with_status "pending"; do
    uv run media-search proxy --library "${LIBRARY_SLUG}" --once --verbose
    uv run media-search video-proxy --library "${LIBRARY_SLUG}" --once --verbose

    if ! has_assets_with_status "pending"; then
      break
    fi
  done
}

drain_ai() {
  echo "--- Running AI analysis for '${LIBRARY_SLUG}' ---"

  # Pass 1: drain proxied -> analyzed_light (light mode: fast tags/desc, no OCR).
  while has_assets_with_status "proxied"; do
    uv run media-search ai start --library "${LIBRARY_SLUG}" --mode light --once --verbose
    uv run media-search ai video --library "${LIBRARY_SLUG}" --mode light --once --verbose

    if ! has_assets_with_status "proxied"; then
      break
    fi
  done

  # Pass 2: drain analyzed_light -> completed (full mode: OCR merge).
  while has_assets_with_status "analyzed_light"; do
    uv run media-search ai start --library "${LIBRARY_SLUG}" --mode full --once --verbose
    uv run media-search ai video --library "${LIBRARY_SLUG}" --mode full --once --verbose

    if ! has_assets_with_status "analyzed_light"; then
      break
    fi
  done
}

run_scan
drain_proxies
drain_ai

echo "=== MediaSearch build: completed pipeline for library '${LIBRARY_SLUG}' ==="

