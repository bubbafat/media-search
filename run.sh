#!/usr/bin/env bash
# Run the ingestion pipeline for a library: scan, then image + video proxy in parallel, then AI.
# Usage: ./run.sh <library_slug>
# Example: ./run.sh nas-main
# Requires: uv sync (with extras) already run; DATABASE_URL and config set.

set -e
if [ -z "$1" ]; then
  echo "Usage: $0 <library_slug>"
  echo "Example: $0 nas-main"
  exit 1
fi

LIBRARY="$1"

echo "Scanning library: $LIBRARY"
uv run media-search scan "$LIBRARY"

echo "Running image and video proxy workers (once each)..."
uv run media-search proxy --once --library "$LIBRARY" &
uv run media-search video-proxy --once --library "$LIBRARY" &
wait

echo "Running AI worker (once)..."
uv run media-search ai start --once --library "$LIBRARY"

echo "Pipeline complete for $LIBRARY"
