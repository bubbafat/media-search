#!/usr/bin/env bash
# Run tests by category: --fast (no DB), --slow (DB), --ai (real AI), --all (everything).
# Default: fast + slow (no ai, no migration). Extra args are passed to pytest (e.g. test.sh --fast tests/test_storage.py).

set -e

uv sync --all-extras

PYTEST_M=""
PASSTHROUGH=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)
      PYTEST_M='fast or slow or ai or migration'
      shift
      ;;
    --fast)
      PYTEST_M='fast'
      shift
      ;;
    --slow)
      PYTEST_M='slow'
      shift
      ;;
    --ai)
      PYTEST_M='ai'
      shift
      ;;
    *)
      PASSTHROUGH+=("$1")
      shift
      ;;
  esac
done

# Default: fast + slow, exclude ai and migration
if [[ -z "$PYTEST_M" ]]; then
  PYTEST_M='not migration and (fast or slow)'
fi

exec uv run --env-file .env pytest tests/ -v -m "$PYTEST_M" "${PASSTHROUGH[@]}"
