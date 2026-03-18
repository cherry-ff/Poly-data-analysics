#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_ROOT="${PROJECT_ROOT}-run"
UPLOAD_ENV="$PROJECT_ROOT/.env"
VENV_PYTHON="$PROJECT_ROOT/.venv/bin/python"
DASHBOARD_PORT="${POLY15_DASHBOARD_PORT:-18080}"
SYNC_DELETE_GRACE_SECONDS="${POLY15_SYNC_DELETE_GRACE_SECONDS:-21600}"
SYNC_GC_INTERVAL_SECONDS="${POLY15_SYNC_GC_INTERVAL_SECONDS:-300}"

if [ ! -x "$VENV_PYTHON" ]; then
  echo "virtualenv not found: $VENV_PYTHON" >&2
  exit 1
fi

mkdir -p \
  "$RUNTIME_ROOT" \
  "$PROJECT_ROOT/logs" \
  "$PROJECT_ROOT/dashboard/data" \
  "$PROJECT_ROOT/runtime_data/records" \
  "$PROJECT_ROOT/runtime_data/sync_state"

: > "$RUNTIME_ROOT/.env"

if [ -f "$UPLOAD_ENV" ]; then
  set -a
  source "$UPLOAD_ENV"
  set +a
fi

cd "$RUNTIME_ROOT"
exec "$VENV_PYTHON" \
  "$PROJECT_ROOT/dashboard/server.py" \
  --host 127.0.0.1 \
  --port "$DASHBOARD_PORT" \
  --records-root "$PROJECT_ROOT/runtime_data/records" \
  --sync-token-file "$PROJECT_ROOT/runtime_data/sync_api_token.txt" \
  --sync-state-dir "$PROJECT_ROOT/runtime_data/sync_state" \
  --sync-delete-grace-seconds "$SYNC_DELETE_GRACE_SECONDS" \
  --sync-gc-interval-seconds "$SYNC_GC_INTERVAL_SECONDS"
