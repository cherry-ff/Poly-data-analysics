#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_ROOT="${PROJECT_ROOT}-run"
UPLOAD_ENV="$PROJECT_ROOT/.env"
VENV_PYTHON="$PROJECT_ROOT/.venv/bin/python"
DECRYPT_PASSWORD="${POLY15_DECRYPT_PASSWORD:-captain}"

if [ ! -f "$UPLOAD_ENV" ]; then
  echo "missing env file: $UPLOAD_ENV" >&2
  exit 1
fi

if [ ! -x "$VENV_PYTHON" ]; then
  echo "virtualenv not found: $VENV_PYTHON" >&2
  exit 1
fi

mkdir -p \
  "$RUNTIME_ROOT" \
  "$PROJECT_ROOT/logs" \
  "$PROJECT_ROOT/runtime_data/records" \
  "$PROJECT_ROOT/runtime_data/snapshots"

# Keep an empty .env in the runtime cwd so dotenv does not climb into parent dirs.
: > "$RUNTIME_ROOT/.env"

set -a
source "$UPLOAD_ENV"
set +a

# The uploaded env contains an encrypted private key that does not decrypt with
# the provided password. Run in public-feed collection mode until valid creds
# are supplied.
export POLY15_PM_ENCRYPTED_PRIVATE_KEY=
export POLY15_PM_USER_ENABLED=0
export POLY15_PROXY_ENABLED=0
export POLY15_PRIMARY_SYMBOL="${POLY15_PRIMARY_SYMBOL:-BTC-USDT}"
export POLY15_BINANCE_SYMBOL="${POLY15_BINANCE_SYMBOL:-BTC-USDT}"
export POLY15_BINANCE_REST_BASE_URL="${POLY15_BINANCE_REST_BASE_URL:-https://www.okx.com}"
export POLY15_BINANCE_WS_URL="${POLY15_BINANCE_WS_URL:-wss://ws.okx.com:8443/ws/v5/public}"
export POLY15_BINANCE_DEPTH_WS_URL="${POLY15_BINANCE_DEPTH_WS_URL:-wss://ws.okx.com:8443/ws/v5/public}"
export POLY15_RECORDER_OUTPUT_DIR="$PROJECT_ROOT/runtime_data/records"
export POLY15_DB_PATH="$PROJECT_ROOT/runtime_data/events.sqlite3"
export POLY15_SNAPSHOT_OUTPUT_DIR="$PROJECT_ROOT/runtime_data/snapshots"
export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"

cd "$RUNTIME_ROOT"
exec "$VENV_PYTHON" "$PROJECT_ROOT/app/main.py" "$DECRYPT_PASSWORD"
