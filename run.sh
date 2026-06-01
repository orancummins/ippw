#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
PORT="${PORT:-2012}"

# Parse flags
SERVER_FLAG=""
for arg in "$@"; do
  case "$arg" in
    --server) SERVER_FLAG="--server" ;;
  esac
done

if [[ ! -d ".venv" ]]; then
  echo "[run] Creating virtual environment (.venv)"
  python3 -m venv .venv
fi

source .venv/bin/activate

if [[ "${SKIP_PIP_INSTALL:-0}" != "1" ]]; then
  echo "[run] Installing/updating dependencies"
  python -m pip install --upgrade pip
  python -m pip install -r requirements.txt
fi

existing_pids="$(lsof -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null || true)"
if [[ -n "$existing_pids" ]]; then
  echo "[run] Stopping existing process(es) on port $PORT: $existing_pids"
  kill $existing_pids 2>/dev/null || true

  # Wait briefly for graceful shutdown, then force if still listening.
  for _ in {1..10}; do
    sleep 0.2
    if ! lsof -tiTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
      break
    fi
  done

  remaining_pids="$(lsof -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "$remaining_pids" ]]; then
    echo "[run] Force-stopping stubborn process(es): $remaining_pids"
    kill -9 $remaining_pids 2>/dev/null || true
  fi
fi

echo "[run] Starting app on http://localhost:$PORT"
exec python app.py $SERVER_FLAG
