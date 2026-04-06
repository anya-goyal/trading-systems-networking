#!/usr/bin/env bash
# Start Exchange 1, the HTTP orderbook UI, and run the debug market maker once.
#
# Usage (from Git Bash / WSL / Linux; repo root is auto-detected):
#   ./scripts/start_exchange1_mm_frontend.sh
#   ./scripts/start_exchange1_mm_frontend.sh --symbol MSFT --theo 300 --spread 2 --qty 50
#
# Environment:
#   PYTHON           Python executable (default: python)
#   FRONTEND_PORT    Passed as --port to exchange1_frontend_server.py (default: 8765)
#   EXCHANGE_SLEEP   Seconds to wait after starting the exchange (default: 1)
#
# The market maker loops every 2s across all tickers (change with --interval / --symbol); Ctrl+C stops it.
# Exchange and frontend keep running until you press Ctrl+C.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON="${PYTHON:-python}"
EXCHANGE_SLEEP="${EXCHANGE_SLEEP:-1}"
FRONTEND_PORT="${FRONTEND_PORT:-8765}"

FE_ARGS=(--port "$FRONTEND_PORT")
if [[ -n "${FRONTEND_BIND:-}" ]]; then
  FE_ARGS=(--host "$FRONTEND_BIND" "${FE_ARGS[@]}")
fi

EX1_PID=""
FE_PID=""s

cleanup() {
  if [[ -n "${FE_PID:-}" ]] && kill -0 "$FE_PID" 2>/dev/null; then
    kill "$FE_PID" 2>/dev/null || true
    wait "$FE_PID" 2>/dev/null || true
  fi
  if [[ -n "${EX1_PID:-}" ]] && kill -0 "$EX1_PID" 2>/dev/null; then
    kill "$EX1_PID" 2>/dev/null || true
    wait "$EX1_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "[run] starting exchange1.py (background)..."
"$PYTHON" exchange1.py &
EX1_PID=$!

echo "[run] waiting ${EXCHANGE_SLEEP}s for listeners..."
sleep "$EXCHANGE_SLEEP"

echo "[run] starting exchange1_frontend_server.py (background)..."
"$PYTHON" exchange1_frontend_server.py "${FE_ARGS[@]}" &
FE_PID=$!

HOST_DISP="${FRONTEND_BIND:-127.0.0.1}"
echo "[run] market maker: exchange1_mm/exchange1_fix_client.py $*"
"$PYTHON" exchange1_mm/exchange1_fix_client.py "$@"

echo "[run] UI: http://${HOST_DISP}:${FRONTEND_PORT}/"
echo "[run] exchange pid=${EX1_PID} frontend pid=${FE_PID} — Ctrl+C to stop both."
wait
