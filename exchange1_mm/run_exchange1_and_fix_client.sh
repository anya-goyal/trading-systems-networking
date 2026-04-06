#!/usr/bin/env bash
# Start Exchange 1, then run the debug FIX client against it.
# Usage (from repo root or any cwd — script cd's to repo root):
#   ./scripts/run_exchange1_and_fix_client.sh
#   ./scripts/run_exchange1_and_fix_client.sh --symbol MSFT --side sell --qty 50 --price 300
#
# Requires: bash, Python on PATH (override with PYTHON=/path/to/python).
# On Windows, run from Git Bash or WSL.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON="${PYTHON:-python}"

cleanup() {
  if [[ -n "${EX1_PID:-}" ]] && kill -0 "$EX1_PID" 2>/dev/null; then
    kill "$EX1_PID" 2>/dev/null || true
    wait "$EX1_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "[run] starting exchange1.py (background)..."
"$PYTHON" exchange1.py &
EX1_PID=$!

# Give the FIX listener time to bind (adjust if your machine is slow).
sleep 1

echo "[run] sending NewOrderSingle via scripts.exchange1_fix_client..."
"$PYTHON" -m scripts.exchange1_fix_client "$@"

echo "[run] fix client finished; stopping exchange1."
