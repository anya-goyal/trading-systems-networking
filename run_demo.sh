#!/bin/bash
# ── IOG + Fast Strategy Demo ─────────────────────────────────────────────────
# Opens 4 terminal windows:
#   1: Fake Exchange (TCP server on :6001)
#   2: Internal Order Gateway (listens on :8001, connects to exchange)
#   3: Fast Strategy (subscribes to multicast, connects to IOG)
#   4: MDH Sender (sends fake market data via multicast)
#
# Usage: ./run_demo.sh
# Stop:  Ctrl-C in each window

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

osascript <<EOF
tell application "Terminal"
    activate

    -- Window 1: Fake Exchange
    do script "cd '$PROJECT_DIR' && echo '=== FAKE EXCHANGE ===' && python tests/_fake_exchange.py"
    delay 1

    -- Window 2: IOG
    do script "cd '$PROJECT_DIR' && echo '=== INTERNAL ORDER GATEWAY ===' && python internal_order_gateway.py"
    delay 2

    -- Window 3: Fast Strategy
    do script "cd '$PROJECT_DIR' && echo '=== FAST STRATEGY ===' && python strategy_fast.py"
    delay 1

    -- Window 4: MDH Sender
    do script "cd '$PROJECT_DIR' && echo '=== MDH MARKET DATA SENDER ===' && python tests/_mdh_sender.py"
end tell
EOF

echo "Demo launched — check the 4 Terminal windows."
