#!/bin/bash
# ── IOG + Fast Strategy Demo ─────────────────────────────────────────────────
# Opens 4 terminal tabs:
#   Tab 1: Fake Exchange (TCP server on :6001)
#   Tab 2: Internal Order Gateway (listens on :8001, connects to exchange)
#   Tab 3: Fast Strategy (subscribes to multicast, connects to IOG)
#   Tab 4: MDH Sender (sends fake market data via multicast)
#
# Usage: ./run_demo.sh
# Stop:  Ctrl-C in each tab, or close the terminal window

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

osascript <<EOF
tell application "Terminal"
    activate

    -- Tab 1: Fake Exchange
    do script "cd '$PROJECT_DIR' && echo '=== FAKE EXCHANGE ===' && python -c '
import socket
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind((\"127.0.0.1\", 6001))
srv.listen(1)
print(\"[EXCH1] Listening on :6001\")
conn, addr = srv.accept()
print(f\"[EXCH1] IOG connected from {addr}\")
while True:
    data = conn.recv(4096)
    if not data: break
    print(f\"[EXCH1] {data}\")
'"

    delay 1

    -- Tab 2: IOG
    tell application "System Events" to keystroke "t" using command down
    delay 0.5
    do script "cd '$PROJECT_DIR' && echo '=== INTERNAL ORDER GATEWAY ===' && python internal_order_gateway.py" in front window

    delay 2

    -- Tab 3: Fast Strategy
    tell application "System Events" to keystroke "t" using command down
    delay 0.5
    do script "cd '$PROJECT_DIR' && echo '=== FAST STRATEGY ===' && python strategy_fast.py" in front window

    delay 1

    -- Tab 4: MDH Sender
    tell application "System Events" to keystroke "t" using command down
    delay 0.5
    do script "cd '$PROJECT_DIR' && echo '=== MDH MARKET DATA SENDER ===' && python tests/_mdh_sender.py" in front window

end tell
EOF

echo "Demo launched in Terminal. Check the 4 tabs."
