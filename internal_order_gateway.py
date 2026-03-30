"""
internal_order_gateway.py

Internal Order Gateway (IOG).
  - Receives FIX orders from both strategies over TCP
  - Validates orders
  - Converts orders from internal representation into FIX format
  - Routes and sends orders to the correct exchange over TCP (FIX)
"""

import socket
import config

def main():
    # ── TCP socket: receive FIX orders from strategies ────────────────────────
    inbound_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    inbound_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    inbound_sock.bind((config.IOG_HOST, config.IOG_PORT))
    inbound_sock.listen(5)
    print(f"[IOG] Listening for orders from strategies on {config.IOG_HOST}:{config.IOG_PORT}")

    # ── TCP socket: send FIX orders to exchange 1 ────────────────────────────
    exchange1_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    exchange1_sock.connect((config.EXCHANGE1_FIX_HOST, config.EXCHANGE1_FIX_PORT))
    print(f"[IOG] Connected to exchange 1 FIX gateway at {config.EXCHANGE1_FIX_HOST}:{config.EXCHANGE1_FIX_PORT}")

    # ── TCP socket: send FIX orders to exchange 2 ────────────────────────────
    exchange2_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    exchange2_sock.connect((config.EXCHANGE2_FIX_HOST, config.EXCHANGE2_FIX_PORT))
    print(f"[IOG] Connected to exchange 2 FIX gateway at {config.EXCHANGE2_FIX_HOST}:{config.EXCHANGE2_FIX_PORT}")

    print("[IOG] Running.")
    while True:
        pass

if __name__ == "__main__":
    main()
