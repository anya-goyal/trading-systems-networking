"""
exchange2.py

Simulated exchange 2.
  - Publishes a market data feed over UDP
  - Listens for incoming FIX orders over TCP
"""

import socket
import config

def main():
    # ── UDP socket: publish market feed ──────────────────────────────────────
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(f"[Exchange 2] UDP feed socket open → {config.EXCHANGE2_UDP_HOST}:{config.EXCHANGE2_UDP_PORT}")

    # ── TCP socket: receive FIX orders ────────────────────────────────────────
    fix_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fix_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    fix_sock.bind((config.EXCHANGE2_FIX_HOST, config.EXCHANGE2_FIX_PORT))
    fix_sock.listen(5)
    print(f"[Exchange 2] FIX order socket listening on {config.EXCHANGE2_FIX_HOST}:{config.EXCHANGE2_FIX_PORT}")

    print("[Exchange 2] Running.")
    while True:
        pass

if __name__ == "__main__":
    main()
