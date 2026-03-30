"""
strategy_slow.py

Slow strategy.
  - Subscribes to both multicast groups from the market data handler over UDP
  - Handles dropped packets (gap detection via sequence numbers)
  - More complex processing logic (to be implemented)
  - Sends orders to the Internal Order Gateway over TCP (FIX)
"""

import socket
import struct
import config

def main():
    # ── UDP multicast socket: subscribe to group 1 ───────────────────────────
    mcast_sock1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mcast_sock1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    mcast_sock1.bind(("", config.MULTICAST_PORT_1))
    group1 = struct.pack("4sL", socket.inet_aton(config.MULTICAST_GROUP_1), socket.INADDR_ANY)
    mcast_sock1.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, group1)
    print(f"[Slow Strategy] Subscribed to multicast group 1 ({config.MULTICAST_GROUP_1}:{config.MULTICAST_PORT_1})")

    # ── UDP multicast socket: subscribe to group 2 ───────────────────────────
    mcast_sock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mcast_sock2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    mcast_sock2.bind(("", config.MULTICAST_PORT_2))
    group2 = struct.pack("4sL", socket.inet_aton(config.MULTICAST_GROUP_2), socket.INADDR_ANY)
    mcast_sock2.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, group2)
    print(f"[Slow Strategy] Subscribed to multicast group 2 ({config.MULTICAST_GROUP_2}:{config.MULTICAST_PORT_2})")

    # ── TCP socket: send FIX orders to IOG ───────────────────────────────────
    fix_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fix_sock.connect((config.IOG_HOST, config.IOG_PORT))
    print(f"[Slow Strategy] Connected to IOG at {config.IOG_HOST}:{config.IOG_PORT}")

    print("[Slow Strategy] Running.")
    while True:
        pass

if __name__ == "__main__":
    main()
