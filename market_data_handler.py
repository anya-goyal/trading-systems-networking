"""
market_data_handler.py

Reads raw market feeds from both exchanges over UDP.
Normalizes the data into a common internal format.
Publishes normalized data over two UDP multicast groups partitioned by asset class (?).
"""

import socket
import struct
import config

def main():
    # ── UDP socket: receive feed from exchange 1 ─────────────────────────────
    udp_sock1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock1.bind((config.EXCHANGE1_UDP_HOST, config.EXCHANGE1_UDP_PORT))
    print(f"[MDH] Listening to exchange 1 feed on {config.EXCHANGE1_UDP_HOST}:{config.EXCHANGE1_UDP_PORT}")

    # ── UDP socket: receive feed from exchange 2 ─────────────────────────────
    udp_sock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock2.bind((config.EXCHANGE2_UDP_HOST, config.EXCHANGE2_UDP_PORT))
    print(f"[MDH] Listening to exchange 2 feed on {config.EXCHANGE2_UDP_HOST}:{config.EXCHANGE2_UDP_PORT}")

    # ── UDP multicast socket: publish normalized feed on group 1 ─────────────
    mcast_sock1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mcast_sock1.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    print(f"[MDH] Multicast group 1 publisher open → {config.MULTICAST_GROUP_1}:{config.MULTICAST_PORT_1}")

    # ── UDP multicast socket: publish normalized feed on group 2 ─────────────
    mcast_sock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mcast_sock2.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    print(f"[MDH] Multicast group 2 publisher open → {config.MULTICAST_GROUP_2}:{config.MULTICAST_PORT_2}")

    print("[MDH] Running.")
    while True:
        pass

if __name__ == "__main__":
    main()
