"""Send fake MDH multicast packets to drive strategy_fast."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import socket
import struct
import time
import config

MDH_BODY_FMT = "<BQQB8sBBdIQ"
HDR_FMT = "<H"
GROUP = config.MULTICAST_GROUP_1
PORT = config.MULTICAST_PORT_1


def send(symbol, side, price, qty, seq):
    body = struct.pack(
        MDH_BODY_FMT,
        config.MSG_TYPE_UPDATE,
        1,
        seq,
        config.ASSET_EQUITIES,
        symbol.encode().ljust(8, b"\x00")[:8],
        side,
        config.UPDATE_NEW_ORDER,
        price,
        qty,
        int(time.time()),
    )
    frame = struct.pack(HDR_FMT, len(body)) + body
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    sock.sendto(frame, (GROUP, PORT))
    sock.close()


seq = 1
while True:
    send("AAPL", config.SIDE_BUY, 150.00, 100, seq)
    send("AAPL", config.SIDE_SELL, 150.10, 100, seq + 1)
    seq += 2
    print(f"Sent AAPL bid=150.00 ask=150.10 (seq={seq})")
    time.sleep(0.5)
