"""
exchange2.py

Simulated exchange 2.
  - Publishes a market data feed over UDP
  - Listens for incoming FIX orders over TCP
"""

import random
import select
import socket
import time

import config

SOH = "\x01"

# Exchange 2 uses a small symbol set
SYMBOLS = [
    ("AAPL", config.ASSET_EQUITIES),
    ("MSFT", config.ASSET_EQUITIES),
    ("ES", config.ASSET_FUTURES),
]


def build_market_update(seq_num):
    symbol, asset_class = random.choice(SYMBOLS)
    update_type = random.choice(["NEW_ORDER", "CANCEL", "FILL"])
    side = random.choice([config.SIDE_BUY, config.SIDE_SELL])
    price_cents = random.randint(10000, 30000)   # integer cents
    qty = random.randint(1, 100)
    timestamp = time.time()

    # Different format from exchange 1:
    # asset_class|seq|type|symbol|side|price_cents|qty|timestamp
    msg = f"{asset_class}|{seq_num}|{update_type}|{symbol}|{side}|{price_cents}|{qty}|{timestamp}"
    return msg.encode()


def parse_fix(raw_msg):
    # Allow either real FIX SOH separators or | for easier testing
    raw_msg = raw_msg.replace("|", SOH)

    fields = {}
    parts = raw_msg.split(SOH)

    for part in parts:
        if "=" in part:
            tag, value = part.split("=", 1)
            fields[tag] = value

    return fields


def build_fix_message(tags):
    body = ""
    for tag, value in tags:
        body += f"{tag}={value}{SOH}"

    head = f"8=FIX.4.2{SOH}9={len(body)}{SOH}"
    checksum = sum((head + body).encode()) % 256

    return f"{head}{body}10={checksum:03d}{SOH}"


def build_exec_report(cl_ord_id, exec_type, ord_status, text=""):
    tags = [
        ("35", "8"),           # ExecutionReport
        ("49", "EXCHANGE2"),
        ("56", "CLIENT"),
        ("11", cl_ord_id),
        ("150", exec_type),    # ExecType
        ("39", ord_status),    # OrdStatus
    ]

    if text:
        tags.append(("58", text))

    return build_fix_message(tags)


def handle_fix_message(raw_msg):
    fields = parse_fix(raw_msg)

    msg_type = fields.get("35")
    cl_ord_id = fields.get("11", "UNKNOWN")

    if msg_type == "D":
        print(f"[Exchange 2] NEW ORDER received: {fields}")
        return build_exec_report(cl_ord_id, "0", "0", "order accepted")

    elif msg_type == "F":
        print(f"[Exchange 2] CANCEL received: {fields}")
        return build_exec_report(cl_ord_id, "4", "4", "order cancelled")

    else:
        print(f"[Exchange 2] UNKNOWN FIX message: {fields}")
        return build_exec_report(cl_ord_id, "8", "8", "unsupported message type")


def main():
    # UDP socket: publish market feed
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(f"[Exchange 2] UDP feed socket open -> {config.EXCHANGE2_UDP_HOST}:{config.EXCHANGE2_UDP_PORT}")

    # TCP socket: receive FIX orders
    fix_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fix_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    fix_sock.bind((config.EXCHANGE2_FIX_HOST, config.EXCHANGE2_FIX_PORT))
    fix_sock.listen(5)
    print(f"[Exchange 2] FIX order socket listening on {config.EXCHANGE2_FIX_HOST}:{config.EXCHANGE2_FIX_PORT}")

    clients = []
    seq_num = 1
    last_send = 0

    print("[Exchange 2] Running.")

    try:
        while True:
            readable, _, _ = select.select([fix_sock] + clients, [], [], 0.2)

            for sock in readable:
                if sock is fix_sock:
                    client_sock, addr = fix_sock.accept()
                    clients.append(client_sock)
                    print(f"[Exchange 2] Connected by {addr}")

                else:
                    data = sock.recv(4096)

                    if not data:
                        print("[Exchange 2] Client disconnected")
                        clients.remove(sock)
                        sock.close()
                        continue

                    raw_msg = data.decode(errors="ignore")
                    reply = handle_fix_message(raw_msg)
                    sock.sendall(reply.encode())

            # Send one fake UDP update every second
            if time.time() - last_send >= 1.0:
                update = build_market_update(seq_num)
                udp_sock.sendto(update, (config.EXCHANGE2_UDP_HOST, config.EXCHANGE2_UDP_PORT))
                print(f"[Exchange 2] Sent UDP update: {update.decode()}")
                seq_num += 1
                last_send = time.time()

    except KeyboardInterrupt:
        print("\n[Exchange 2] Shutting down...")

    finally:
        for sock in clients:
            sock.close()
        fix_sock.close()
        udp_sock.close()


if __name__ == "__main__":
    main()