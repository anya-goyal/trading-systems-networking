"""
exchange2.py

Simulated exchange 2.
  - Publishes a market data feed over UDP
  - Listens for incoming FIX orders over TCP
  - Sends FIX execution reports back to IOG
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

# Very basic in-memory order store
# clOrdID -> order info
OPEN_ORDERS = {}


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


def build_exec_report(
    cl_ord_id,
    exec_type,
    ord_status,
    cum_qty=0,
    last_px=0.0,
    last_qty=0,
    text="",
):
    """
    Build FIX ExecutionReport (35=8)

    Important fields for Ari's IOG:
      11  ClOrdID
      150 ExecType
      14  CumQty
      31  LastPx
      32  LastQty
      58  Text (optional)
    """
    tags = [
        ("35", "8"),                 # ExecutionReport
        ("49", "EXCHANGE2"),         # SenderCompID
        ("56", "IOG"),               # TargetCompID
        ("11", cl_ord_id),           # ClOrdID
        ("150", exec_type),          # ExecType
        ("39", ord_status),          # OrdStatus
        ("14", str(cum_qty)),        # CumQty
        ("31", f"{last_px:.2f}"),    # LastPx
        ("32", str(last_qty)),       # LastQty
    ]

    if text:
        tags.append(("58", text))

    return build_fix_message(tags)


def handle_fix_message(raw_msg):
    fields = parse_fix(raw_msg)

    msg_type = fields.get("35")

    # --------------------------------------------------
    # New Order Single (35=D)
    # --------------------------------------------------
    if msg_type == "D":
        cl_ord_id = fields.get("11", "UNKNOWN")
        symbol = fields.get("55")
        side = fields.get("54")
        qty_text = fields.get("38")
        ord_type = fields.get("40", "2")   # default to limit
        price_text = fields.get("44", "0")

        print(f"[Exchange 2] NEW ORDER received: {fields}")

        # Basic validation
        if cl_ord_id in OPEN_ORDERS:
            return build_exec_report(
                cl_ord_id,
                exec_type="8",
                ord_status="8",
                text="reject: duplicate ClOrdID",
            )

        if symbol is None:
            return build_exec_report(
                cl_ord_id,
                exec_type="8",
                ord_status="8",
                text="reject: missing symbol",
            )

        if side not in {"1", "2"}:
            return build_exec_report(
                cl_ord_id,
                exec_type="8",
                ord_status="8",
                text="reject: bad side",
            )

        try:
            qty = int(qty_text)
        except (TypeError, ValueError):
            return build_exec_report(
                cl_ord_id,
                exec_type="8",
                ord_status="8",
                text="reject: bad quantity",
            )

        if qty <= 0:
            return build_exec_report(
                cl_ord_id,
                exec_type="8",
                ord_status="8",
                text="reject: quantity must be > 0",
            )

        price = 0.0
        if ord_type == "2":   # limit order
            try:
                price = float(price_text)
            except (TypeError, ValueError):
                return build_exec_report(
                    cl_ord_id,
                    exec_type="8",
                    ord_status="8",
                    text="reject: bad price",
                )

            if price <= 0:
                return build_exec_report(
                    cl_ord_id,
                    exec_type="8",
                    ord_status="8",
                    text="reject: price must be > 0",
                )

        # Store order in simple order book / state store
        OPEN_ORDERS[cl_ord_id] = {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "price": price,
            "cum_qty": 0,
            "status": "OPEN",
        }

        # Send ACK back to IOG
        return build_exec_report(
            cl_ord_id,
            exec_type="0",   # ack
            ord_status="0",  # new
            cum_qty=0,
            last_px=0.0,
            last_qty=0,
            text="order accepted",
        )

    # --------------------------------------------------
    # Cancel Request (35=F)
    # --------------------------------------------------
    elif msg_type == "F":
        print(f"[Exchange 2] CANCEL received: {fields}")

        # Some cancel serializers use 41=OrigClOrdID.
        # Fall back to 11 if 41 is not present.
        orig_cl_ord_id = fields.get("41") or fields.get("11", "UNKNOWN")

        order = OPEN_ORDERS.pop(orig_cl_ord_id, None)
        if order is None:
            return build_exec_report(
                orig_cl_ord_id,
                exec_type="8",
                ord_status="8",
                text="reject: unknown order to cancel",
            )

        return build_exec_report(
            orig_cl_ord_id,
            exec_type="4",   # cancelled
            ord_status="4",
            cum_qty=order["cum_qty"],
            last_px=0.0,
            last_qty=0,
            text="order cancelled",
        )

    # --------------------------------------------------
    # Unknown / unsupported message type
    # --------------------------------------------------
    else:
        cl_ord_id = fields.get("11", "UNKNOWN")
        print(f"[Exchange 2] UNKNOWN FIX message: {fields}")

        return build_exec_report(
            cl_ord_id,
            exec_type="8",
            ord_status="8",
            text="reject: unsupported message type",
        )


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

                    # Send execution report back to IOG on same TCP connection
                    sock.sendall(reply.encode())
                    print(f"[Exchange 2] Sent ExecReport: {reply.replace(SOH, '|')}")

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