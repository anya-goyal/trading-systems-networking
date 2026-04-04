"""
strategy_fast.py
Simple fast strategy:
  - Read incoming MDH multicast packets
  - Maintain a lightweight best bid/ask snapshot per symbol
  - If spread >= configured threshold, send a simple order to IOG
    - Ignore IOG responses and keep flow MDH-in / IOG-out
"""

import logging
import select
import socket
import struct
import time
from dataclasses import dataclass
from typing import Optional

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FastStrategy] %(levelname)s %(message)s",
)
log = logging.getLogger("fast_strategy")


# MDH wire format: [2-byte body length][body]
# body: B Q Q B 8s B B d I Q
MDH_HDR_FMT = "!H"
MDH_HDR_SIZE = struct.calcsize(MDH_HDR_FMT)
MDH_BODY_FMT = "!BQQB8sBBdIQ"
MDH_BODY_SIZE = struct.calcsize(MDH_BODY_FMT)

# Strategy -> IOG order wire format: !HB16s8sBBIdB8s
IOG_ORDER_FMT = "!HB16s8sBBIdB8s"
IOG_ORDER_SIZE = struct.calcsize(IOG_ORDER_FMT)
IOG_ORDER_BODY = IOG_ORDER_SIZE - MDH_HDR_SIZE

STRATEGY_ID = b"FAST\x00\x00\x00\x00"

IOG_NEW_ORDER = 0x01
ORD_LIMIT = 2
DEST_EXCHANGE_1 = 1


@dataclass
class MDHMessage:
    msg_type: int
    order_id: int
    seq_no: int
    asset_class: int
    symbol: str
    side: int
    update_type: int
    price: float
    qty: int


@dataclass
class MarketSnapshot:
    symbol: str
    best_bid: float = 0.0
    best_ask: float = 0.0
    bid_qty: int = 0
    ask_qty: int = 0
    last_seq: int = 0
    updated_at: float = 0.0


class RateLimiter:
    def __init__(self, max_per_second: int) -> None:
        self._max = max_per_second
        self._count = 0
        self._window_start = time.monotonic()

    def allow(self) -> bool:
        now = time.monotonic()
        if now - self._window_start >= 1.0:
            self._window_start = now
            self._count = 0
        if self._count < self._max:
            self._count += 1
            return True
        return False


_order_seq = 0


def next_cl_ord_id() -> str:
    global _order_seq
    _order_seq += 1
    return f"FAST{_order_seq:012d}"


def make_multicast_socket(group: str, port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    reuse_port = getattr(socket, "SO_REUSEPORT", None)
    if reuse_port is not None:
        sock.setsockopt(socket.SOL_SOCKET, reuse_port, 1)
    sock.bind(("", port))
    membership = struct.pack("4sL", socket.inet_aton(group), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
    sock.setblocking(False)
    return sock


def connect_iog() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((config.IOG_HOST, config.IOG_PORT))
    sock.setblocking(False)
    log.info("Connected to IOG at %s:%d", config.IOG_HOST, config.IOG_PORT)
    return sock


def parse_mdh_packet(data: bytes) -> Optional[MDHMessage]:
    if len(data) < MDH_HDR_SIZE:
        return None

    (body_len,) = struct.unpack_from(MDH_HDR_FMT, data, 0)
    if body_len < MDH_BODY_SIZE or len(data) < MDH_HDR_SIZE + body_len:
        return None

    (
        msg_type,
        order_id,
        seq_no,
        asset_class,
        symbol_raw,
        side,
        update_type,
        price,
        qty,
        _timestamp,
    ) = struct.unpack_from(MDH_BODY_FMT, data, MDH_HDR_SIZE)

    symbol = symbol_raw.rstrip(b"\x00").decode("utf-8", errors="replace")
    return MDHMessage(
        msg_type=msg_type,
        order_id=order_id,
        seq_no=seq_no,
        asset_class=asset_class,
        symbol=symbol,
        side=side,
        update_type=update_type,
        price=price,
        qty=qty,
    )


def update_snapshot(store: dict[str, MarketSnapshot], msg: MDHMessage) -> MarketSnapshot:
    snap = store.get(msg.symbol)
    if snap is None:
        snap = MarketSnapshot(symbol=msg.symbol)
        store[msg.symbol] = snap

    snap.last_seq = msg.seq_no
    snap.updated_at = time.monotonic()

    if msg.update_type == config.UPDATE_CANCEL:
        if msg.side == config.SIDE_BUY:
            snap.best_bid = 0.0
            snap.bid_qty = 0
        elif msg.side == config.SIDE_SELL:
            snap.best_ask = 0.0
            snap.ask_qty = 0
        return snap

    if msg.side == config.SIDE_BUY:
        snap.best_bid = msg.price
        snap.bid_qty = msg.qty
    elif msg.side == config.SIDE_SELL:
        snap.best_ask = msg.price
        snap.ask_qty = msg.qty

    return snap


def build_new_order(symbol: str, side: int, qty: int, price: float) -> tuple[str, bytes]:
    cl_ord_id = next_cl_ord_id()
    raw = struct.pack(
        IOG_ORDER_FMT,
        IOG_ORDER_BODY,
        IOG_NEW_ORDER,
        cl_ord_id.encode().ljust(16, b"\x00")[:16],
        symbol.encode().ljust(8, b"\x00")[:8],
        side,
        ORD_LIMIT,
        qty,
        price,
        DEST_EXCHANGE_1,
        STRATEGY_ID,
    )
    return cl_ord_id, raw


def send_bytes(sock: socket.socket, data: bytes) -> bool:
    view = memoryview(data)
    total_sent = 0

    while total_sent < len(data):
        try:
            sent = sock.send(view[total_sent:])
        except BlockingIOError:
            time.sleep(0.0001)
            continue
        except OSError as exc:
            log.error("IOG send failed: %s", exc)
            return False

        if sent <= 0:
            return False
        total_sent += sent

    return True


def should_trade(snap: MarketSnapshot) -> bool:
    if snap.best_bid <= 0.0 or snap.best_ask <= 0.0:
        return False
    if snap.best_bid >= snap.best_ask:
        return False

    spread = snap.best_ask - snap.best_bid
    return spread >= config.MIN_SPREAD_TO_TRADE


def pick_order(snap: MarketSnapshot) -> tuple[int, float, int]:
    # Keep it simple: buy at bid when spread is wide enough.
    return config.SIDE_BUY, snap.best_bid, config.DEFAULT_ORDER_QTY


def subscribed_sockets() -> list[socket.socket]:
    sockets: list[socket.socket] = []
    for group, port in config.SUBSCRIBED_GROUPS:
        sock = make_multicast_socket(group, port)
        sockets.append(sock)
        log.info("Subscribed to %s:%d", group, port)
    return sockets


def main() -> None:
    log.info("Simple fast strategy starting")

    mdh_sockets = subscribed_sockets()
    if not mdh_sockets:
        log.error("No multicast subscriptions configured")
        return

    iog_sock = connect_iog()

    snapshots: dict[str, MarketSnapshot] = {}
    last_trade_at: dict[str, float] = {}

    limiter = RateLimiter(config.MAX_ORDER_RATE)

    while True:
        readable, _, _ = select.select(mdh_sockets, [], [], 0.001)

        for sock in readable:
            try:
                data, _ = sock.recvfrom(config.UDP_BUF_SIZE)
            except BlockingIOError:
                continue
            except OSError as exc:
                log.error("MDH recv error: %s", exc)
                continue

            msg = parse_mdh_packet(data)
            if msg is None:
                continue

            if msg.asset_class not in config.SUBSCRIBED_ASSET_CLASSES:
                continue
            if config.SUBSCRIBED_SYMBOLS and msg.symbol not in config.SUBSCRIBED_SYMBOLS:
                continue

            snap = update_snapshot(snapshots, msg)
            if not should_trade(snap):
                continue

            now = time.monotonic()
            if now - last_trade_at.get(snap.symbol, 0.0) < config.SYMBOL_COOLDOWN_SEC:
                continue

            if not limiter.allow():
                continue

            side, price, qty = pick_order(snap)
            cl_ord_id, raw = build_new_order(snap.symbol, side, qty, price)

            if send_bytes(iog_sock, raw):
                last_trade_at[snap.symbol] = now
                spread = snap.best_ask - snap.best_bid
                log.info(
                    "[ORDER] %s qty=%d price=%.4f symbol=%s spread=%.4f clordid=%s",
                    "BUY" if side == config.SIDE_BUY else "SELL",
                    qty,
                    price,
                    snap.symbol,
                    spread,
                    cl_ord_id,
                )


if __name__ == "__main__":
    main()
