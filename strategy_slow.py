"""
strategy_slow.py

Slow strategy.
  - Subscribes to both multicast groups from the market data handler over UDP
  - Maintains a full order book per symbol (not just top-of-book)
  - Handles dropped/out-of-order packets via sequence number tracking
  - Sends orders to the Internal Order Gateway over TCP
  - Reconnects to IOG on send failure
"""

import logging
import select
import socket
import struct
import time
import config
from dataclasses import dataclass
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SlowStrategy] %(levelname)s %(message)s",
)
log = logging.getLogger("slow_strategy")


# MDH wire format: [2-byte body length][body]
# body: B Q Q B 8s B B d I Q
MDH_HDR_FMT  = "<H"
MDH_HDR_SIZE = struct.calcsize(MDH_HDR_FMT)
MDH_BODY_FMT = "<BQQB8sBBdIQ"   # little-endian; price = float64 USD
MDH_BODY_SIZE = struct.calcsize(MDH_BODY_FMT)

# Strategy -> IOG order wire format
IOG_ORDER_FMT = "!HB16s8sBBIdB8s"
IOG_ORDER_SIZE = struct.calcsize(IOG_ORDER_FMT)
IOG_ORDER_BODY = IOG_ORDER_SIZE - MDH_HDR_SIZE

STRATEGY_ID = b"SLOW\x00\x00\x00\x00"

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


class OrderBook:
    def __init__(self):
        self.books = {}

    def _ensure_symbol(self, symbol):
        if symbol not in self.books:
            self.books[symbol] = {"bids": {}, "asks": {}}

    def add_order(self, symbol, side, order_id, price, qty):
        self._ensure_symbol(symbol)
        side_key = "bids" if side == config.SIDE_BUY else "asks"
        book = self.books[symbol][side_key]
        if price not in book:
            book[price] = {}
        book[price][order_id] = qty

    def remove_order(self, symbol, side, order_id, price):
        self._ensure_symbol(symbol)
        side_key = "bids" if side == config.SIDE_BUY else "asks"
        book = self.books[symbol][side_key]
        if price in book:
            book[price].pop(order_id, None)
            if not book[price]:
                del book[price]

    def fill_order(self, symbol, side, order_id, price, fill_qty):
        self._ensure_symbol(symbol)
        side_key = "bids" if side == config.SIDE_BUY else "asks"
        book = self.books[symbol][side_key]
        if price in book and order_id in book[price]:
            book[price][order_id] -= fill_qty
            if book[price][order_id] <= 0:
                del book[price][order_id]
                if not book[price]:
                    del book[price]

    def best_bid(self, symbol):
        bids = self.books.get(symbol, {}).get("bids", {})
        return max(bids.keys()) if bids else 0.0

    def best_ask(self, symbol):
        asks = self.books.get(symbol, {}).get("asks", {})
        return min(asks.keys()) if asks else 0.0

    def bid_depth(self, symbol):
        bids = self.books.get(symbol, {}).get("bids", {})
        return sum(sum(orders.values()) for orders in bids.values())

    def ask_depth(self, symbol):
        asks = self.books.get(symbol, {}).get("asks", {})
        return sum(sum(orders.values()) for orders in asks.values())

    def spread(self, symbol):
        bid = self.best_bid(symbol)
        ask = self.best_ask(symbol)
        if bid > 0.0 and ask > 0.0 and ask > bid:
            return ask - bid
        return 0.0


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
    return f"SLOW{_order_seq:012d}"


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
    # order_id = struct.unpack("!Q", order_id_raw)[0]

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


def update_book(book: OrderBook, msg: MDHMessage) -> None:
    if msg.update_type == config.UPDATE_NEW_ORDER:
        book.add_order(msg.symbol, msg.side, msg.order_id, msg.price, msg.qty)
    elif msg.update_type == config.UPDATE_CANCEL:
        book.remove_order(msg.symbol, msg.side, msg.order_id, msg.price)
    elif msg.update_type == config.UPDATE_FILL:
        book.fill_order(msg.symbol, msg.side, msg.order_id, msg.price, msg.qty)


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


def should_trade(book: OrderBook, symbol: str) -> bool:
    bid = book.best_bid(symbol)
    ask = book.best_ask(symbol)
    if bid <= 0.0 or ask <= 0.0:
        return False
    if bid >= ask:
        return False
    spread = ask - bid
    return spread >= config.MIN_SPREAD_TO_TRADE


def pick_order(book: OrderBook, symbol: str) -> tuple[int, float, int]:
    # currently buys at the best bid when spread is wide
    return config.SIDE_BUY, book.best_bid(symbol), config.DEFAULT_ORDER_QTY


def subscribed_sockets() -> list[socket.socket]:
    sockets: list[socket.socket] = []
    for group, port in config.SUBSCRIBED_GROUPS:
        sock = make_multicast_socket(group, port)
        sockets.append(sock)
        log.info("Subscribed to %s:%d", group, port)
    return sockets


def main() -> None:
    log.info("Slow strategy starting")

    mdh_sockets = subscribed_sockets()
    if not mdh_sockets:
        log.error("No multicast subscriptions configured")
        return

    iog_sock = connect_iog()

    book = OrderBook()
    last_seq = {}
    last_trade_at = {}
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
                log.warning("Received unparseable MDH packet (%d bytes)", len(data))
                continue

            # asset and symbol filter
            if msg.asset_class not in config.SUBSCRIBED_ASSET_CLASSES:
                continue
            if config.SUBSCRIBED_SYMBOLS and msg.symbol not in config.SUBSCRIBED_SYMBOLS:
                continue

            # packet gap detection
            expected = last_seq.get(msg.symbol, 0) + 1
            if msg.seq_no < expected:
                log.warning(
                    "Out of order packet seq=%d for %s (expected > %d), dropping",
                    msg.seq_no, msg.symbol, last_seq[msg.symbol],
                )
                continue
            if msg.seq_no > expected:
                log.warning(
                    "Gap detected for %s: missing seq %d through %d",
                    msg.symbol, expected, msg.seq_no - 1,
                )
            last_seq[msg.symbol] = msg.seq_no

            update_book(book, msg)

            # TODO: let it run for a bit and didn't see any trades execute - change strat a bit
            if not should_trade(book, msg.symbol):
                # print("[DEBUG] should NOT trade")
                continue

            now = time.monotonic()
            if now - last_trade_at.get(msg.symbol, 0.0) < config.SYMBOL_COOLDOWN_SEC:
                continue

            if not limiter.allow():
                continue

            # build and send order
            side, price, qty = pick_order(book, msg.symbol)
            cl_ord_id, raw = build_new_order(msg.symbol, side, qty, price)

            if send_bytes(iog_sock, raw):
                last_trade_at[msg.symbol] = now
                spread = book.spread(msg.symbol)
                log.info(
                    "[ORDER] %s qty=%d price=%.4f symbol=%s spread=%.4f clordid=%s",
                    "BUY" if side == config.SIDE_BUY else "SELL",
                    qty,
                    price,
                    msg.symbol,
                    spread,
                    cl_ord_id,
                )
            else:
                log.error(
                    "Failed to send order %s for %s, reconnecting to IOG",
                    cl_ord_id, msg.symbol,
                )
                try:
                    iog_sock.close()
                except OSError:
                    pass
                try:
                    iog_sock = connect_iog()
                    log.info("Reconnected to IOG successfully")
                except OSError as exc:
                    log.error("IOG reconnect failed: %s", exc)

if __name__ == "__main__":
    main()