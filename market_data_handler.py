"""
market_data_handler.py

Receives raw market feeds from both exchanges over UDP.
Normalizes each update into a fixed-size binary message.
Publishes normalized data over UDP multicast, partitioned by asset class / symbol range.

Raw feed formats (text, Exchange → MDH):

    Exchange 2 (8 fields):
      asset_class|seq_num|update_type|symbol|side|price_cents|qty|timestamp
      asset_class int 1 = Equities, 2 = Options, 3 = Futures; update_type NEW_ORDER/CANCEL/FILL;
      price_cents int (MDH converts to dollars on the wire).

    Exchange 1 PITCH (see docs/exchange1.md):
      Seq|Time_ms|A|OrderID|Symbol|AssetType|Side|Shares|PriceInt   (Add)
      Seq|Time_ms|E|OrderID|ExecutedShares                             (Exec)
      Seq|Time_ms|X|OrderID|CanceledShares                            (Cancel)
      Seq|Time_ms|S|EventCode                                         (System — ignored, no multicast)
      PriceInt is fixed-point price × 10000; converted to dollars for the wire.

Wire format (little-endian, MDH → Strategies):
    Length Prefix   2B  byte count of body
    Message Type    1B  0x01 = snapshot, 0x02 = update
    Order ID        8B  unique across updates for the same order
    Sequence Number 8B  monotonically increasing per exchange
    Asset Class     1B  1 = Equities, 2 = Options, 3 = Futures
    Symbol          8B  UTF-8 null-padded ticker
    Side            1B  1 = BUY, 2 = SELL, 0 = N/A
    Update Type     1B  1 = NEW_ORDER, 2 = CANCEL, 3 = FILL
    Price           8B  limit / trade price (IEEE float64 dollars; matches IOG / FIX 44)
    Quantity        4B  shares / contracts
    Timestamp       8B  seconds since epoch
    ─────────────────── body total = 48 B
"""

from __future__ import annotations

import select
import socket
import struct
import logging
import time

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MDH] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("mdh")

BODY_FMT  = "<BQQB8sBBdIQ"
BODY_SIZE = struct.calcsize(BODY_FMT)

HDR_FMT   = "<H"
HDR_SIZE  = struct.calcsize(HDR_FMT)    # 2

assert BODY_SIZE == 48, f"Body size mismatch: expected 48, got {BODY_SIZE}"

UPDATE_TYPE_MAP = {
    "NEW_ORDER": config.UPDATE_NEW_ORDER,
    "CANCEL":    config.UPDATE_CANCEL,
    "FILL":      config.UPDATE_FILL,
}

_order_id_counter = 0

# Exchange 1 PITCH: exchange order id → (symbol_raw, asset_class, side, price_usd, leaves)
_pitch_open: dict[int, tuple[bytes, int, int, float, int]] = {}

ASSET_FROM_PITCH = {
    "EQ": config.ASSET_EQUITIES,
    "OPT": config.ASSET_OPTIONS,
    "FUT": config.ASSET_FUTURES,
}


def _next_order_id() -> int:
    global _order_id_counter
    _order_id_counter += 1
    return _order_id_counter

ASSET_CLASS_LABELS = {
    config.ASSET_EQUITIES: "EQ",
    config.ASSET_OPTIONS:  "OPT",
    config.ASSET_FUTURES:  "FUT",
}

def _partition_for(asset_class: int, symbol: bytes) -> int:
    """Return the MULTICAST_PARTITIONS index for this asset/symbol."""
    if asset_class == config.ASSET_OPTIONS:
        return 3
    if asset_class == config.ASSET_FUTURES:
        return 4
    first_char = chr(symbol[0]).upper()
    if "A" <= first_char <= "F":
        return 0
    if first_char <= "M":
        return 1
    return 2


class SequenceTracker:
    """Detects gaps and out-of-order delivery per exchange feed."""

    def __init__(self, label: str):
        self._label = label
        self._expected: int | None = None

    def check(self, seq: int) -> None:
        if self._expected is None:
            self._expected = seq + 1
            return
        if seq > self._expected:
            gap = seq - self._expected
            log.warning("%s: gap of %d (expected %d, got %d)",
                        self._label, gap, self._expected, seq)
        elif seq < self._expected:
            log.warning("%s: out-of-order (expected %d, got %d)",
                        self._label, self._expected, seq)
        self._expected = seq + 1


def _pitch_side_token(token: str) -> int:
    if token == "B":
        return config.SIDE_BUY
    if token == "S":
        return config.SIDE_SELL
    return config.SIDE_NA


def _price_usd_from_pitch_int(price_int: int) -> float:
    return price_int / 10000.0


def _is_pitch_message(parts: list[str]) -> bool:
    return len(parts) >= 4 and parts[2] in ("A", "E", "X", "S")


def _emit_update_frame(
    order_id: int,
    seq_num: int,
    asset_class: int,
    symbol_raw: bytes,
    side: int,
    update_type: int,
    price_usd: float,
    quantity: int,
    timestamp_sec: int,
) -> bytes:
    body = struct.pack(
        BODY_FMT,
        config.MSG_TYPE_UPDATE,
        order_id,
        seq_num,
        asset_class,
        symbol_raw,
        side,
        update_type,
        price_usd,
        quantity,
        timestamp_sec,
    )
    return struct.pack(HDR_FMT, len(body)) + body


def _normalize_pitch(parts: list[str], seq_tracker: SequenceTracker) -> tuple[bytes, int, bytes] | None:
    try:
        seq_num = int(parts[0])
        ts_ms = int(parts[1])
        kind = parts[2]
    except (ValueError, IndexError) as exc:
        log.error("PITCH header parse error: %s raw=%r", exc, "|".join(parts))
        return None

    ts_sec = ts_ms // 1000
    seq_tracker.check(seq_num)

    if kind == "S":
        return None

    if kind == "A":
        if len(parts) != 9:
            log.error("PITCH Add expected 9 fields, got %d: %r", len(parts), "|".join(parts))
            return None
        try:
            order_id = int(parts[3])
            symbol_str = parts[4]
            pitch_asset = parts[5]
            side = _pitch_side_token(parts[6])
            quantity = int(parts[7])
            price_int = int(parts[8])
        except (ValueError, IndexError) as exc:
            log.error("PITCH Add field error: %s", exc)
            return None
        asset_class = ASSET_FROM_PITCH.get(pitch_asset)
        if asset_class is None:
            log.error("Unknown PITCH asset type %r", pitch_asset)
            return None
        if side == config.SIDE_NA:
            log.error("Unknown PITCH side %r", parts[6])
            return None
        price_usd = _price_usd_from_pitch_int(price_int)
        symbol_raw = symbol_str.encode("utf-8").ljust(8, b"\x00")[:8]
        _pitch_open[order_id] = (symbol_raw, asset_class, side, price_usd, quantity)
        frame = _emit_update_frame(
            order_id,
            seq_num,
            asset_class,
            symbol_raw,
            side,
            config.UPDATE_NEW_ORDER,
            price_usd,
            quantity,
            ts_sec,
        )
        return frame, asset_class, symbol_raw

    if kind in ("E", "X"):
        if len(parts) != 5:
            log.error("PITCH %s expected 5 fields, got %d", kind, len(parts))
            return None
        try:
            order_id = int(parts[3])
            qty = int(parts[4])
        except (ValueError, IndexError) as exc:
            log.error("PITCH %s field error: %s", kind, exc)
            return None
        book_row = _pitch_open.get(order_id)
        if book_row is None:
            log.warning("PITCH %s for unknown order_id=%d — dropping", kind, order_id)
            return None
        symbol_raw, asset_class, side, price_usd, leaves = book_row
        update_type = config.UPDATE_FILL if kind == "E" else config.UPDATE_CANCEL
        new_leaves = leaves - qty
        if new_leaves <= 0:
            _pitch_open.pop(order_id, None)
        else:
            _pitch_open[order_id] = (symbol_raw, asset_class, side, price_usd, new_leaves)
        frame = _emit_update_frame(
            order_id,
            seq_num,
            asset_class,
            symbol_raw,
            side,
            update_type,
            price_usd,
            qty,
            ts_sec,
        )
        return frame, asset_class, symbol_raw

    log.error("Unknown PITCH message kind %r", kind)
    return None


def _normalize(raw: bytes, seq_tracker: SequenceTracker) -> tuple[bytes, int, bytes] | None:
    """Parse a pipe-delimited exchange datagram, track sequence, and return
    (frame, asset_class, symbol_raw) or None on decode error / skipped message.
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        log.error("UTF-8 decode failed: %s", exc)
        return None

    parts = text.split("|")
    if _is_pitch_message(parts):
        return _normalize_pitch(parts, seq_tracker)

    if len(parts) != 8:
        log.error("Expected 8 pipe-delimited fields (Exchange 2), got %d: %r", len(parts), text)
        return None

    try:
        asset_class = int(parts[0])
        seq_num = int(parts[1])
        update_str = parts[2]
        symbol_str = parts[3]
        side = int(parts[4])
        price_cents = int(parts[5])
        quantity = int(parts[6])
        timestamp = int(float(parts[7]))
    except (ValueError, IndexError) as exc:
        log.error("Field parse error: %s  raw=%r", exc, text)
        return None

    update_type = UPDATE_TYPE_MAP.get(update_str)
    if update_type is None:
        log.error("Unknown update type %r", update_str)
        return None

    seq_tracker.check(seq_num)

    order_id = _next_order_id()
    symbol_raw = symbol_str.encode("utf-8").ljust(8, b"\x00")[:8]
    price_usd = price_cents / 100.0

    frame = _emit_update_frame(
        order_id,
        seq_num,
        asset_class,
        symbol_raw,
        side,
        update_type,
        price_usd,
        quantity,
        timestamp,
    )
    return frame, asset_class, symbol_raw

def _publish(mcast_socks: list[socket.socket],
             partition: int,
             frame: bytes) -> None:
    dest_ip, dest_port = config.MULTICAST_PARTITIONS[partition]
    try:
        mcast_socks[partition].sendto(frame, (dest_ip, dest_port))
        print(f"[MDH] Sent multicast → {dest_ip}:{dest_port} (partition={partition}, bytes={len(frame)})")
    except OSError as exc:
        log.error("Multicast send failed (partition %d): %s", partition, exc)

def _bind_exchange_socket(host: str, port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setblocking(False)
    sock.bind((host, port))
    return sock


def _make_mcast_socket() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    return sock

RECV_BUF = 65535

def main() -> None:
    feeds: list[tuple[socket.socket, SequenceTracker]] = []
    for label, host, port in [
        ("Exchange1", config.EXCHANGE1_UDP_HOST, config.EXCHANGE1_UDP_PORT),
        ("Exchange2", config.EXCHANGE2_UDP_HOST, config.EXCHANGE2_UDP_PORT),
    ]:
        sock = _bind_exchange_socket(host, port)
        tracker = SequenceTracker(label)
        feeds.append((sock, tracker))
        log.info("Listening to %s feed on %s:%d", label, host, port)

    mcast_socks: list[socket.socket] = []
    for ip, port in config.MULTICAST_PARTITIONS:
        msock = _make_mcast_socket()
        mcast_socks.append(msock)
        log.info("Multicast publisher ready → %s:%d", ip, port)

    read_fds = [sock for sock, _ in feeds]

    log.info("Running.  body_size=%d  partitions=%d",
             BODY_SIZE, len(config.MULTICAST_PARTITIONS))

    while True:
        try:
            readable, _, _ = select.select(read_fds, [], [], 1.0)
        except OSError as exc:
            log.error("select() error: %s", exc)
            continue

        for sock in readable:
            tracker = next(t for s, t in feeds if s is sock)
            try:
                raw, addr = sock.recvfrom(RECV_BUF)
            except OSError as exc:
                log.error("recvfrom failed: %s — feed may be down", exc)
                continue

            result = _normalize(raw, tracker)
            if result is None:
                continue

            frame, asset_class, symbol_raw = result
            partition = _partition_for(asset_class, symbol_raw)
            _publish(mcast_socks, partition, frame)


if __name__ == "__main__":
    main()
