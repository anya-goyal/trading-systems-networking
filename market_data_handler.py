"""
market_data_handler.py

Receives raw market feeds from both exchanges over UDP.
Normalizes each update into a fixed-size binary message.
Publishes normalized data over UDP multicast, partitioned by asset class / symbol range.

Raw feed format (text, Exchange → MDH):
    Pipe-delimited UTF-8:  asset_class|seq_num|update_type|symbol|side|price_cents|qty|timestamp

    asset_class   int   1 = Equities, 2 = Options, 3 = Futures
    seq_num       int   monotonically increasing per exchange
    update_type   str   NEW_ORDER, CANCEL, FILL
    symbol        str   ticker (e.g. AAPL, ES)
    side          int   1 = BUY, 2 = SELL
    price_cents   int   price in integer cents
    qty           int   shares / contracts
    timestamp     float seconds since epoch

Wire format (little-endian, MDH → Strategies):
    Length Prefix   2B  byte count of body
    Message Type    1B  0x01 = snapshot, 0x02 = update
    Order ID        8B  unique across updates for the same order
    Sequence Number 8B  monotonically increasing per exchange
    Asset Class     1B  1 = Equities, 2 = Options, 3 = Futures
    Symbol          8B  UTF-8 null-padded ticker
    Side            1B  1 = BUY, 2 = SELL, 0 = N/A
    Update Type     1B  1 = NEW_ORDER, 2 = CANCEL, 3 = FILL
    Price           8B  limit / trade price
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

BODY_FMT  = "<BQQB8sBBQIQ"
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

def _normalize(raw: bytes, seq_tracker: SequenceTracker) -> tuple[bytes, int, bytes] | None:
    """Parse a pipe-delimited exchange datagram, track sequence, and return
    (frame, asset_class, symbol_raw) or None on decode error.

    Exchange text format:  asset_class|seq_num|update_type|symbol|side|price_cents|qty|timestamp
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        log.error("UTF-8 decode failed: %s", exc)
        return None

    parts = text.split("|")
    if len(parts) != 8:
        log.error("Expected 8 pipe-delimited fields, got %d: %r", len(parts), text)
        return None

    try:
        asset_class = int(parts[0])
        seq_num     = int(parts[1])
        update_str  = parts[2]
        symbol_str  = parts[3]
        side        = int(parts[4])
        price_cents = int(parts[5])
        quantity    = int(parts[6])
        timestamp   = int(float(parts[7]))
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

    body = struct.pack(
        BODY_FMT,
        config.MSG_TYPE_UPDATE,
        order_id,
        seq_num,
        asset_class,
        symbol_raw,
        side,
        update_type,
        price_cents,
        quantity,
        timestamp,
    )
    frame = struct.pack(HDR_FMT, len(body)) + body
    return frame, asset_class, symbol_raw

def _publish(mcast_socks: list[socket.socket],
             partition: int,
             frame: bytes) -> None:
    dest_ip, dest_port = config.MULTICAST_PARTITIONS[partition]
    try:
        mcast_socks[partition].sendto(frame, (dest_ip, dest_port))
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
