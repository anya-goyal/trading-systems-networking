"""
iog/messages.py

Binary message serialization/deserialization for the IOG internal protocol.

Strategy → IOG frame (50 bytes):
  [2B msg_length][1B msg_type][16B clOrdID][8B symbol][1B side][1B ord_type]
  [4B qty][8B price][1B destination][8B strategy_id]

IOG → Strategy response frame (44 bytes):
  [2B msg_length][1B msg_type][16B clOrdID][1B reject_reason]
  [4B filled_qty][4B cumulative_qty][8B fill_price][8B timestamp]
"""

import struct
from dataclasses import dataclass

# ── Message types (strategy → IOG) ──────────────────────────────────────────
MSG_TYPE_NEW_ORDER = 0x01
MSG_TYPE_CANCEL = 0x02

# ── Response types (IOG → strategy) ─────────────────────────────────────────
RESP_ACK = 0x10
RESP_FILL = 0x11
RESP_PARTIAL_FILL = 0x12
RESP_REJECT = 0x13
RESP_CANCELLED = 0x14

# ── Side ─────────────────────────────────────────────────────────────────────
SIDE_BUY = 1
SIDE_SELL = 2

# ── Order type ───────────────────────────────────────────────────────────────
ORD_TYPE_MARKET = 1
ORD_TYPE_LIMIT = 2

# ── Destination ──────────────────────────────────────────────────────────────
DEST_EXCH1 = 1
DEST_EXCH2 = 2

# ── Reject reasons ───────────────────────────────────────────────────────────
REJECT_NONE = 0
REJECT_VALIDATION = 1
REJECT_RISK = 2
REJECT_EXCH_DOWN = 3
REJECT_EXCH_REJECT = 4
REJECT_KILL_SWITCH = 5

# ── Struct formats ───────────────────────────────────────────────────────────
# Body only (no length prefix)
ORDER_BODY_FMT = "!B16s8sBBIdB8s"
ORDER_BODY_SIZE = struct.calcsize(ORDER_BODY_FMT)  # 48

RESPONSE_BODY_FMT = "!B16sBIIdd"
RESPONSE_BODY_SIZE = struct.calcsize(RESPONSE_BODY_FMT)  # 42

LENGTH_PREFIX_FMT = "!H"
LENGTH_PREFIX_SIZE = 2


@dataclass
class InternalOrder:
    msg_type: int
    clOrdID: str
    symbol: str
    side: int
    order_type: int
    qty: int
    price: float
    destination: int
    strategy_id: str


def _pad(s: str, width: int) -> bytes:
    """Encode string to utf-8 and null-pad to fixed width."""
    encoded = s.encode("utf-8")[:width]
    return encoded.ljust(width, b"\x00")


def _unpad(b: bytes) -> str:
    """Strip null padding and decode utf-8."""
    return b.rstrip(b"\x00").decode("utf-8")


def serialize_order(order: InternalOrder) -> bytes:
    """Serialize an InternalOrder into a length-prefixed binary frame."""
    body = struct.pack(
        ORDER_BODY_FMT,
        order.msg_type,
        _pad(order.clOrdID, 16),
        _pad(order.symbol, 8),
        order.side,
        order.order_type,
        order.qty,
        order.price,
        order.destination,
        _pad(order.strategy_id, 8),
    )
    return struct.pack(LENGTH_PREFIX_FMT, len(body)) + body


def deserialize_order(body: bytes) -> InternalOrder:
    """Deserialize a binary body (without length prefix) into an InternalOrder."""
    if len(body) < ORDER_BODY_SIZE:
        raise ValueError(
            f"Order body too short: got {len(body)} bytes, need {ORDER_BODY_SIZE}"
        )
    (
        msg_type,
        raw_clOrdID,
        raw_symbol,
        side,
        order_type,
        qty,
        price,
        destination,
        raw_strategy_id,
    ) = struct.unpack(ORDER_BODY_FMT, body[:ORDER_BODY_SIZE])

    return InternalOrder(
        msg_type=msg_type,
        clOrdID=_unpad(raw_clOrdID),
        symbol=_unpad(raw_symbol),
        side=side,
        order_type=order_type,
        qty=qty,
        price=price,
        destination=destination,
        strategy_id=_unpad(raw_strategy_id),
    )


def serialize_response(
    msg_type: int,
    clOrdID: str,
    reject_reason: int,
    filled_qty: int,
    cumulative_qty: int,
    fill_price: float,
    timestamp: float,
) -> bytes:
    """Serialize an IOG response into a length-prefixed binary frame."""
    body = struct.pack(
        RESPONSE_BODY_FMT,
        msg_type,
        _pad(clOrdID, 16),
        reject_reason,
        filled_qty,
        cumulative_qty,
        fill_price,
        timestamp,
    )
    return struct.pack(LENGTH_PREFIX_FMT, len(body)) + body


def deserialize_response(body: bytes) -> dict:
    """Deserialize an IOG response body (without length prefix) into a dict."""
    if len(body) < RESPONSE_BODY_SIZE:
        raise ValueError(
            f"Response body too short: got {len(body)} bytes, need {RESPONSE_BODY_SIZE}"
        )
    (
        msg_type,
        raw_clOrdID,
        reject_reason,
        filled_qty,
        cumulative_qty,
        fill_price,
        timestamp,
    ) = struct.unpack(RESPONSE_BODY_FMT, body[:RESPONSE_BODY_SIZE])

    return {
        "msg_type": msg_type,
        "clOrdID": _unpad(raw_clOrdID),
        "reject_reason": reject_reason,
        "filled_qty": filled_qty,
        "cumulative_qty": cumulative_qty,
        "fill_price": fill_price,
        "timestamp": timestamp,
    }


class Deserializer:
    """Streaming deserializer that handles partial TCP reads.

    Feed bytes as they arrive from recv(). Call drain() to extract
    any complete InternalOrder messages parsed so far.
    """

    def __init__(self):
        self._buffer = bytearray()

    def feed(self, data: bytes) -> None:
        """Append received bytes to the internal buffer."""
        self._buffer.extend(data)

    def drain(self):
        """Yield all complete InternalOrder messages from the buffer."""
        while True:
            # Need at least the length prefix
            if len(self._buffer) < LENGTH_PREFIX_SIZE:
                break
            body_length = struct.unpack_from(LENGTH_PREFIX_FMT, self._buffer, 0)[0]
            total = LENGTH_PREFIX_SIZE + body_length
            # Need the full frame
            if len(self._buffer) < total:
                break
            body = bytes(self._buffer[LENGTH_PREFIX_SIZE:total])
            del self._buffer[:total]
            yield deserialize_order(body)
