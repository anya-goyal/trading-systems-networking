"""
iog/fix.py

FIX 4.2 message serialization and parsing.

FIX messages are tag=value pairs separated by SOH (0x01).
Tag 10 (CheckSum) = sum of all preceding bytes mod 256, zero-padded to 3 digits.
"""

from iog.messages import (
    SIDE_BUY,
    SIDE_SELL,
    ORD_TYPE_MARKET,
    ORD_TYPE_LIMIT,
    DEST_EXCH1,
    DEST_EXCH2,
    InternalOrder,
)

SOH = b"\x01"

DEST_TO_COMP_ID = {
    DEST_EXCH1: "EXCH1",
    DEST_EXCH2: "EXCH2",
}


def compute_checksum(data: bytes) -> str:
    """Compute FIX checksum: sum of all bytes mod 256, zero-padded to 3 digits."""
    return f"{sum(data) % 256:03d}"


def _build_message(fields: list[tuple[str, str]]) -> bytes:
    """Build a FIX message from a list of (tag, value) pairs. Appends checksum."""
    body = SOH.join(f"{tag}={val}".encode("ascii") for tag, val in fields) + SOH
    cs = compute_checksum(body)
    return body + f"10={cs}".encode("ascii") + SOH


def _format_price(price: float) -> str:
    """Format price, stripping unnecessary trailing zeros."""
    if price == int(price):
        return str(int(price))
    return f"{price:g}"


def serialize_new_order(order: InternalOrder) -> bytes:
    """Convert an InternalOrder into a FIX NewOrderSingle (MsgType D)."""
    fields = [
        ("8", "FIX.4.2"),
        ("35", "D"),
        ("49", "IOG"),
        ("56", DEST_TO_COMP_ID[order.destination]),
        ("11", order.clOrdID),
        ("55", order.symbol),
        ("54", str(order.side)),
        ("40", str(order.order_type)),
        ("38", str(order.qty)),
        ("44", _format_price(order.price)),
    ]
    return _build_message(fields)


def serialize_cancel_request(
    clOrdID: str, symbol: str, side: int, destination: int
) -> bytes:
    """Build a FIX OrderCancelRequest (MsgType F)."""
    fields = [
        ("8", "FIX.4.2"),
        ("35", "F"),
        ("49", "IOG"),
        ("56", DEST_TO_COMP_ID[destination]),
        ("11", clOrdID),
        ("55", symbol),
        ("54", str(side)),
    ]
    return _build_message(fields)


def parse_execution_report(data: bytes) -> dict:
    """Parse a FIX ExecutionReport (MsgType 8) into a dict.

    Returns dict with keys: clOrdID, exec_type, cum_qty, last_px, last_qty, text.
    Raises ValueError if required fields are missing.
    """
    fields = {}
    for pair in data.split(SOH):
        if not pair or b"=" not in pair:
            continue
        tag, _, value = pair.partition(b"=")
        fields[tag.decode("ascii")] = value.decode("ascii")

    if "11" not in fields:
        raise ValueError("Execution report missing ClOrdID (tag 11)")

    return {
        "clOrdID": fields["11"],
        "exec_type": fields.get("150", ""),
        "cum_qty": int(fields.get("14", "0")),
        "last_px": float(fields.get("31", "0")),
        "last_qty": int(fields.get("32", "0")),
        "text": fields.get("58", ""),
    }
