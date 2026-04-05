"""
iog/validator.py

Stateless order validation. Checks field presence and sanity.
Returns (True, None) on success or (False, reason_string) on failure.
"""

from iog.messages import (
    MSG_TYPE_NEW_ORDER,
    MSG_TYPE_CANCEL,
    SIDE_BUY,
    SIDE_SELL,
    ORD_TYPE_MARKET,
    ORD_TYPE_LIMIT,
    DEST_EXCH1,
    DEST_EXCH2,
    InternalOrder,
)

KNOWN_SYMBOLS = frozenset({
    "AAPL", "MSFT", "GOOG", "AMZN", "TSLA",
    "META", "NVDA", "JPM", "BAC", "WMT",
})

VALID_SIDES = {SIDE_BUY, SIDE_SELL}
VALID_ORD_TYPES = {ORD_TYPE_MARKET, ORD_TYPE_LIMIT}
VALID_DESTINATIONS = {DEST_EXCH1, DEST_EXCH2}
VALID_MSG_TYPES = {MSG_TYPE_NEW_ORDER, MSG_TYPE_CANCEL}


def validate(order: InternalOrder) -> tuple[bool, str | None]:
    """Validate an InternalOrder. Returns (ok, reason)."""

    if order.msg_type not in VALID_MSG_TYPES:
        return False, f"invalid msg_type: {order.msg_type}"

    if not order.clOrdID:
        return False, "missing clOrdID"

    if not order.symbol:
        return False, "missing symbol"

    if not order.strategy_id:
        return False, "missing strategy_id"

    if order.side not in VALID_SIDES:
        return False, f"invalid side: {order.side}"

    if order.order_type not in VALID_ORD_TYPES:
        return False, f"invalid order_type: {order.order_type}"

    if order.destination not in VALID_DESTINATIONS:
        return False, f"invalid destination: {order.destination}"

    if order.symbol not in KNOWN_SYMBOLS:
        return False, f"unknown symbol: {order.symbol}"

    # Cancel requests don't need qty/price checks
    if order.msg_type == MSG_TYPE_CANCEL:
        return True, None

    if order.qty <= 0:
        return False, f"invalid qty: {order.qty}"

    if order.order_type == ORD_TYPE_LIMIT and order.price <= 0.0:
        return False, f"invalid price for LIMIT order: {order.price}"

    return True, None
