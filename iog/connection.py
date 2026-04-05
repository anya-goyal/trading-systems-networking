"""
iog/connection.py

Connection management data structures and order book for the IOG.
"""

import time
from dataclasses import dataclass, field
from enum import Enum, auto

from iog.messages import InternalOrder


# ── Order States ─────────────────────────────────────────────────────────────

class OrderState(Enum):
    RECEIVED = auto()
    VALIDATED = auto()
    RISK_CHECKED = auto()
    ROUTED = auto()
    SENT = auto()
    PARTIALLY_FILLED = auto()
    FILLED = auto()
    REJECTED = auto()
    CANCELLED = auto()
    MALFORMED = auto()
    RISK_FAIL = auto()
    ROUTE_FAIL = auto()


TERMINAL_STATES = {
    OrderState.FILLED,
    OrderState.REJECTED,
    OrderState.CANCELLED,
    OrderState.MALFORMED,
    OrderState.RISK_FAIL,
    OrderState.ROUTE_FAIL,
}


# ── Connection States ────────────────────────────────────────────────────────

class ConnState(Enum):
    CONNECTING = auto()
    CONNECTED = auto()
    STALE = auto()
    DOWN = auto()


# ── Strategy Session ─────────────────────────────────────────────────────────

@dataclass
class StrategySession:
    sock: object
    strategy_id: str
    connected_at: float = field(default_factory=time.time)
    recv_buffer: bytearray = field(default_factory=bytearray)
    open_orders: set = field(default_factory=set)
    order_count: int = 0


# ── Exchange Connection ──────────────────────────────────────────────────────

MAX_BACKOFF = 30.0


@dataclass
class ExchangeConn:
    exchange_id: str
    sock: object = None
    state: ConnState = ConnState.DOWN
    last_heartbeat: float = 0.0
    reconnect_at: float = 0.0
    backoff: float = 1.0
    recv_buffer: bytearray = field(default_factory=bytearray)

    def mark_connected(self, sock) -> None:
        self.sock = sock
        self.state = ConnState.CONNECTED
        self.last_heartbeat = time.time()
        self.backoff = 1.0

    def mark_down(self) -> None:
        self.sock = None
        self.state = ConnState.DOWN

    def bump_backoff(self) -> None:
        self.backoff = min(self.backoff * 2, MAX_BACKOFF)


# ── Order Entry ──────────────────────────────────────────────────────────────

@dataclass
class OrderEntry:
    order: InternalOrder
    strategy_fd: int
    state: OrderState = OrderState.RECEIVED
    filled_qty: int = 0
    avg_fill_px: float = 0.0
    timestamps: dict = field(default_factory=dict)
    fix_msg: bytes = b""

    def __post_init__(self):
        self.timestamps[OrderState.RECEIVED] = time.time()


# ── Order Book ───────────────────────────────────────────────────────────────

class OrderBook:
    def __init__(self):
        self._orders: dict[str, OrderEntry] = {}

    def add(self, order: InternalOrder, strategy_fd: int) -> OrderEntry:
        entry = OrderEntry(order=order, strategy_fd=strategy_fd)
        self._orders[order.clOrdID] = entry
        return entry

    def get(self, clOrdID: str) -> OrderEntry | None:
        return self._orders.get(clOrdID)

    def update_state(self, clOrdID: str, new_state: OrderState) -> None:
        entry = self._orders.get(clOrdID)
        if entry is None:
            return
        entry.state = new_state
        entry.timestamps[new_state] = time.time()

    def get_open_orders(self, strategy_fd: int) -> list[OrderEntry]:
        return [
            e for e in self._orders.values()
            if e.strategy_fd == strategy_fd and e.state not in TERMINAL_STATES
        ]

    def get_open_orders_by_dest(self, destination: int) -> list[OrderEntry]:
        return [
            e for e in self._orders.values()
            if e.order.destination == destination and e.state not in TERMINAL_STATES
        ]

    def update_fill(self, clOrdID: str, filled_qty: int, fill_price: float) -> None:
        entry = self._orders.get(clOrdID)
        if entry is None:
            return
        # Compute new weighted average fill price
        old_total = entry.filled_qty * entry.avg_fill_px
        new_total = old_total + filled_qty * fill_price
        entry.filled_qty += filled_qty
        if entry.filled_qty > 0:
            entry.avg_fill_px = new_total / entry.filled_qty
