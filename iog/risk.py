"""
iog/risk.py

Stateful risk checker. Maintains positions, rate counters, and kill switch.
Checks orders against configurable limits.
"""

import time
from collections import defaultdict, deque

from iog.messages import SIDE_BUY, SIDE_SELL, InternalOrder


class RiskChecker:
    def __init__(
        self,
        max_order_qty: int,
        max_notional: float,
        max_position_per_symbol: int,
        max_orders_per_sec: int,
    ):
        self.max_order_qty = max_order_qty
        self.max_notional = max_notional
        self.max_position_per_symbol = max_position_per_symbol
        self.max_orders_per_sec = max_orders_per_sec

        self.positions: dict[str, int] = defaultdict(int)
        self._order_timestamps: dict[str, deque] = defaultdict(deque)
        self._kill_switch = False

    def check(self, order: InternalOrder) -> tuple[bool, str | None]:
        """Check an order against all risk limits. Returns (ok, reason)."""

        if self._kill_switch:
            return False, "kill_switch active"

        if order.qty > self.max_order_qty:
            return False, f"exceeds max_order_qty ({order.qty} > {self.max_order_qty})"

        notional = order.price * order.qty
        if notional > self.max_notional:
            return False, f"exceeds max_notional ({notional} > {self.max_notional})"

        # Project position after this order
        current = self.positions[order.symbol]
        if order.side == SIDE_BUY:
            projected = current + order.qty
        else:
            projected = current - order.qty

        if abs(projected) > self.max_position_per_symbol:
            return False, (
                f"exceeds position limit for {order.symbol} "
                f"({abs(projected)} > {self.max_position_per_symbol})"
            )

        # Rate limit per strategy
        now = time.monotonic()
        timestamps = self._order_timestamps[order.strategy_id]
        # Expire old timestamps outside the 1-second window
        while timestamps and timestamps[0] <= now - 1.0:
            timestamps.popleft()
        if len(timestamps) >= self.max_orders_per_sec:
            return False, (
                f"rate limit exceeded for {order.strategy_id} "
                f"({len(timestamps)} >= {self.max_orders_per_sec}/s)"
            )
        timestamps.append(now)

        return True, None

    def update_position(self, symbol: str, side: int, qty: int) -> None:
        """Update position on a confirmed fill."""
        if side == SIDE_BUY:
            self.positions[symbol] += qty
        else:
            self.positions[symbol] -= qty

    def activate_kill_switch(self) -> None:
        self._kill_switch = True

    def reset_kill_switch(self) -> None:
        self._kill_switch = False
