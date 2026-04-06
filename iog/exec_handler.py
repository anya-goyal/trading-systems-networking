"""
iog/exec_handler.py

Handles FIX execution reports from exchanges. Updates the order book
and risk state, returns routing info so the caller can forward to the strategy.
"""

from iog.connection import OrderBook, OrderState
from iog.risk import RiskChecker


class ExecReportHandler:
    def __init__(self, order_book: OrderBook, risk_checker: RiskChecker):
        self._book = order_book
        self._risk = risk_checker

    def handle(self, report: dict) -> dict | None:
        """Process a parsed execution report.

        Returns a dict with routing info (strategy_fd, clOrdID, etc.)
        or None if the clOrdID is unknown.
        """
        clOrdID = report["clOrdID"]
        entry = self._book.get(clOrdID)
        if entry is None:
            return None

        exec_type = report["exec_type"]
        last_qty = report.get("last_qty", 0)
        last_px = report.get("last_px", 0.0)

        if exec_type == "0":
            # Ack — exchange received the order, no state change
            pass

        elif exec_type == "1":
            # Partial fill
            self._book.update_fill(clOrdID, filled_qty=last_qty, fill_price=last_px)
            self._book.update_state(clOrdID, OrderState.PARTIALLY_FILLED)
            self._risk.update_position(
                entry.order.symbol, entry.order.side, last_qty
            )

        elif exec_type == "2":
            # Full fill
            self._book.update_fill(clOrdID, filled_qty=last_qty, fill_price=last_px)
            self._book.update_state(clOrdID, OrderState.FILLED)
            self._risk.update_position(
                entry.order.symbol, entry.order.side, last_qty
            )

        elif exec_type == "8":
            # Reject from exchange
            self._book.update_state(clOrdID, OrderState.REJECTED)

        elif exec_type == "4":
            # Canceled (user cancel or IOC remainder)
            self._book.update_state(clOrdID, OrderState.CANCELLED)

        return {
            "strategy_fd": entry.strategy_fd,
            "clOrdID": clOrdID,
            "exec_type": exec_type,
            "last_qty": last_qty,
            "last_px": last_px,
            "cum_qty": report.get("cum_qty", 0),
            "text": report.get("text", ""),
        }
