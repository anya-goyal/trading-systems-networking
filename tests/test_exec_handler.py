"""
Tests for the execution report handler.

The handler takes parsed FIX execution reports, updates the order book
and risk state, and returns information about what to forward to which strategy.
"""

import unittest

from iog.messages import (
    MSG_TYPE_NEW_ORDER,
    SIDE_BUY,
    SIDE_SELL,
    ORD_TYPE_LIMIT,
    DEST_EXCH1,
    InternalOrder,
)
from iog.connection import OrderBook, OrderState
from iog.risk import RiskChecker
from iog.exec_handler import ExecReportHandler


def _make_order(**overrides) -> InternalOrder:
    defaults = dict(
        msg_type=MSG_TYPE_NEW_ORDER,
        clOrdID="ORD001",
        symbol="AAPL",
        side=SIDE_BUY,
        order_type=ORD_TYPE_LIMIT,
        qty=100,
        price=150.25,
        destination=DEST_EXCH1,
        strategy_id="SLOW",
    )
    defaults.update(overrides)
    return InternalOrder(**defaults)


def _setup():
    book = OrderBook()
    risk = RiskChecker(
        max_order_qty=10000,
        max_notional=10_000_000.0,
        max_position_per_symbol=50000,
        max_orders_per_sec=100,
    )
    handler = ExecReportHandler(order_book=book, risk_checker=risk)
    return book, risk, handler


class TestHandleFill(unittest.TestCase):
    def test_full_fill_updates_state(self):
        book, risk, handler = _setup()
        book.add(_make_order(clOrdID="ORD001", qty=100), strategy_fd=10)
        book.update_state("ORD001", OrderState.SENT)

        result = handler.handle({
            "clOrdID": "ORD001",
            "exec_type": "2",  # Fill
            "cum_qty": 100,
            "last_px": 150.25,
            "last_qty": 100,
            "text": "",
        })

        entry = book.get("ORD001")
        self.assertEqual(entry.state, OrderState.FILLED)
        self.assertEqual(entry.filled_qty, 100)
        self.assertEqual(result["strategy_fd"], 10)
        self.assertEqual(result["clOrdID"], "ORD001")

    def test_fill_updates_risk_position(self):
        book, risk, handler = _setup()
        book.add(_make_order(clOrdID="ORD001", symbol="AAPL", side=SIDE_BUY, qty=100), strategy_fd=10)
        book.update_state("ORD001", OrderState.SENT)

        handler.handle({
            "clOrdID": "ORD001",
            "exec_type": "2",
            "cum_qty": 100,
            "last_px": 150.0,
            "last_qty": 100,
            "text": "",
        })

        self.assertEqual(risk.positions["AAPL"], 100)

    def test_sell_fill_decreases_position(self):
        book, risk, handler = _setup()
        risk.update_position("AAPL", SIDE_BUY, 200)  # existing long
        book.add(_make_order(clOrdID="ORD001", symbol="AAPL", side=SIDE_SELL, qty=50), strategy_fd=10)
        book.update_state("ORD001", OrderState.SENT)

        handler.handle({
            "clOrdID": "ORD001",
            "exec_type": "2",
            "cum_qty": 50,
            "last_px": 151.0,
            "last_qty": 50,
            "text": "",
        })

        self.assertEqual(risk.positions["AAPL"], 150)


class TestHandlePartialFill(unittest.TestCase):
    def test_partial_fill_state(self):
        book, risk, handler = _setup()
        book.add(_make_order(clOrdID="ORD001", qty=100), strategy_fd=10)
        book.update_state("ORD001", OrderState.SENT)

        handler.handle({
            "clOrdID": "ORD001",
            "exec_type": "1",  # Partial fill
            "cum_qty": 50,
            "last_px": 150.0,
            "last_qty": 50,
            "text": "",
        })

        entry = book.get("ORD001")
        self.assertEqual(entry.state, OrderState.PARTIALLY_FILLED)
        self.assertEqual(entry.filled_qty, 50)

    def test_partial_then_full_fill(self):
        book, risk, handler = _setup()
        book.add(_make_order(clOrdID="ORD001", qty=100), strategy_fd=10)
        book.update_state("ORD001", OrderState.SENT)

        handler.handle({
            "clOrdID": "ORD001",
            "exec_type": "1",
            "cum_qty": 60,
            "last_px": 150.0,
            "last_qty": 60,
            "text": "",
        })

        handler.handle({
            "clOrdID": "ORD001",
            "exec_type": "2",
            "cum_qty": 100,
            "last_px": 151.0,
            "last_qty": 40,
            "text": "",
        })

        entry = book.get("ORD001")
        self.assertEqual(entry.state, OrderState.FILLED)
        self.assertEqual(entry.filled_qty, 100)
        self.assertEqual(risk.positions["AAPL"], 100)


class TestHandleReject(unittest.TestCase):
    def test_exchange_reject(self):
        book, risk, handler = _setup()
        book.add(_make_order(clOrdID="ORD001"), strategy_fd=10)
        book.update_state("ORD001", OrderState.SENT)

        result = handler.handle({
            "clOrdID": "ORD001",
            "exec_type": "8",  # Reject
            "cum_qty": 0,
            "last_px": 0,
            "last_qty": 0,
            "text": "Insufficient liquidity",
        })

        entry = book.get("ORD001")
        self.assertEqual(entry.state, OrderState.REJECTED)
        self.assertEqual(result["text"], "Insufficient liquidity")
        # Position should not change on reject
        self.assertEqual(risk.positions["AAPL"], 0)


class TestHandleAck(unittest.TestCase):
    def test_ack_does_not_change_state_to_terminal(self):
        book, risk, handler = _setup()
        book.add(_make_order(clOrdID="ORD001"), strategy_fd=10)
        book.update_state("ORD001", OrderState.SENT)

        result = handler.handle({
            "clOrdID": "ORD001",
            "exec_type": "0",  # Ack
            "cum_qty": 0,
            "last_px": 0,
            "last_qty": 0,
            "text": "",
        })

        entry = book.get("ORD001")
        # Should stay SENT, ack just confirms receipt
        self.assertEqual(entry.state, OrderState.SENT)
        self.assertEqual(result["strategy_fd"], 10)


class TestUnknownOrder(unittest.TestCase):
    def test_unknown_clordid_returns_none(self):
        book, risk, handler = _setup()
        result = handler.handle({
            "clOrdID": "UNKNOWN",
            "exec_type": "2",
            "cum_qty": 100,
            "last_px": 150.0,
            "last_qty": 100,
            "text": "",
        })
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
