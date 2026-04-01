"""
Tests for IOG connection manager, order book, and session tracking.
"""

import time
import unittest
from unittest.mock import MagicMock

from iog.messages import (
    MSG_TYPE_NEW_ORDER,
    SIDE_BUY,
    SIDE_SELL,
    ORD_TYPE_LIMIT,
    DEST_EXCH1,
    DEST_EXCH2,
    InternalOrder,
)
from iog.connection import (
    StrategySession,
    ExchangeConn,
    ConnState,
    OrderBook,
    OrderEntry,
    OrderState,
)


# ── StrategySession ─────────────────────────────────────────────────────────

class TestStrategySession(unittest.TestCase):
    def test_creation(self):
        mock_sock = MagicMock()
        session = StrategySession(sock=mock_sock, strategy_id="SLOW")
        self.assertEqual(session.strategy_id, "SLOW")
        self.assertIsInstance(session.recv_buffer, bytearray)
        self.assertEqual(len(session.recv_buffer), 0)
        self.assertEqual(len(session.open_orders), 0)
        self.assertEqual(session.order_count, 0)

    def test_track_open_order(self):
        session = StrategySession(sock=MagicMock(), strategy_id="SLOW")
        session.open_orders.add("ORD001")
        session.open_orders.add("ORD002")
        self.assertEqual(len(session.open_orders), 2)
        session.open_orders.discard("ORD001")
        self.assertEqual(len(session.open_orders), 1)

    def test_increment_order_count(self):
        session = StrategySession(sock=MagicMock(), strategy_id="SLOW")
        session.order_count += 1
        session.order_count += 1
        self.assertEqual(session.order_count, 2)


# ── ExchangeConn ────────────────────────────────────────────────────────────

class TestExchangeConn(unittest.TestCase):
    def test_initial_state(self):
        conn = ExchangeConn(exchange_id="EXCH1")
        self.assertEqual(conn.exchange_id, "EXCH1")
        self.assertEqual(conn.state, ConnState.DOWN)
        self.assertIsNone(conn.sock)
        self.assertEqual(conn.backoff, 1.0)

    def test_mark_connected(self):
        conn = ExchangeConn(exchange_id="EXCH1")
        mock_sock = MagicMock()
        conn.mark_connected(mock_sock)
        self.assertEqual(conn.state, ConnState.CONNECTED)
        self.assertEqual(conn.sock, mock_sock)
        self.assertEqual(conn.backoff, 1.0)  # reset on connect

    def test_mark_down(self):
        conn = ExchangeConn(exchange_id="EXCH1")
        conn.mark_connected(MagicMock())
        conn.mark_down()
        self.assertEqual(conn.state, ConnState.DOWN)
        self.assertIsNone(conn.sock)

    def test_backoff_increases(self):
        conn = ExchangeConn(exchange_id="EXCH1")
        self.assertEqual(conn.backoff, 1.0)
        conn.bump_backoff()
        self.assertEqual(conn.backoff, 2.0)
        conn.bump_backoff()
        self.assertEqual(conn.backoff, 4.0)

    def test_backoff_caps_at_30(self):
        conn = ExchangeConn(exchange_id="EXCH1")
        for _ in range(20):
            conn.bump_backoff()
        self.assertEqual(conn.backoff, 30.0)

    def test_connected_resets_backoff(self):
        conn = ExchangeConn(exchange_id="EXCH1")
        conn.bump_backoff()
        conn.bump_backoff()
        self.assertEqual(conn.backoff, 4.0)
        conn.mark_connected(MagicMock())
        self.assertEqual(conn.backoff, 1.0)


# ── OrderBook ───────────────────────────────────────────────────────────────

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


class TestOrderBook(unittest.TestCase):
    def test_add_and_get(self):
        book = OrderBook()
        order = _make_order()
        book.add(order, strategy_fd=10)
        entry = book.get("ORD001")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.order.clOrdID, "ORD001")
        self.assertEqual(entry.strategy_fd, 10)
        self.assertEqual(entry.state, OrderState.RECEIVED)

    def test_get_missing_returns_none(self):
        book = OrderBook()
        self.assertIsNone(book.get("NONEXISTENT"))

    def test_update_state(self):
        book = OrderBook()
        book.add(_make_order(), strategy_fd=10)
        book.update_state("ORD001", OrderState.VALIDATED)
        entry = book.get("ORD001")
        self.assertEqual(entry.state, OrderState.VALIDATED)
        self.assertIn(OrderState.VALIDATED, entry.timestamps)

    def test_state_transitions_tracked(self):
        book = OrderBook()
        book.add(_make_order(), strategy_fd=10)
        book.update_state("ORD001", OrderState.VALIDATED)
        book.update_state("ORD001", OrderState.RISK_CHECKED)
        book.update_state("ORD001", OrderState.SENT)
        entry = book.get("ORD001")
        self.assertEqual(len(entry.timestamps), 4)  # RECEIVED + 3 transitions

    def test_get_open_orders_by_strategy(self):
        book = OrderBook()
        book.add(_make_order(clOrdID="ORD001"), strategy_fd=10)
        book.add(_make_order(clOrdID="ORD002"), strategy_fd=10)
        book.add(_make_order(clOrdID="ORD003", strategy_id="FAST"), strategy_fd=20)
        book.update_state("ORD001", OrderState.SENT)
        book.update_state("ORD002", OrderState.SENT)
        book.update_state("ORD003", OrderState.SENT)

        open_for_10 = book.get_open_orders(strategy_fd=10)
        self.assertEqual(len(open_for_10), 2)

        open_for_20 = book.get_open_orders(strategy_fd=20)
        self.assertEqual(len(open_for_20), 1)

    def test_filled_order_not_in_open(self):
        book = OrderBook()
        book.add(_make_order(clOrdID="ORD001"), strategy_fd=10)
        book.update_state("ORD001", OrderState.SENT)
        book.update_state("ORD001", OrderState.FILLED)

        open_orders = book.get_open_orders(strategy_fd=10)
        self.assertEqual(len(open_orders), 0)

    def test_rejected_order_not_in_open(self):
        book = OrderBook()
        book.add(_make_order(clOrdID="ORD001"), strategy_fd=10)
        book.update_state("ORD001", OrderState.REJECTED)

        open_orders = book.get_open_orders(strategy_fd=10)
        self.assertEqual(len(open_orders), 0)

    def test_get_open_orders_by_destination(self):
        book = OrderBook()
        book.add(_make_order(clOrdID="ORD001", destination=DEST_EXCH1), strategy_fd=10)
        book.add(_make_order(clOrdID="ORD002", destination=DEST_EXCH2), strategy_fd=10)
        book.update_state("ORD001", OrderState.SENT)
        book.update_state("ORD002", OrderState.SENT)

        exch1_orders = book.get_open_orders_by_dest(DEST_EXCH1)
        self.assertEqual(len(exch1_orders), 1)
        self.assertEqual(exch1_orders[0].order.clOrdID, "ORD001")

    def test_update_fill(self):
        book = OrderBook()
        book.add(_make_order(clOrdID="ORD001"), strategy_fd=10)
        book.update_state("ORD001", OrderState.SENT)
        book.update_fill("ORD001", filled_qty=50, fill_price=150.0)
        entry = book.get("ORD001")
        self.assertEqual(entry.filled_qty, 50)
        self.assertAlmostEqual(entry.avg_fill_px, 150.0)

    def test_update_fill_multiple(self):
        """Two partial fills should compute weighted average price."""
        book = OrderBook()
        book.add(_make_order(clOrdID="ORD001", qty=100), strategy_fd=10)
        book.update_state("ORD001", OrderState.SENT)
        book.update_fill("ORD001", filled_qty=60, fill_price=150.0)
        book.update_fill("ORD001", filled_qty=40, fill_price=151.0)
        entry = book.get("ORD001")
        self.assertEqual(entry.filled_qty, 100)
        # Weighted avg: (60*150 + 40*151) / 100 = 150.4
        self.assertAlmostEqual(entry.avg_fill_px, 150.4)


if __name__ == "__main__":
    unittest.main()
