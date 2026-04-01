"""
Integration test for the IOG pipeline.

Tests the full order processing pipeline without real sockets —
verifies that the components are wired correctly by exercising
IOGServer._process_order directly.
"""

import unittest
from unittest.mock import MagicMock, patch

from iog.messages import (
    MSG_TYPE_NEW_ORDER,
    MSG_TYPE_CANCEL,
    SIDE_BUY,
    SIDE_SELL,
    ORD_TYPE_LIMIT,
    ORD_TYPE_MARKET,
    DEST_EXCH1,
    DEST_EXCH2,
    RESP_REJECT,
    REJECT_VALIDATION,
    REJECT_RISK,
    REJECT_EXCH_DOWN,
    InternalOrder,
    serialize_order,
    serialize_response,
    deserialize_response,
)
from iog.connection import OrderState, ConnState

# Import the server class
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from internal_order_gateway import IOGServer


def _order(**overrides) -> InternalOrder:
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


class TestIOGPipeline(unittest.TestCase):
    """Test the order pipeline by calling _process_order directly."""

    def setUp(self):
        self.server = IOGServer()
        # Set up a mock strategy session
        mock_sock = MagicMock()
        mock_sock.sendall = MagicMock()
        from iog.connection import StrategySession
        session = StrategySession(sock=mock_sock, strategy_id="SLOW")
        self.server._strategy_sessions[10] = session
        self.strategy_sock = mock_sock

    def _connect_mock_exchange(self, dest):
        """Wire up a mock exchange connection."""
        from iog.connection import ExchangeConn
        conn = ExchangeConn(exchange_id=f"EXCH{dest}")
        mock_sock = MagicMock()
        mock_sock.sendall = MagicMock()
        conn.mark_connected(mock_sock)
        self.server._exchange_conns[dest] = conn
        return mock_sock

    def test_valid_order_sent_to_exchange(self):
        exch_sock = self._connect_mock_exchange(DEST_EXCH1)
        self.server._process_order(_order(), strategy_fd=10)

        # Exchange socket should have received a FIX message
        exch_sock.sendall.assert_called_once()
        sent_data = exch_sock.sendall.call_args[0][0]
        self.assertIn(b"35=D", sent_data)  # NewOrderSingle
        self.assertIn(b"55=AAPL", sent_data)

        # Order book should show SENT
        entry = self.server._order_book.get("ORD001")
        self.assertEqual(entry.state, OrderState.SENT)

    def test_invalid_order_rejected(self):
        self._connect_mock_exchange(DEST_EXCH1)
        # Invalid side = 0
        self.server._process_order(_order(side=0), strategy_fd=10)

        # Strategy should have received a reject
        self.strategy_sock.sendall.assert_called_once()
        sent_data = self.strategy_sock.sendall.call_args[0][0]
        resp = deserialize_response(sent_data[2:])  # strip length prefix
        self.assertEqual(resp["msg_type"], RESP_REJECT)
        self.assertEqual(resp["reject_reason"], REJECT_VALIDATION)

    def test_risk_fail_rejected(self):
        self._connect_mock_exchange(DEST_EXCH1)
        # Quantity exceeds MAX_ORDER_QTY (10_000)
        self.server._process_order(_order(qty=99999), strategy_fd=10)

        self.strategy_sock.sendall.assert_called_once()
        sent_data = self.strategy_sock.sendall.call_args[0][0]
        resp = deserialize_response(sent_data[2:])
        self.assertEqual(resp["msg_type"], RESP_REJECT)
        self.assertEqual(resp["reject_reason"], REJECT_RISK)

    def test_exchange_down_rejected(self):
        # Don't connect exchange — it should be DOWN
        self.server._process_order(_order(), strategy_fd=10)

        self.strategy_sock.sendall.assert_called_once()
        sent_data = self.strategy_sock.sendall.call_args[0][0]
        resp = deserialize_response(sent_data[2:])
        self.assertEqual(resp["msg_type"], RESP_REJECT)
        self.assertEqual(resp["reject_reason"], REJECT_EXCH_DOWN)

    def test_route_to_correct_exchange(self):
        exch1_sock = self._connect_mock_exchange(DEST_EXCH1)
        exch2_sock = self._connect_mock_exchange(DEST_EXCH2)

        self.server._process_order(
            _order(clOrdID="ORD_E2", destination=DEST_EXCH2), strategy_fd=10
        )

        exch1_sock.sendall.assert_not_called()
        exch2_sock.sendall.assert_called_once()
        sent_data = exch2_sock.sendall.call_args[0][0]
        self.assertIn(b"56=EXCH2", sent_data)

    def test_multiple_orders_tracked(self):
        self._connect_mock_exchange(DEST_EXCH1)
        self.server._process_order(_order(clOrdID="ORD001"), strategy_fd=10)
        self.server._process_order(_order(clOrdID="ORD002"), strategy_fd=10)

        self.assertEqual(
            self.server._order_book.get("ORD001").state, OrderState.SENT
        )
        self.assertEqual(
            self.server._order_book.get("ORD002").state, OrderState.SENT
        )

    def test_kill_switch_rejects(self):
        self._connect_mock_exchange(DEST_EXCH1)
        self.server._risk.activate_kill_switch()
        self.server._process_order(_order(), strategy_fd=10)

        sent_data = self.strategy_sock.sendall.call_args[0][0]
        resp = deserialize_response(sent_data[2:])
        self.assertEqual(resp["reject_reason"], REJECT_RISK)

    def test_cancel_request(self):
        exch_sock = self._connect_mock_exchange(DEST_EXCH1)
        # First send a valid order
        self.server._process_order(_order(clOrdID="ORD001"), strategy_fd=10)
        exch_sock.sendall.reset_mock()

        # Now cancel it
        cancel = _order(
            msg_type=MSG_TYPE_CANCEL, clOrdID="ORD001", qty=0, price=0.0
        )
        self.server._process_order(cancel, strategy_fd=10)

        # Exchange should have received a cancel request
        exch_sock.sendall.assert_called_once()
        sent_data = exch_sock.sendall.call_args[0][0]
        self.assertIn(b"35=F", sent_data)  # CancelRequest


class TestExecReportForwarding(unittest.TestCase):
    """Test that execution reports are forwarded to the right strategy."""

    def setUp(self):
        self.server = IOGServer()
        mock_sock = MagicMock()
        mock_sock.sendall = MagicMock()
        from iog.connection import StrategySession
        session = StrategySession(sock=mock_sock, strategy_id="SLOW")
        self.server._strategy_sessions[10] = session
        self.strategy_sock = mock_sock

        # Connect exchange and send an order
        from iog.connection import ExchangeConn
        conn = ExchangeConn(exchange_id="EXCH1")
        exch_sock = MagicMock()
        exch_sock.sendall = MagicMock()
        conn.mark_connected(exch_sock)
        self.server._exchange_conns[DEST_EXCH1] = conn

        self.server._process_order(_order(clOrdID="ORD001"), strategy_fd=10)
        self.strategy_sock.sendall.reset_mock()

    def test_fill_forwarded(self):
        result = self.server._exec_handler.handle({
            "clOrdID": "ORD001",
            "exec_type": "2",
            "cum_qty": 100,
            "last_px": 150.25,
            "last_qty": 100,
            "text": "",
        })
        self.server._forward_exec_report(result)

        self.strategy_sock.sendall.assert_called_once()
        sent_data = self.strategy_sock.sendall.call_args[0][0]
        resp = deserialize_response(sent_data[2:])
        self.assertEqual(resp["clOrdID"], "ORD001")
        self.assertEqual(resp["filled_qty"], 100)

    def test_reject_forwarded(self):
        result = self.server._exec_handler.handle({
            "clOrdID": "ORD001",
            "exec_type": "8",
            "cum_qty": 0,
            "last_px": 0,
            "last_qty": 0,
            "text": "No liquidity",
        })
        self.server._forward_exec_report(result)

        self.strategy_sock.sendall.assert_called_once()


if __name__ == "__main__":
    unittest.main()
