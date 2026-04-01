"""
Tests for IOG stateful risk checking.

The risk checker maintains positions, rate counters, and a kill switch.
It checks orders against configurable limits.
"""

import time
import unittest

from iog.messages import (
    MSG_TYPE_NEW_ORDER,
    SIDE_BUY,
    SIDE_SELL,
    ORD_TYPE_LIMIT,
    DEST_EXCH1,
    InternalOrder,
)
from iog.risk import RiskChecker


def _order(**overrides) -> InternalOrder:
    defaults = dict(
        msg_type=MSG_TYPE_NEW_ORDER,
        clOrdID="ORD001",
        symbol="AAPL",
        side=SIDE_BUY,
        order_type=ORD_TYPE_LIMIT,
        qty=100,
        price=150.0,
        destination=DEST_EXCH1,
        strategy_id="SLOW",
    )
    defaults.update(overrides)
    return InternalOrder(**defaults)


class TestRiskCheckerPassingOrders(unittest.TestCase):
    def setUp(self):
        self.risk = RiskChecker(
            max_order_qty=1000,
            max_notional=1_000_000.0,
            max_position_per_symbol=5000,
            max_orders_per_sec=100,
        )

    def test_normal_order_passes(self):
        ok, reason = self.risk.check(_order())
        self.assertTrue(ok)
        self.assertIsNone(reason)

    def test_sell_order_passes(self):
        ok, reason = self.risk.check(_order(side=SIDE_SELL))
        self.assertTrue(ok)


class TestMaxOrderQty(unittest.TestCase):
    def setUp(self):
        self.risk = RiskChecker(
            max_order_qty=100,
            max_notional=1_000_000.0,
            max_position_per_symbol=5000,
            max_orders_per_sec=100,
        )

    def test_order_at_limit_passes(self):
        ok, _ = self.risk.check(_order(qty=100))
        self.assertTrue(ok)

    def test_order_over_limit_rejected(self):
        ok, reason = self.risk.check(_order(qty=101))
        self.assertFalse(ok)
        self.assertIn("max_order_qty", reason)


class TestMaxNotional(unittest.TestCase):
    def setUp(self):
        self.risk = RiskChecker(
            max_order_qty=10000,
            max_notional=10_000.0,
            max_position_per_symbol=50000,
            max_orders_per_sec=100,
        )

    def test_notional_at_limit_passes(self):
        # 100 * 100.0 = 10,000
        ok, _ = self.risk.check(_order(qty=100, price=100.0))
        self.assertTrue(ok)

    def test_notional_over_limit_rejected(self):
        # 100 * 150.0 = 15,000 > 10,000
        ok, reason = self.risk.check(_order(qty=100, price=150.0))
        self.assertFalse(ok)
        self.assertIn("max_notional", reason)


class TestPositionLimit(unittest.TestCase):
    def setUp(self):
        self.risk = RiskChecker(
            max_order_qty=10000,
            max_notional=10_000_000.0,
            max_position_per_symbol=500,
            max_orders_per_sec=100,
        )

    def test_buy_within_limit(self):
        ok, _ = self.risk.check(_order(qty=500))
        self.assertTrue(ok)

    def test_buy_exceeds_limit(self):
        # Simulate existing long position of 400
        self.risk.update_position("AAPL", SIDE_BUY, 400)
        ok, reason = self.risk.check(_order(qty=200))
        self.assertFalse(ok)
        self.assertIn("position", reason)

    def test_sell_builds_short_position(self):
        ok, _ = self.risk.check(_order(side=SIDE_SELL, qty=500))
        self.assertTrue(ok)

    def test_sell_exceeds_short_limit(self):
        self.risk.update_position("AAPL", SIDE_SELL, 400)
        ok, reason = self.risk.check(_order(side=SIDE_SELL, qty=200))
        self.assertFalse(ok)
        self.assertIn("position", reason)

    def test_sell_reduces_long_position(self):
        """Selling when long should reduce position, not increase it."""
        self.risk.update_position("AAPL", SIDE_BUY, 400)
        # Selling 200 from +400 → +200, well within limit
        ok, _ = self.risk.check(_order(side=SIDE_SELL, qty=200))
        self.assertTrue(ok)

    def test_positions_tracked_per_symbol(self):
        """AAPL position should not affect MSFT."""
        self.risk.update_position("AAPL", SIDE_BUY, 500)
        ok, _ = self.risk.check(_order(symbol="MSFT", qty=500))
        self.assertTrue(ok)


class TestRateLimit(unittest.TestCase):
    def setUp(self):
        self.risk = RiskChecker(
            max_order_qty=10000,
            max_notional=10_000_000.0,
            max_position_per_symbol=50000,
            max_orders_per_sec=5,
        )

    def test_within_rate_limit(self):
        for i in range(5):
            ok, _ = self.risk.check(_order(clOrdID=f"ORD{i}"))
            self.assertTrue(ok)

    def test_exceeds_rate_limit(self):
        for i in range(5):
            self.risk.check(_order(clOrdID=f"ORD{i}"))
        ok, reason = self.risk.check(_order(clOrdID="ORD_OVER"))
        self.assertFalse(ok)
        self.assertIn("rate", reason)

    def test_rate_limit_per_strategy(self):
        """Different strategies have independent rate limits."""
        for i in range(5):
            self.risk.check(_order(clOrdID=f"ORD{i}", strategy_id="SLOW"))
        # FAST strategy should still have capacity
        ok, _ = self.risk.check(_order(clOrdID="ORDX", strategy_id="FAST"))
        self.assertTrue(ok)


class TestKillSwitch(unittest.TestCase):
    def setUp(self):
        self.risk = RiskChecker(
            max_order_qty=10000,
            max_notional=10_000_000.0,
            max_position_per_symbol=50000,
            max_orders_per_sec=100,
        )

    def test_kill_switch_off_by_default(self):
        ok, _ = self.risk.check(_order())
        self.assertTrue(ok)

    def test_kill_switch_rejects_all(self):
        self.risk.activate_kill_switch()
        ok, reason = self.risk.check(_order())
        self.assertFalse(ok)
        self.assertIn("kill_switch", reason)

    def test_kill_switch_reset(self):
        self.risk.activate_kill_switch()
        self.risk.reset_kill_switch()
        ok, _ = self.risk.check(_order())
        self.assertTrue(ok)


class TestUpdatePosition(unittest.TestCase):
    def setUp(self):
        self.risk = RiskChecker(
            max_order_qty=10000,
            max_notional=10_000_000.0,
            max_position_per_symbol=50000,
            max_orders_per_sec=100,
        )

    def test_buy_increases_position(self):
        self.risk.update_position("AAPL", SIDE_BUY, 100)
        self.assertEqual(self.risk.positions["AAPL"], 100)

    def test_sell_decreases_position(self):
        self.risk.update_position("AAPL", SIDE_SELL, 100)
        self.assertEqual(self.risk.positions["AAPL"], -100)

    def test_multiple_updates(self):
        self.risk.update_position("AAPL", SIDE_BUY, 100)
        self.risk.update_position("AAPL", SIDE_BUY, 50)
        self.risk.update_position("AAPL", SIDE_SELL, 30)
        self.assertEqual(self.risk.positions["AAPL"], 120)


if __name__ == "__main__":
    unittest.main()
