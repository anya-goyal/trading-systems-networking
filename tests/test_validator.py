"""
Tests for IOG stateless order validation.

The validator checks field presence and sanity without consulting
any external state. It returns (True, None) or (False, reason_string).
"""

import unittest

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
from iog.validator import validate, KNOWN_SYMBOLS


def _valid_order(**overrides) -> InternalOrder:
    """Helper: build a valid order, override specific fields for testing."""
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


class TestValidOrder(unittest.TestCase):
    def test_valid_limit_buy(self):
        ok, reason = validate(_valid_order())
        self.assertTrue(ok)
        self.assertIsNone(reason)

    def test_valid_market_sell(self):
        ok, reason = validate(_valid_order(
            side=SIDE_SELL, order_type=ORD_TYPE_MARKET, price=0.0
        ))
        self.assertTrue(ok)
        self.assertIsNone(reason)

    def test_valid_cancel_request(self):
        ok, reason = validate(_valid_order(
            msg_type=MSG_TYPE_CANCEL, qty=0, price=0.0
        ))
        self.assertTrue(ok)
        self.assertIsNone(reason)


class TestMissingFields(unittest.TestCase):
    def test_empty_clOrdID(self):
        ok, reason = validate(_valid_order(clOrdID=""))
        self.assertFalse(ok)
        self.assertIn("clOrdID", reason)

    def test_empty_symbol(self):
        ok, reason = validate(_valid_order(symbol=""))
        self.assertFalse(ok)
        self.assertIn("symbol", reason)

    def test_empty_strategy_id(self):
        ok, reason = validate(_valid_order(strategy_id=""))
        self.assertFalse(ok)
        self.assertIn("strategy_id", reason)


class TestSideValidation(unittest.TestCase):
    def test_invalid_side_zero(self):
        ok, reason = validate(_valid_order(side=0))
        self.assertFalse(ok)
        self.assertIn("side", reason)

    def test_invalid_side_three(self):
        ok, reason = validate(_valid_order(side=3))
        self.assertFalse(ok)
        self.assertIn("side", reason)


class TestOrderTypeValidation(unittest.TestCase):
    def test_invalid_order_type(self):
        ok, reason = validate(_valid_order(order_type=99))
        self.assertFalse(ok)
        self.assertIn("order_type", reason)


class TestQuantityValidation(unittest.TestCase):
    def test_zero_qty_on_new_order(self):
        ok, reason = validate(_valid_order(qty=0))
        self.assertFalse(ok)
        self.assertIn("qty", reason)


class TestPriceValidation(unittest.TestCase):
    def test_limit_order_zero_price(self):
        ok, reason = validate(_valid_order(order_type=ORD_TYPE_LIMIT, price=0.0))
        self.assertFalse(ok)
        self.assertIn("price", reason)

    def test_limit_order_negative_price(self):
        ok, reason = validate(_valid_order(order_type=ORD_TYPE_LIMIT, price=-1.0))
        self.assertFalse(ok)
        self.assertIn("price", reason)

    def test_market_order_zero_price_ok(self):
        """Market orders don't need a price."""
        ok, reason = validate(_valid_order(order_type=ORD_TYPE_MARKET, price=0.0))
        self.assertTrue(ok)


class TestDestinationValidation(unittest.TestCase):
    def test_invalid_destination(self):
        ok, reason = validate(_valid_order(destination=99))
        self.assertFalse(ok)
        self.assertIn("destination", reason)

    def test_dest_exch2_valid(self):
        ok, reason = validate(_valid_order(destination=DEST_EXCH2))
        self.assertTrue(ok)


class TestSymbolValidation(unittest.TestCase):
    def test_unknown_symbol(self):
        ok, reason = validate(_valid_order(symbol="ZZZZZZ"))
        self.assertFalse(ok)
        self.assertIn("symbol", reason)

    def test_all_known_symbols_pass(self):
        for sym in KNOWN_SYMBOLS:
            ok, _ = validate(_valid_order(symbol=sym))
            self.assertTrue(ok, f"Symbol {sym} should be valid")


class TestMessageType(unittest.TestCase):
    def test_invalid_msg_type(self):
        ok, reason = validate(_valid_order(msg_type=0xFF))
        self.assertFalse(ok)
        self.assertIn("msg_type", reason)


if __name__ == "__main__":
    unittest.main()
