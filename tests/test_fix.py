"""
Tests for FIX 4.2 serialization and parsing.

FIX messages are tag=value pairs delimited by SOH (0x01).
Tag 10 (CheckSum) is the sum of all preceding bytes mod 256, zero-padded to 3 digits.
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
from iog.fix import (
    SOH,
    serialize_new_order,
    serialize_cancel_request,
    parse_execution_report,
    compute_checksum,
)


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


class TestChecksum(unittest.TestCase):
    def test_known_checksum(self):
        """Checksum is sum of all bytes mod 256, zero-padded to 3 chars."""
        data = b"8=FIX.4.2\x0135=D\x01"
        cs = compute_checksum(data)
        expected = sum(data) % 256
        self.assertEqual(cs, f"{expected:03d}")

    def test_checksum_is_3_digits(self):
        cs = compute_checksum(b"\x01")
        self.assertEqual(len(cs), 3)


class TestSerializeNewOrder(unittest.TestCase):
    def test_contains_begin_string(self):
        msg = serialize_new_order(_order())
        self.assertIn(b"8=FIX.4.2" + SOH, msg)

    def test_msg_type_is_D(self):
        msg = serialize_new_order(_order())
        self.assertIn(b"35=D" + SOH, msg)

    def test_sender_comp_id(self):
        msg = serialize_new_order(_order())
        self.assertIn(b"49=IOG" + SOH, msg)

    def test_target_comp_id_exch1(self):
        msg = serialize_new_order(_order(destination=DEST_EXCH1))
        self.assertIn(b"56=EXCH1" + SOH, msg)

    def test_target_comp_id_exch2(self):
        msg = serialize_new_order(_order(destination=DEST_EXCH2))
        self.assertIn(b"56=EXCH2" + SOH, msg)

    def test_clordid(self):
        msg = serialize_new_order(_order(clOrdID="MY_ORDER_42"))
        self.assertIn(b"11=MY_ORDER_42" + SOH, msg)

    def test_symbol(self):
        msg = serialize_new_order(_order(symbol="TSLA"))
        self.assertIn(b"55=TSLA" + SOH, msg)

    def test_side_buy(self):
        msg = serialize_new_order(_order(side=SIDE_BUY))
        self.assertIn(b"54=1" + SOH, msg)

    def test_side_sell(self):
        msg = serialize_new_order(_order(side=SIDE_SELL))
        self.assertIn(b"54=2" + SOH, msg)

    def test_ord_type_limit(self):
        msg = serialize_new_order(_order(order_type=ORD_TYPE_LIMIT))
        self.assertIn(b"40=2" + SOH, msg)

    def test_ord_type_market(self):
        msg = serialize_new_order(_order(order_type=ORD_TYPE_MARKET))
        self.assertIn(b"40=1" + SOH, msg)

    def test_order_qty(self):
        msg = serialize_new_order(_order(qty=500))
        self.assertIn(b"38=500" + SOH, msg)

    def test_price(self):
        msg = serialize_new_order(_order(price=99.5))
        self.assertIn(b"44=99.5" + SOH, msg)

    def test_ends_with_checksum(self):
        msg = serialize_new_order(_order())
        # Last field should be 10=XXX followed by SOH
        self.assertTrue(msg.endswith(SOH))
        fields = msg.split(SOH)
        # Last element is empty (after trailing SOH), second-to-last is checksum
        last_field = fields[-2]
        self.assertTrue(last_field.startswith(b"10="))

    def test_checksum_is_valid(self):
        msg = serialize_new_order(_order())
        # Split off checksum field
        parts = msg.split(b"10=")
        before_checksum = parts[0]
        stated_cs = parts[1].rstrip(SOH).decode("ascii")
        expected_cs = compute_checksum(before_checksum)
        self.assertEqual(stated_cs, expected_cs)


class TestSerializeCancelRequest(unittest.TestCase):
    def test_msg_type_is_F(self):
        msg = serialize_cancel_request("ORD001", "AAPL", SIDE_BUY, DEST_EXCH1)
        self.assertIn(b"35=F" + SOH, msg)

    def test_contains_clordid(self):
        msg = serialize_cancel_request("ORD999", "AAPL", SIDE_BUY, DEST_EXCH1)
        self.assertIn(b"11=ORD999" + SOH, msg)

    def test_contains_symbol(self):
        msg = serialize_cancel_request("ORD001", "MSFT", SIDE_BUY, DEST_EXCH1)
        self.assertIn(b"55=MSFT" + SOH, msg)

    def test_target_exch2(self):
        msg = serialize_cancel_request("ORD001", "AAPL", SIDE_BUY, DEST_EXCH2)
        self.assertIn(b"56=EXCH2" + SOH, msg)

    def test_has_valid_checksum(self):
        msg = serialize_cancel_request("ORD001", "AAPL", SIDE_BUY, DEST_EXCH1)
        parts = msg.split(b"10=")
        before_checksum = parts[0]
        stated_cs = parts[1].rstrip(SOH).decode("ascii")
        self.assertEqual(stated_cs, compute_checksum(before_checksum))


class TestParseExecutionReport(unittest.TestCase):
    def _build_exec_report(self, **overrides):
        """Build a raw FIX execution report for testing."""
        fields = {
            "8": "FIX.4.2",
            "35": "8",
            "49": "EXCH1",
            "56": "IOG",
            "11": "ORD001",
            "150": "2",      # Fill
            "14": "100",     # CumQty
            "31": "150.25",  # LastPx
            "32": "100",     # LastQty
        }
        fields.update(overrides)
        body = SOH.join(
            f"{k}={v}".encode("ascii") for k, v in fields.items()
        ) + SOH
        cs = compute_checksum(body)
        return body + f"10={cs}".encode("ascii") + SOH

    def test_parse_fill(self):
        msg = self._build_exec_report()
        report = parse_execution_report(msg)
        self.assertEqual(report["clOrdID"], "ORD001")
        self.assertEqual(report["exec_type"], "2")
        self.assertEqual(report["cum_qty"], 100)
        self.assertAlmostEqual(report["last_px"], 150.25)
        self.assertEqual(report["last_qty"], 100)

    def test_parse_reject(self):
        msg = self._build_exec_report(**{
            "150": "8",
            "14": "0",
            "31": "0",
            "32": "0",
            "58": "Unknown symbol",
        })
        report = parse_execution_report(msg)
        self.assertEqual(report["exec_type"], "8")
        self.assertEqual(report["text"], "Unknown symbol")

    def test_parse_partial_fill(self):
        msg = self._build_exec_report(**{
            "150": "1",
            "14": "50",
            "31": "149.75",
            "32": "50",
        })
        report = parse_execution_report(msg)
        self.assertEqual(report["exec_type"], "1")
        self.assertEqual(report["cum_qty"], 50)
        self.assertEqual(report["last_qty"], 50)

    def test_parse_ack(self):
        msg = self._build_exec_report(**{
            "150": "0",
            "14": "0",
            "31": "0",
            "32": "0",
        })
        report = parse_execution_report(msg)
        self.assertEqual(report["exec_type"], "0")

    def test_missing_clordid_raises(self):
        """Execution report without ClOrdID should raise."""
        fields = {
            "8": "FIX.4.2",
            "35": "8",
            "150": "2",
        }
        body = SOH.join(
            f"{k}={v}".encode("ascii") for k, v in fields.items()
        ) + SOH
        cs = compute_checksum(body)
        msg = body + f"10={cs}".encode("ascii") + SOH
        with self.assertRaises(ValueError):
            parse_execution_report(msg)


if __name__ == "__main__":
    unittest.main()
