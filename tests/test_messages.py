"""
Tests for IOG message serialization and deserialization.

Message format (strategy → IOG):
  - 2 bytes: msg_length (uint16, big-endian)
  - 1 byte:  msg_type (0x01 = NewOrder, 0x02 = CancelRequest)
  - 16 bytes: clOrdID (utf-8, null-padded)
  - 8 bytes:  symbol (utf-8, null-padded)
  - 1 byte:  side (1=BUY, 2=SELL)
  - 1 byte:  order_type (1=MARKET, 2=LIMIT)
  - 4 bytes: qty (uint32)
  - 8 bytes: price (float64)
  - 1 byte:  destination (1=EXCH1, 2=EXCH2)
  - 8 bytes: strategy_id (utf-8, null-padded)
  Total: 2 + 48 = 50 bytes

Response format (IOG → strategy):
  - 2 bytes: msg_length (uint16)
  - 1 byte:  msg_type (0x10=Ack, 0x11=Fill, 0x12=PartialFill, 0x13=Reject, 0x14=Cancelled)
  - 16 bytes: clOrdID
  - 1 byte:  reject_reason (0=n/a, 1=validation, 2=risk, 3=exch_down, 4=exch_reject, 5=kill_switch)
  - 4 bytes: filled_qty (uint32)
  - 4 bytes: cumulative_qty (uint32)
  - 8 bytes: fill_price (float64)
  - 8 bytes: timestamp (float64)
  Total: 2 + 42 = 44 bytes
"""

import struct
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
    RESP_ACK,
    RESP_FILL,
    RESP_PARTIAL_FILL,
    RESP_REJECT,
    RESP_CANCELLED,
    REJECT_NONE,
    REJECT_VALIDATION,
    REJECT_RISK,
    REJECT_EXCH_DOWN,
    REJECT_EXCH_REJECT,
    REJECT_KILL_SWITCH,
    InternalOrder,
    deserialize_order,
    serialize_order,
    serialize_response,
    deserialize_response,
    Deserializer,
)


class TestSerializeOrder(unittest.TestCase):
    """Test that we can build a binary frame from order fields."""

    def test_serialize_limit_buy(self):
        order = InternalOrder(
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
        data = serialize_order(order)
        # 2-byte length prefix + 48 bytes body = 50
        self.assertEqual(len(data), 50)
        # length prefix should encode body length (48)
        length_prefix = struct.unpack("!H", data[:2])[0]
        self.assertEqual(length_prefix, 48)

    def test_serialize_market_sell(self):
        order = InternalOrder(
            msg_type=MSG_TYPE_NEW_ORDER,
            clOrdID="ORD002",
            symbol="MSFT",
            side=SIDE_SELL,
            order_type=ORD_TYPE_MARKET,
            qty=50,
            price=0.0,
            destination=DEST_EXCH2,
            strategy_id="FAST",
        )
        data = serialize_order(order)
        self.assertEqual(len(data), 50)


class TestDeserializeOrder(unittest.TestCase):
    """Test that we can parse a binary frame back into an InternalOrder."""

    def test_roundtrip_limit_buy(self):
        original = InternalOrder(
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
        data = serialize_order(original)
        parsed = deserialize_order(data[2:])  # strip length prefix
        self.assertEqual(parsed.clOrdID, "ORD001")
        self.assertEqual(parsed.symbol, "AAPL")
        self.assertEqual(parsed.side, SIDE_BUY)
        self.assertEqual(parsed.order_type, ORD_TYPE_LIMIT)
        self.assertEqual(parsed.qty, 100)
        self.assertAlmostEqual(parsed.price, 150.25)
        self.assertEqual(parsed.destination, DEST_EXCH1)
        self.assertEqual(parsed.strategy_id, "SLOW")

    def test_roundtrip_market_sell(self):
        original = InternalOrder(
            msg_type=MSG_TYPE_NEW_ORDER,
            clOrdID="ORD999",
            symbol="TSLA",
            side=SIDE_SELL,
            order_type=ORD_TYPE_MARKET,
            qty=200,
            price=0.0,
            destination=DEST_EXCH2,
            strategy_id="FAST",
        )
        data = serialize_order(original)
        parsed = deserialize_order(data[2:])
        self.assertEqual(parsed.clOrdID, "ORD999")
        self.assertEqual(parsed.symbol, "TSLA")
        self.assertEqual(parsed.side, SIDE_SELL)
        self.assertEqual(parsed.order_type, ORD_TYPE_MARKET)
        self.assertEqual(parsed.qty, 200)
        self.assertAlmostEqual(parsed.price, 0.0)
        self.assertEqual(parsed.destination, DEST_EXCH2)
        self.assertEqual(parsed.strategy_id, "FAST")

    def test_cancel_request(self):
        original = InternalOrder(
            msg_type=MSG_TYPE_CANCEL,
            clOrdID="ORD001",
            symbol="AAPL",
            side=SIDE_BUY,
            order_type=ORD_TYPE_LIMIT,
            qty=0,
            price=0.0,
            destination=DEST_EXCH1,
            strategy_id="SLOW",
        )
        data = serialize_order(original)
        parsed = deserialize_order(data[2:])
        self.assertEqual(parsed.msg_type, MSG_TYPE_CANCEL)
        self.assertEqual(parsed.clOrdID, "ORD001")

    def test_malformed_data_too_short(self):
        with self.assertRaises(ValueError):
            deserialize_order(b"\x00" * 10)

    def test_null_padding_stripped(self):
        """clOrdID and symbol should have null padding stripped."""
        original = InternalOrder(
            msg_type=MSG_TYPE_NEW_ORDER,
            clOrdID="A",
            symbol="X",
            side=SIDE_BUY,
            order_type=ORD_TYPE_LIMIT,
            qty=1,
            price=10.0,
            destination=DEST_EXCH1,
            strategy_id="S",
        )
        data = serialize_order(original)
        parsed = deserialize_order(data[2:])
        self.assertEqual(parsed.clOrdID, "A")
        self.assertEqual(parsed.symbol, "X")
        self.assertEqual(parsed.strategy_id, "S")


class TestSerializeResponse(unittest.TestCase):
    """Test IOG → strategy response serialization."""

    def test_reject_response(self):
        data = serialize_response(
            msg_type=RESP_REJECT,
            clOrdID="ORD001",
            reject_reason=REJECT_VALIDATION,
            filled_qty=0,
            cumulative_qty=0,
            fill_price=0.0,
            timestamp=1000000.0,
        )
        self.assertEqual(len(data), 44)
        length_prefix = struct.unpack("!H", data[:2])[0]
        self.assertEqual(length_prefix, 42)

    def test_fill_response(self):
        data = serialize_response(
            msg_type=RESP_FILL,
            clOrdID="ORD001",
            reject_reason=REJECT_NONE,
            filled_qty=100,
            cumulative_qty=100,
            fill_price=150.25,
            timestamp=1000000.0,
        )
        self.assertEqual(len(data), 44)

    def test_response_roundtrip(self):
        data = serialize_response(
            msg_type=RESP_PARTIAL_FILL,
            clOrdID="ORD005",
            reject_reason=REJECT_NONE,
            filled_qty=50,
            cumulative_qty=50,
            fill_price=99.50,
            timestamp=1234567.89,
        )
        parsed = deserialize_response(data[2:])
        self.assertEqual(parsed["msg_type"], RESP_PARTIAL_FILL)
        self.assertEqual(parsed["clOrdID"], "ORD005")
        self.assertEqual(parsed["reject_reason"], REJECT_NONE)
        self.assertEqual(parsed["filled_qty"], 50)
        self.assertEqual(parsed["cumulative_qty"], 50)
        self.assertAlmostEqual(parsed["fill_price"], 99.50)
        self.assertAlmostEqual(parsed["timestamp"], 1234567.89)


class TestDeserializer(unittest.TestCase):
    """Test the streaming deserializer that handles partial TCP reads."""

    def test_single_complete_message(self):
        order = InternalOrder(
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
        data = serialize_order(order)
        deser = Deserializer()
        deser.feed(data)
        messages = list(deser.drain())
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].clOrdID, "ORD001")

    def test_partial_reads(self):
        """Simulate TCP delivering bytes in chunks."""
        order = InternalOrder(
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
        data = serialize_order(order)
        deser = Deserializer()

        # Feed first 10 bytes — not enough for a message
        deser.feed(data[:10])
        messages = list(deser.drain())
        self.assertEqual(len(messages), 0)

        # Feed the rest
        deser.feed(data[10:])
        messages = list(deser.drain())
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].clOrdID, "ORD001")

    def test_two_messages_in_one_read(self):
        """Simulate TCP delivering two messages in a single recv()."""
        order1 = InternalOrder(
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
        order2 = InternalOrder(
            msg_type=MSG_TYPE_NEW_ORDER,
            clOrdID="ORD002",
            symbol="MSFT",
            side=SIDE_SELL,
            order_type=ORD_TYPE_MARKET,
            qty=50,
            price=0.0,
            destination=DEST_EXCH2,
            strategy_id="FAST",
        )
        data = serialize_order(order1) + serialize_order(order2)
        deser = Deserializer()
        deser.feed(data)
        messages = list(deser.drain())
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0].clOrdID, "ORD001")
        self.assertEqual(messages[1].clOrdID, "ORD002")

    def test_byte_by_byte(self):
        """Extreme case: one byte at a time."""
        order = InternalOrder(
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
        data = serialize_order(order)
        deser = Deserializer()

        for i in range(len(data) - 1):
            deser.feed(data[i : i + 1])
            messages = list(deser.drain())
            self.assertEqual(len(messages), 0)

        deser.feed(data[-1:])
        messages = list(deser.drain())
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].clOrdID, "ORD001")


if __name__ == "__main__":
    unittest.main()
