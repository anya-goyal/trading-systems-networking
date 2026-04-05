"""
test_strategy_slow.py

Unit tests for strategy_slow.py components:
  - OrderBook (add, remove, fill, best bid/ask, depth, spread)
  - Sequence gap detection
  - MDH packet parsing
  - Order building
  - Rate limiter
"""

import struct
import time
import unittest
from unittest.mock import patch, MagicMock

# mock config before importing strategy_slow
import sys
config_mock = MagicMock()
config_mock.SIDE_BUY = 1
config_mock.SIDE_SELL = 2
config_mock.UPDATE_NEW_ORDER = 1
config_mock.UPDATE_CANCEL = 2
config_mock.UPDATE_FILL = 3
config_mock.MIN_SPREAD_TO_TRADE = 0.10
config_mock.DEFAULT_ORDER_QTY = 100
config_mock.MAX_ORDER_RATE = 10
config_mock.SYMBOL_COOLDOWN_SEC = 1.0
config_mock.IOG_HOST = "127.0.0.1"
config_mock.IOG_PORT = 9000
config_mock.UDP_BUF_SIZE = 65536
config_mock.SUBSCRIBED_GROUPS = []
config_mock.SUBSCRIBED_ASSET_CLASSES = {1}
config_mock.SUBSCRIBED_SYMBOLS = set()
sys.modules["config"] = config_mock

import strategy_slow
from strategy_slow import (
    OrderBook,
    RateLimiter,
    MDHMessage,
    parse_mdh_packet,
    update_book,
    build_new_order,
    should_trade,
    pick_order,
    send_bytes,
    next_cl_ord_id,
    MDH_HDR_FMT,
    MDH_HDR_SIZE,
    MDH_BODY_FMT,
    MDH_BODY_SIZE,
)

# makes a mdh packet
def make_mdh_packet(
    msg_type=0x02,
    order_id=1,
    seq_no=1,
    asset_class=1,
    symbol="AAPL",
    side=1,
    update_type=1,
    price=150.0,
    qty=100,
    timestamp=1000000,
) -> bytes:
    body = struct.pack(
        MDH_BODY_FMT,
        msg_type,
        struct.pack("!Q", order_id),
        seq_no,
        asset_class,
        symbol.encode().ljust(8, b"\x00")[:8],
        side,
        update_type,
        price,
        qty,
        timestamp,
    )
    header = struct.pack(MDH_HDR_FMT, len(body))
    return header + body


# order book
class TestOrderBook(unittest.TestCase):
    def setUp(self):
        self.book = OrderBook()

    # bid and ask tests
    def test_add_single_bid(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.assertEqual(self.book.best_bid("AAPL"), 150.0)

    def test_add_single_ask(self):
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 2, 151.0, 200)
        self.assertEqual(self.book.best_ask("AAPL"), 151.0)

    def test_add_multiple_bids_best_is_highest(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 149.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 2, 150.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 3, 148.0, 100)
        self.assertEqual(self.book.best_bid("AAPL"), 150.0)

    def test_add_multiple_asks_best_is_lowest(self):
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 1, 153.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 2, 151.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 3, 152.0, 100)
        self.assertEqual(self.book.best_ask("AAPL"), 151.0)

    def test_add_multiple_orders_same_price(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 2, 150.0, 50)
        self.assertEqual(self.book.bid_depth("AAPL"), 150)

    def test_add_orders_different_symbols(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.add_order("GOOG", config_mock.SIDE_BUY, 2, 2800.0, 10)
        self.assertEqual(self.book.best_bid("AAPL"), 150.0)
        self.assertEqual(self.book.best_bid("GOOG"), 2800.0)

    # removing orders
    def test_remove_order(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 2, 150.0, 50)
        self.book.remove_order("AAPL", config_mock.SIDE_BUY, 1, 150.0)
        self.assertEqual(self.book.bid_depth("AAPL"), 50)

    def test_remove_last_order_at_price_cleans_level(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.remove_order("AAPL", config_mock.SIDE_BUY, 1, 150.0)
        self.assertEqual(self.book.best_bid("AAPL"), 0.0)

    def test_remove_best_bid_reveals_next(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 2, 149.0, 100)
        self.book.remove_order("AAPL", config_mock.SIDE_BUY, 1, 150.0)
        self.assertEqual(self.book.best_bid("AAPL"), 149.0)

    def test_remove_best_ask_reveals_next(self):
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 1, 151.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 2, 152.0, 100)
        self.book.remove_order("AAPL", config_mock.SIDE_SELL, 1, 151.0)
        self.assertEqual(self.book.best_ask("AAPL"), 152.0)

    def test_remove_nonexistent_order_no_crash(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.remove_order("AAPL", config_mock.SIDE_BUY, 999, 150.0)
        self.assertEqual(self.book.bid_depth("AAPL"), 100)

    def test_remove_nonexistent_price_no_crash(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.remove_order("AAPL", config_mock.SIDE_BUY, 1, 999.0)
        self.assertEqual(self.book.bid_depth("AAPL"), 100)

    # fill tests
    def test_partial_fill(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.fill_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 40)
        self.assertEqual(self.book.bid_depth("AAPL"), 60)

    def test_full_fill_removes_order(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.fill_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.assertEqual(self.book.best_bid("AAPL"), 0.0)

    def test_overfill_removes_order(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.fill_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 150)
        self.assertEqual(self.book.best_bid("AAPL"), 0.0)

    def test_fill_nonexistent_no_crash(self):
        self.book.fill_order("AAPL", config_mock.SIDE_BUY, 999, 150.0, 50)
        self.assertEqual(self.book.best_bid("AAPL"), 0.0)

    # multiple orders for a ticker
    def test_bid_depth_multiple_levels(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 2, 149.0, 200)
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 3, 150.0, 50)
        self.assertEqual(self.book.bid_depth("AAPL"), 350)

    def test_ask_depth_multiple_levels(self):
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 1, 151.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 2, 152.0, 200)
        self.assertEqual(self.book.ask_depth("AAPL"), 300)

    def test_depth_empty_symbol(self):
        self.assertEqual(self.book.bid_depth("AAPL"), 0)
        self.assertEqual(self.book.ask_depth("AAPL"), 0)
    
    # spread tests
    def test_spread_normal(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 2, 150.50, 100)
        self.assertAlmostEqual(self.book.spread("AAPL"), 0.50)

    def test_spread_no_bids(self):
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 1, 151.0, 100)
        self.assertEqual(self.book.spread("AAPL"), 0.0)

    def test_spread_no_asks(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.assertEqual(self.book.spread("AAPL"), 0.0)

    def test_spread_empty(self):
        self.assertEqual(self.book.spread("AAPL"), 0.0)

    def test_best_bid_unknown_symbol(self):
        self.assertEqual(self.book.best_bid("NOPE"), 0.0)

    def test_best_ask_unknown_symbol(self):
        self.assertEqual(self.book.best_ask("NOPE"), 0.0)

# update book
class TestUpdateBook(unittest.TestCase):
    def setUp(self):
        self.book = OrderBook()

    def test_new_order(self):
        msg = MDHMessage(0x02, 1, 1, 1, "AAPL", config_mock.SIDE_BUY, config_mock.UPDATE_NEW_ORDER, 150.0, 100)
        update_book(self.book, msg)
        self.assertEqual(self.book.best_bid("AAPL"), 150.0)
        self.assertEqual(self.book.bid_depth("AAPL"), 100)

    def test_cancel_order(self):
        add_msg = MDHMessage(0x02, 1, 1, 1, "AAPL", config_mock.SIDE_BUY, config_mock.UPDATE_NEW_ORDER, 150.0, 100)
        cancel_msg = MDHMessage(0x02, 1, 2, 1, "AAPL", config_mock.SIDE_BUY, config_mock.UPDATE_CANCEL, 150.0, 100)
        update_book(self.book, add_msg)
        update_book(self.book, cancel_msg)
        self.assertEqual(self.book.best_bid("AAPL"), 0.0)

    def test_fill_order(self):
        add_msg = MDHMessage(0x02, 1, 1, 1, "AAPL", config_mock.SIDE_BUY, config_mock.UPDATE_NEW_ORDER, 150.0, 100)
        fill_msg = MDHMessage(0x02, 1, 2, 1, "AAPL", config_mock.SIDE_BUY, config_mock.UPDATE_FILL, 150.0, 60)
        update_book(self.book, add_msg)
        update_book(self.book, fill_msg)
        self.assertEqual(self.book.bid_depth("AAPL"), 40)

    def test_full_sequence_add_partial_fill_cancel(self):
        # new order -> partial fill -> cancel remainder
        update_book(self.book, MDHMessage(0x02, 1, 1, 1, "AAPL", config_mock.SIDE_SELL, config_mock.UPDATE_NEW_ORDER, 151.0, 200))
        self.assertEqual(self.book.ask_depth("AAPL"), 200)

        update_book(self.book, MDHMessage(0x02, 1, 2, 1, "AAPL", config_mock.SIDE_SELL, config_mock.UPDATE_FILL, 151.0, 80))
        self.assertEqual(self.book.ask_depth("AAPL"), 120)

        update_book(self.book, MDHMessage(0x02, 1, 3, 1, "AAPL", config_mock.SIDE_SELL, config_mock.UPDATE_CANCEL, 151.0, 120))
        self.assertEqual(self.book.ask_depth("AAPL"), 0)
        self.assertEqual(self.book.best_ask("AAPL"), 0.0)


# sequence tracking
class TestSequenceTracking(unittest.TestCase):

    def test_normal_sequence(self):
        # packets 1, 2, 3 should all be processed
        last_seq = {}
        processed = []

        for seq in [1, 2, 3]:
            symbol = "AAPL"
            expected = last_seq.get(symbol, 0) + 1
            if seq < expected:
                continue  # drop
            last_seq[symbol] = seq
            processed.append(seq)

        self.assertEqual(processed, [1, 2, 3])

    def test_gap_detection(self):
        # packets 1, 2, 5; should detect gap at 3-4, still process 5
        last_seq = {}
        processed = []
        gaps = []

        for seq in [1, 2, 5]:
            symbol = "AAPL"
            expected = last_seq.get(symbol, 0) + 1
            if seq < expected:
                continue
            if seq > expected:
                gaps.append((expected, seq - 1))
            last_seq[symbol] = seq
            processed.append(seq)

        self.assertEqual(processed, [1, 2, 5])
        self.assertEqual(gaps, [(3, 4)])

    def test_out_of_order_dropped(self):
        # after processing 1, 2, 5, packet 4 should be dropped
        last_seq = {}
        processed = []

        for seq in [1, 2, 5, 4]:
            symbol = "AAPL"
            expected = last_seq.get(symbol, 0) + 1
            if seq < expected:
                continue  # out of order, drop
            last_seq[symbol] = seq
            processed.append(seq)

        self.assertEqual(processed, [1, 2, 5])

    def test_out_of_order_after_gap_dropped(self):
        # packets 1, 2, 5, 3, 4; 3 and 4 should be dropped
        last_seq = {}
        processed = []

        for seq in [1, 2, 5, 3, 4]:
            symbol = "AAPL"
            expected = last_seq.get(symbol, 0) + 1
            if seq < expected:
                continue
            last_seq[symbol] = seq
            processed.append(seq)

        self.assertEqual(processed, [1, 2, 5])

    def test_duplicate_dropped(self):
        # same packet twice should only be processed once
        last_seq = {}
        processed = []

        for seq in [1, 1, 2]:
            symbol = "AAPL"
            expected = last_seq.get(symbol, 0) + 1
            if seq < expected:
                continue
            last_seq[symbol] = seq
            processed.append(seq)

        self.assertEqual(processed, [1, 2])

    def test_independent_per_symbol(self):
        last_seq = {}
        processed = []

        messages = [("AAPL", 1), ("GOOG", 1), ("AAPL", 2), ("GOOG", 3), ("AAPL", 3)]

        gaps = []
        for symbol, seq in messages:
            expected = last_seq.get(symbol, 0) + 1
            if seq < expected:
                continue
            if seq > expected:
                gaps.append((symbol, expected, seq - 1))
            last_seq[symbol] = seq
            processed.append((symbol, seq))

        self.assertEqual(processed, [("AAPL", 1), ("GOOG", 1), ("AAPL", 2), ("GOOG", 3), ("AAPL", 3)])
        self.assertEqual(gaps, [("GOOG", 2, 2)])


# mdh packet parsing
class TestParseMDHPacket(unittest.TestCase):
    def test_parse_valid_packet(self):
        pkt = make_mdh_packet(
            msg_type=0x02, order_id=42, seq_no=7, asset_class=1,
            symbol="TSLA", side=2, update_type=1, price=250.75, qty=50,
        )
        msg = parse_mdh_packet(pkt)
        self.assertIsNotNone(msg)
        self.assertEqual(msg.msg_type, 0x02)
        self.assertEqual(msg.order_id, 42)
        self.assertEqual(msg.seq_no, 7)
        self.assertEqual(msg.asset_class, 1)
        self.assertEqual(msg.symbol, "TSLA")
        self.assertEqual(msg.side, 2)
        self.assertEqual(msg.update_type, 1)
        self.assertAlmostEqual(msg.price, 250.75)
        self.assertEqual(msg.qty, 50)

    def test_parse_symbol_padding_stripped(self):
        pkt = make_mdh_packet(symbol="FB")
        msg = parse_mdh_packet(pkt)
        self.assertEqual(msg.symbol, "FB")

    def test_parse_too_short_returns_none(self):
        self.assertIsNone(parse_mdh_packet(b""))
        self.assertIsNone(parse_mdh_packet(b"\x00"))

    def test_parse_truncated_body_returns_none(self):
        pkt = make_mdh_packet()
        truncated = pkt[:MDH_HDR_SIZE + 5]
        self.assertIsNone(parse_mdh_packet(truncated))

    def test_parse_body_len_mismatch_returns_none(self):
        bad_header = struct.pack(MDH_HDR_FMT, 2)
        body = make_mdh_packet()[MDH_HDR_SIZE:]
        self.assertIsNone(parse_mdh_packet(bad_header + body))


# order building
class TestBuildNewOrder(unittest.TestCase):
    def test_build_order_returns_bytes(self):
        cl_ord_id, raw = build_new_order("AAPL", config_mock.SIDE_BUY, 100, 150.0)
        self.assertIsInstance(raw, bytes)
        self.assertTrue(cl_ord_id.startswith("SLOW"))

    def test_build_order_correct_size(self):
        from strategy_slow import IOG_ORDER_SIZE
        _, raw = build_new_order("AAPL", config_mock.SIDE_BUY, 100, 150.0)
        self.assertEqual(len(raw), IOG_ORDER_SIZE)

    def test_cl_ord_id_increments(self):
        id1, _ = build_new_order("AAPL", config_mock.SIDE_BUY, 100, 150.0)
        id2, _ = build_new_order("AAPL", config_mock.SIDE_BUY, 100, 150.0)
        self.assertNotEqual(id1, id2)

    def test_symbol_padded_to_8_bytes(self):
        _, raw = build_new_order("FB", config_mock.SIDE_BUY, 100, 150.0)
        symbol_bytes = raw[19:27]
        self.assertEqual(symbol_bytes, b"FB\x00\x00\x00\x00\x00\x00")


# trading test
class TestTradingLogic(unittest.TestCase):
    def setUp(self):
        self.book = OrderBook()

    def test_should_trade_wide_spread(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 2, 150.50, 100)
        self.assertTrue(should_trade(self.book, "AAPL"))

    def test_should_not_trade_narrow_spread(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 2, 150.05, 100)
        self.assertFalse(should_trade(self.book, "AAPL"))

    def test_should_not_trade_no_bids(self):
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 1, 151.0, 100)
        self.assertFalse(should_trade(self.book, "AAPL"))

    def test_should_not_trade_no_asks(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.assertFalse(should_trade(self.book, "AAPL"))

    def test_should_not_trade_crossed_book(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 151.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 2, 150.0, 100)
        self.assertFalse(should_trade(self.book, "AAPL"))

    def test_should_not_trade_empty_book(self):
        self.assertFalse(should_trade(self.book, "AAPL"))

    def test_pick_order_buys_at_bid(self):
        self.book.add_order("AAPL", config_mock.SIDE_BUY, 1, 150.0, 100)
        self.book.add_order("AAPL", config_mock.SIDE_SELL, 2, 151.0, 100)
        side, price, qty = pick_order(self.book, "AAPL")
        self.assertEqual(side, config_mock.SIDE_BUY)
        self.assertEqual(price, 150.0)
        self.assertEqual(qty, config_mock.DEFAULT_ORDER_QTY)


# rate limiter
class TestRateLimiter(unittest.TestCase):
    def test_allows_up_to_max(self):
        limiter = RateLimiter(3)
        self.assertTrue(limiter.allow())
        self.assertTrue(limiter.allow())
        self.assertTrue(limiter.allow())
        self.assertFalse(limiter.allow())

    def test_resets_after_one_second(self):
        limiter = RateLimiter(2)
        self.assertTrue(limiter.allow())
        self.assertTrue(limiter.allow())
        self.assertFalse(limiter.allow())

        # time passing
        limiter._window_start = time.monotonic() - 1.1
        self.assertTrue(limiter.allow())


# send bytes
class TestSendBytes(unittest.TestCase):
    def test_send_success(self):
        mock_sock = MagicMock()
        mock_sock.send.return_value = 10
        data = b"0123456789"
        self.assertTrue(send_bytes(mock_sock, data))

    def test_send_partial_then_complete(self):
        mock_sock = MagicMock()
        mock_sock.send.side_effect = [3, 3, 4]
        data = b"0123456789"
        self.assertTrue(send_bytes(mock_sock, data))

    def test_send_os_error_returns_false(self):
        mock_sock = MagicMock()
        mock_sock.send.side_effect = OSError("connection reset")
        self.assertFalse(send_bytes(mock_sock, b"data"))

    def test_send_zero_bytes_returns_false(self):
        mock_sock = MagicMock()
        mock_sock.send.return_value = 0
        self.assertFalse(send_bytes(mock_sock, b"data"))


if __name__ == "__main__":
    unittest.main()