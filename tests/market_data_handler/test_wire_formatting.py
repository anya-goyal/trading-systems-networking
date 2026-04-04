"""
test_wire_formatting.py

Unit tests for market_data_handler.py covering:
  1. Compatibility with Exchange 2 raw UDP message format
  2. Wire frame binary encoding correctness (per spec)
  3. Multicast partition routing correctness
  4. Sequence tracker gap/out-of-order detection
"""

import logging
import struct
import types
import unittest
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Minimal stub for `config` so we can import mdh without the real module
# ---------------------------------------------------------------------------
config = types.ModuleType("config")
config.ASSET_EQUITIES    = 1
config.ASSET_OPTIONS     = 2
config.ASSET_FUTURES     = 3
config.SIDE_BUY          = 1
config.SIDE_SELL         = 2
config.UPDATE_NEW_ORDER  = 1
config.UPDATE_CANCEL     = 2
config.UPDATE_FILL       = 3
config.MSG_TYPE_SNAPSHOT = 0x01
config.MSG_TYPE_UPDATE   = 0x02
config.EXCHANGE1_UDP_HOST = "127.0.0.1"
config.EXCHANGE1_UDP_PORT = 9001
config.EXCHANGE2_UDP_HOST = "127.0.0.1"
config.EXCHANGE2_UDP_PORT = 9002
config.EXCHANGE2_FIX_HOST = "127.0.0.1"
config.EXCHANGE2_FIX_PORT = 9102
config.MULTICAST_PARTITIONS = [
    ("239.1.1.1", 7001),  # 0 – Equities A–F
    ("239.1.1.2", 7002),  # 1 – Equities G–M
    ("239.1.1.3", 7003),  # 2 – Equities N–Z
    ("239.1.2.1", 7004),  # 3 – Options
    ("239.1.2.2", 7005),  # 4 – Futures
]

import sys
sys.modules["config"] = config

import market_data_handler as mdh  # noqa: E402 (after stub)

# ---------------------------------------------------------------------------
# Shared constants & helpers
# ---------------------------------------------------------------------------
BODY_FMT  = "<BQQB8sBBQIQ"
HDR_FMT   = "<H"
BODY_SIZE = 48


def _make_raw(
    asset_class=1,
    seq_num=1,
    update_type="NEW_ORDER",
    symbol="AAPL",
    side=1,
    price_cents=15000,
    qty=10,
    timestamp=1_700_000_000.0,
) -> bytes:
    """Build a pipe-delimited datagram identical to what Exchange 2 sends."""
    return (
        f"{asset_class}|{seq_num}|{update_type}|{symbol}"
        f"|{side}|{price_cents}|{qty}|{timestamp}"
    ).encode()


def _decode_frame(frame: bytes) -> dict:
    """Unpack a complete length-prefixed wire frame into a plain dict."""
    hdr_size = struct.calcsize(HDR_FMT)
    (body_len,) = struct.unpack_from(HDR_FMT, frame, 0)
    assert body_len == BODY_SIZE, f"body_len mismatch: {body_len}"
    (
        msg_type,
        order_id,
        seq_num,
        asset_class,
        symbol_raw,
        side,
        update_type,
        price,
        quantity,
        timestamp,
    ) = struct.unpack_from(BODY_FMT, frame, hdr_size)
    return dict(
        body_len=body_len,
        msg_type=msg_type,
        order_id=order_id,
        seq_num=seq_num,
        asset_class=asset_class,
        symbol_raw=symbol_raw,
        side=side,
        update_type=update_type,
        price=price,
        quantity=quantity,
        timestamp=timestamp,
    )


def _fresh_tracker(label="test") -> mdh.SequenceTracker:
    return mdh.SequenceTracker(label)


# ===========================================================================
# 1. Exchange 2 message compatibility
# ===========================================================================

class TestExchange2MessageParsing(unittest.TestCase):
    """_normalize() must accept every message variant Exchange 2 can produce."""

    def setUp(self):
        self.tracker = _fresh_tracker()
        mdh._order_id_counter = 0

    # --- Happy-path for each symbol Exchange 2 uses ---

    def test_parses_aapl_equities_new_order(self):
        raw = _make_raw(asset_class=1, symbol="AAPL", update_type="NEW_ORDER",
                        side=1, price_cents=18000, qty=5, seq_num=1)
        result = mdh._normalize(raw, self.tracker)
        self.assertIsNotNone(result)
        _, asset_class, symbol_raw = result
        self.assertEqual(asset_class, config.ASSET_EQUITIES)
        self.assertTrue(symbol_raw.startswith(b"AAPL"))

    def test_parses_msft_equities_cancel(self):
        raw = _make_raw(asset_class=1, symbol="MSFT", update_type="CANCEL",
                        side=2, price_cents=25000, qty=20, seq_num=2)
        result = mdh._normalize(raw, self.tracker)
        self.assertIsNotNone(result)
        _, asset_class, symbol_raw = result
        self.assertEqual(asset_class, config.ASSET_EQUITIES)
        self.assertTrue(symbol_raw.startswith(b"MSFT"))

    def test_parses_es_futures_fill(self):
        raw = _make_raw(asset_class=3, symbol="ES", update_type="FILL",
                        side=1, price_cents=400000, qty=2, seq_num=3)
        result = mdh._normalize(raw, self.tracker)
        self.assertIsNotNone(result)
        _, asset_class, symbol_raw = result
        self.assertEqual(asset_class, config.ASSET_FUTURES)
        self.assertTrue(symbol_raw.startswith(b"ES"))

    # --- All three update types ---

    def test_all_update_types_accepted(self):
        for update_str, expected_code in [
            ("NEW_ORDER", config.UPDATE_NEW_ORDER),
            ("CANCEL",    config.UPDATE_CANCEL),
            ("FILL",      config.UPDATE_FILL),
        ]:
            with self.subTest(update_str=update_str):
                raw = _make_raw(update_type=update_str, seq_num=10)
                result = mdh._normalize(raw, _fresh_tracker())
                self.assertIsNotNone(result)
                frame, _, _ = result
                self.assertEqual(_decode_frame(frame)["update_type"], expected_code)

    # --- Both sides ---

    def test_buy_side_encoded(self):
        raw = _make_raw(side=config.SIDE_BUY, seq_num=20)
        frame, _, _ = mdh._normalize(raw, self.tracker)
        self.assertEqual(_decode_frame(frame)["side"], config.SIDE_BUY)

    def test_sell_side_encoded(self):
        raw = _make_raw(side=config.SIDE_SELL, seq_num=21)
        frame, _, _ = mdh._normalize(raw, self.tracker)
        self.assertEqual(_decode_frame(frame)["side"], config.SIDE_SELL)

    # --- Exchange 2 sends a float timestamp; MDH must truncate to int ---

    def test_float_timestamp_truncated_to_int(self):
        ts = 1_700_000_000.987654
        raw = _make_raw(timestamp=ts, seq_num=30)
        frame, _, _ = mdh._normalize(raw, self.tracker)
        self.assertEqual(_decode_frame(frame)["timestamp"], int(ts))

    # --- Bad inputs must return None, never raise ---

    def test_rejects_non_utf8(self):
        self.assertIsNone(mdh._normalize(b"\xff\xfe bad bytes", self.tracker))

    def test_rejects_too_few_fields(self):
        self.assertIsNone(mdh._normalize(b"1|1|NEW_ORDER|AAPL|1|100", self.tracker))

    def test_rejects_too_many_fields(self):
        self.assertIsNone(
            mdh._normalize(b"1|1|NEW_ORDER|AAPL|1|100|5|123.0|EXTRA", self.tracker))

    def test_rejects_unknown_update_type(self):
        self.assertIsNone(mdh._normalize(_make_raw(update_type="MODIFY"), self.tracker))

    def test_rejects_non_numeric_price(self):
        self.assertIsNone(
            mdh._normalize(b"1|1|NEW_ORDER|AAPL|1|NOPE|10|1700000000.0", self.tracker))

    def test_rejects_non_numeric_seq(self):
        self.assertIsNone(
            mdh._normalize(b"1|ABC|NEW_ORDER|AAPL|1|10000|10|1700000000.0", self.tracker))


# ===========================================================================
# 2. Wire frame binary encoding (spec compliance)
# ===========================================================================

class TestWireFrameEncoding(unittest.TestCase):
    """The produced binary frame must match the spec field-by-field."""

    def setUp(self):
        mdh._order_id_counter = 0

    def _decode(self, **kwargs) -> dict:
        raw = _make_raw(**kwargs)
        result = mdh._normalize(raw, _fresh_tracker())
        self.assertIsNotNone(result)
        frame, _, _ = result
        return _decode_frame(frame)

    # --- Length prefix (2 B) ---

    def test_length_prefix_equals_body_size(self):
        self.assertEqual(self._decode(seq_num=1)["body_len"], BODY_SIZE)

    def test_total_frame_length_is_50_bytes(self):
        raw = _make_raw(seq_num=1)
        frame, _, _ = mdh._normalize(raw, _fresh_tracker())
        self.assertEqual(len(frame), struct.calcsize(HDR_FMT) + BODY_SIZE)  # 2 + 48

    # --- Message Type (1 B) ---

    def test_message_type_is_0x02_update(self):
        self.assertEqual(self._decode(seq_num=1)["msg_type"], 0x02)

    # --- Order ID (8 B) ---

    def test_order_id_nonzero(self):
        self.assertGreater(self._decode(seq_num=1)["order_id"], 0)

    def test_order_id_increments_across_messages(self):
        mdh._order_id_counter = 0
        tracker = _fresh_tracker()
        r1, _, _ = mdh._normalize(_make_raw(seq_num=1), tracker)
        r2, _, _ = mdh._normalize(_make_raw(seq_num=2), tracker)
        self.assertLess(_decode_frame(r1)["order_id"], _decode_frame(r2)["order_id"])

    # --- Sequence Number (8 B) ---

    def test_sequence_number_small(self):
        self.assertEqual(self._decode(seq_num=1)["seq_num"], 1)

    def test_sequence_number_large(self):
        self.assertEqual(self._decode(seq_num=999_999_999)["seq_num"], 999_999_999)

    # --- Asset Class (1 B) ---

    def test_asset_class_equities(self):
        self.assertEqual(self._decode(asset_class=1)["asset_class"], 1)

    def test_asset_class_options(self):
        self.assertEqual(self._decode(asset_class=2)["asset_class"], 2)

    def test_asset_class_futures(self):
        self.assertEqual(self._decode(asset_class=3)["asset_class"], 3)

    # --- Symbol (8 B, null-padded) ---

    def test_symbol_two_chars_padded(self):
        self.assertEqual(self._decode(symbol="ES")["symbol_raw"],
                         b"ES\x00\x00\x00\x00\x00\x00")

    def test_symbol_four_chars_padded(self):
        self.assertEqual(self._decode(symbol="AAPL")["symbol_raw"],
                         b"AAPL\x00\x00\x00\x00")

    def test_symbol_exactly_eight_chars(self):
        self.assertEqual(self._decode(symbol="ABCDEFGH")["symbol_raw"],
                         b"ABCDEFGH")

    def test_symbol_truncated_at_eight_chars(self):
        sym = self._decode(symbol="TOOLONGSYM")["symbol_raw"]
        self.assertEqual(len(sym), 8)
        self.assertEqual(sym, b"TOOLONGS")

    # --- Side (1 B) ---

    def test_side_buy(self):
        self.assertEqual(self._decode(side=1)["side"], 1)

    def test_side_sell(self):
        self.assertEqual(self._decode(side=2)["side"], 2)

    # --- Update Type (1 B) ---

    def test_update_type_new_order(self):
        self.assertEqual(self._decode(update_type="NEW_ORDER")["update_type"],
                         config.UPDATE_NEW_ORDER)

    def test_update_type_cancel(self):
        self.assertEqual(self._decode(update_type="CANCEL")["update_type"],
                         config.UPDATE_CANCEL)

    def test_update_type_fill(self):
        self.assertEqual(self._decode(update_type="FILL")["update_type"],
                         config.UPDATE_FILL)

    # --- Price (8 B) ---

    def test_price_preserved(self):
        self.assertEqual(self._decode(price_cents=29999)["price"], 29999)

    def test_price_zero(self):
        self.assertEqual(self._decode(price_cents=0)["price"], 0)

    def test_price_large(self):
        self.assertEqual(self._decode(price_cents=10_000_000)["price"], 10_000_000)

    # --- Quantity (4 B) ---

    def test_quantity_preserved(self):
        self.assertEqual(self._decode(qty=77)["quantity"], 77)

    def test_quantity_one(self):
        self.assertEqual(self._decode(qty=1)["quantity"], 1)

    # --- Timestamp (8 B) ---

    def test_timestamp_integer_value(self):
        self.assertEqual(self._decode(timestamp=1_700_000_000.0)["timestamp"],
                         1_700_000_000)

    def test_timestamp_float_truncated(self):
        self.assertEqual(self._decode(timestamp=1_700_000_000.999)["timestamp"],
                         1_700_000_000)

    # --- Little-endian byte order ---

    def test_little_endian_seq_num_bytes(self):
        """Seq-num field sits at frame[11:19]; verify LE layout directly."""
        raw = _make_raw(seq_num=1)
        frame, _, _ = mdh._normalize(raw, _fresh_tracker())
        # offset: HDR(2) + msg_type(1) + order_id(8) = 11
        seq_bytes = frame[11:19]
        self.assertEqual(struct.unpack("<Q", seq_bytes)[0], 1)

    def test_little_endian_price_not_big_endian(self):
        price = 0x0102030405060708
        raw = _make_raw(price_cents=price)
        frame, _, _ = mdh._normalize(raw, _fresh_tracker())
        decoded = _decode_frame(frame)
        # If accidentally big-endian the value would be byte-reversed
        self.assertEqual(decoded["price"], price)


# ===========================================================================
# 3. Multicast partition routing
# ===========================================================================

class TestPartitionRouting(unittest.TestCase):
    """_partition_for() must map every asset/symbol to the correct partition,
    and _publish() must send to the matching multicast address."""

    # --- _partition_for() ---

    def test_equities_a_to_f_is_partition_0(self):
        for sym in [b"AAPL\x00\x00\x00\x00", b"AMZN\x00\x00\x00\x00",
                    b"F\x00\x00\x00\x00\x00\x00\x00"]:
            with self.subTest(sym=sym):
                self.assertEqual(mdh._partition_for(config.ASSET_EQUITIES, sym), 0)

    def test_equities_g_to_m_is_partition_1(self):
        for sym in [b"GOOG\x00\x00\x00\x00", b"IBM\x00\x00\x00\x00\x00",
                    b"META\x00\x00\x00\x00", b"MSFT\x00\x00\x00\x00"]:
            with self.subTest(sym=sym):
                self.assertEqual(mdh._partition_for(config.ASSET_EQUITIES, sym), 1)

    def test_equities_n_to_z_is_partition_2(self):
        for sym in [b"NFLX\x00\x00\x00\x00", b"TSLA\x00\x00\x00\x00",
                    b"NVDA\x00\x00\x00\x00", b"ZM\x00\x00\x00\x00\x00\x00"]:
            with self.subTest(sym=sym):
                self.assertEqual(mdh._partition_for(config.ASSET_EQUITIES, sym), 2)

    def test_options_always_partition_3(self):
        for sym in [b"AAPL\x00\x00\x00\x00", b"TSLA\x00\x00\x00\x00"]:
            with self.subTest(sym=sym):
                self.assertEqual(mdh._partition_for(config.ASSET_OPTIONS, sym), 3)

    def test_futures_always_partition_4(self):
        for sym in [b"ES\x00\x00\x00\x00\x00\x00", b"NQ\x00\x00\x00\x00\x00\x00"]:
            with self.subTest(sym=sym):
                self.assertEqual(mdh._partition_for(config.ASSET_FUTURES, sym), 4)

    # Exact boundary letters

    def test_boundary_a_partition_0(self):
        self.assertEqual(
            mdh._partition_for(config.ASSET_EQUITIES, b"A\x00\x00\x00\x00\x00\x00\x00"), 0)

    def test_boundary_f_partition_0(self):
        self.assertEqual(
            mdh._partition_for(config.ASSET_EQUITIES, b"F\x00\x00\x00\x00\x00\x00\x00"), 0)

    def test_boundary_g_partition_1(self):
        self.assertEqual(
            mdh._partition_for(config.ASSET_EQUITIES, b"G\x00\x00\x00\x00\x00\x00\x00"), 1)

    def test_boundary_m_partition_1(self):
        self.assertEqual(
            mdh._partition_for(config.ASSET_EQUITIES, b"M\x00\x00\x00\x00\x00\x00\x00"), 1)

    def test_boundary_n_partition_2(self):
        self.assertEqual(
            mdh._partition_for(config.ASSET_EQUITIES, b"N\x00\x00\x00\x00\x00\x00\x00"), 2)

    def test_boundary_z_partition_2(self):
        self.assertEqual(
            mdh._partition_for(config.ASSET_EQUITIES, b"Z\x00\x00\x00\x00\x00\x00\x00"), 2)

    # --- _publish() sends only to the right socket with the right address ---

    def _mocks(self, n=5):
        return [MagicMock(spec=["sendto"]) for _ in range(n)]

    def test_publish_equities_af_address(self):
        socks = self._mocks()
        mdh._publish(socks, 0, b"frame")
        socks[0].sendto.assert_called_once_with(b"frame", ("239.1.1.1", 7001))
        for i in range(1, 5):
            socks[i].sendto.assert_not_called()

    def test_publish_equities_gm_address(self):
        socks = self._mocks()
        mdh._publish(socks, 1, b"frame")
        socks[1].sendto.assert_called_once_with(b"frame", ("239.1.1.2", 7002))
        for i in [0, 2, 3, 4]:
            socks[i].sendto.assert_not_called()

    def test_publish_equities_nz_address(self):
        socks = self._mocks()
        mdh._publish(socks, 2, b"frame")
        socks[2].sendto.assert_called_once_with(b"frame", ("239.1.1.3", 7003))

    def test_publish_options_address(self):
        socks = self._mocks()
        mdh._publish(socks, 3, b"frame")
        socks[3].sendto.assert_called_once_with(b"frame", ("239.1.2.1", 7004))

    def test_publish_futures_address(self):
        socks = self._mocks()
        mdh._publish(socks, 4, b"frame")
        socks[4].sendto.assert_called_once_with(b"frame", ("239.1.2.2", 7005))

    def test_publish_does_not_send_to_wrong_partitions(self):
        socks = self._mocks()
        mdh._publish(socks, 3, b"data")
        for i in [0, 1, 2, 4]:
            socks[i].sendto.assert_not_called()

    # --- End-to-end: normalize → route → publish ---

    def _e2e(self, asset_class, symbol, expected_partition, expected_dest):
        socks = self._mocks()
        raw = _make_raw(asset_class=asset_class, symbol=symbol, seq_num=1)
        result = mdh._normalize(raw, _fresh_tracker())
        self.assertIsNotNone(result)
        frame, ac, sym_raw = result
        partition = mdh._partition_for(ac, sym_raw)
        self.assertEqual(partition, expected_partition,
                         msg=f"symbol={symbol} routed to partition {partition}, expected {expected_partition}")
        mdh._publish(socks, partition, frame)
        socks[expected_partition].sendto.assert_called_once_with(frame, expected_dest)

    def test_e2e_aapl_equities_af(self):
        self._e2e(config.ASSET_EQUITIES, "AAPL", 0, ("239.1.1.1", 7001))

    def test_e2e_msft_equities_gm(self):
        self._e2e(config.ASSET_EQUITIES, "MSFT", 1, ("239.1.1.2", 7002))

    def test_e2e_nvda_equities_nz(self):
        self._e2e(config.ASSET_EQUITIES, "NVDA", 2, ("239.1.1.3", 7003))

    def test_e2e_es_futures(self):
        self._e2e(config.ASSET_FUTURES, "ES", 4, ("239.1.2.2", 7005))


# ===========================================================================
# 4. Sequence tracker gap / out-of-order detection
# ===========================================================================

class TestSequenceTracker(unittest.TestCase):

    def test_in_order_no_warnings(self):
        tracker = _fresh_tracker()
        tracker.check(1)
        with self.assertLogs("mdh", level="WARNING") as cm:
            logging.getLogger("mdh").warning("sentinel")
            tracker.check(2)
            tracker.check(3)
        # Only the sentinel should be present
        self.assertFalse(any("gap" in line or "out-of-order" in line
                             for line in cm.output))

    def test_gap_logs_warning(self):
        tracker = _fresh_tracker()
        tracker.check(1)
        with self.assertLogs("mdh", level="WARNING") as cm:
            tracker.check(5)  # gap of 4
        self.assertTrue(any("gap" in line for line in cm.output))

    def test_out_of_order_logs_warning(self):
        tracker = _fresh_tracker()
        tracker.check(10)
        with self.assertLogs("mdh", level="WARNING") as cm:
            tracker.check(7)
        self.assertTrue(any("out-of-order" in line for line in cm.output))

    def test_expected_advances_after_gap(self):
        """After a gap, the tracker expects seq+1 of the received packet."""
        tracker = _fresh_tracker()
        tracker.check(1)
        with self.assertLogs("mdh", level="WARNING"):
            tracker.check(10)          # gap; no warning on next in-order
        with self.assertLogs("mdh", level="WARNING") as cm:
            logging.getLogger("mdh").warning("sentinel")
            tracker.check(11)
        self.assertFalse(any("gap" in line or "out-of-order" in line
                             for line in cm.output))

    def test_first_message_sets_baseline(self):
        """First packet should not produce a warning regardless of seq value."""
        tracker = _fresh_tracker()
        with self.assertLogs("mdh", level="WARNING") as cm:
            logging.getLogger("mdh").warning("sentinel")
            tracker.check(999)
        self.assertFalse(any("gap" in line or "out-of-order" in line
                             for line in cm.output))


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)   # suppress mdh log noise during runs
    unittest.main(verbosity=2)