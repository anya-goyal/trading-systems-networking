"""
End-to-end integration: strategy client (internal binary) <-> IOG <-> Exchange 1 engine.

Uses real TCP, real FIX framing, and test-scoped ports (see test_threaded_iog_strategy).
"""

from __future__ import annotations

import os
import socket
import struct
import sys
import threading
import time
import unittest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import config
from iog.messages import (
    MSG_TYPE_NEW_ORDER,
    MSG_TYPE_CANCEL,
    ORD_TYPE_LIMIT,
    SIDE_BUY,
    SIDE_SELL,
    DEST_EXCH1,
    InternalOrder,
    LENGTH_PREFIX_FMT,
    LENGTH_PREFIX_SIZE,
    RESP_ACK,
    RESP_CANCELLED,
    RESP_FILL,
    RESP_PARTIAL_FILL,
    RESP_REJECT,
    REJECT_EXCH_REJECT,
    deserialize_response,
    serialize_order,
)
from _exchange1_fix_harness import Exchange1FixHarness, StubExchangeTcpDrain

TEST_HOST = "127.0.0.1"
TEST_IOG_PORT = config.IOG_PORT + 1000
TEST_EXCH1_PORT = config.EXCHANGE1_FIX_PORT + 1000
TEST_EXCH2_PORT = config.EXCHANGE2_FIX_PORT + 1000


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(f"socket closed after {len(buf)} of {n} bytes")
        buf += chunk
    return bytes(buf)


def recv_internal_response(sock: socket.socket) -> dict:
    hdr = _recv_exact(sock, LENGTH_PREFIX_SIZE)
    (body_len,) = struct.unpack(LENGTH_PREFIX_FMT, hdr)
    body = _recv_exact(sock, body_len)
    return deserialize_response(body)


def _make_internal_order(**overrides) -> InternalOrder:
    d = dict(
        msg_type=MSG_TYPE_NEW_ORDER,
        clOrdID="O1",
        symbol="AAPL",
        side=SIDE_BUY,
        order_type=ORD_TYPE_LIMIT,
        qty=10,
        price=150.25,
        destination=DEST_EXCH1,
        strategy_id="INTTEST",
    )
    d.update(overrides)
    return InternalOrder(**d)


def _drain_udp(sock: socket.socket) -> None:
    sock.settimeout(0.05)
    try:
        while True:
            sock.recv(65536)
    except socket.timeout:
        pass
    except OSError:
        pass


class IOGExchange1Fixture:
    """Class-level IOG + Exchange1 harness + EXCH2 stub. Subclasses set engine_symbols."""

    engine_symbols: frozenset[str] | set[str] | None = None

    @classmethod
    def setUpClass(cls):
        cls._pitch_rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cls._pitch_rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        cls._pitch_rx.bind((TEST_HOST, 0))
        _, pitch_port = cls._pitch_rx.getsockname()

        cls._orig = {
            "IOG_PORT": config.IOG_PORT,
            "EXCHANGE1_FIX_PORT": config.EXCHANGE1_FIX_PORT,
            "EXCHANGE2_FIX_PORT": config.EXCHANGE2_FIX_PORT,
            "EXCHANGE1_UDP_HOST": config.EXCHANGE1_UDP_HOST,
            "EXCHANGE1_UDP_PORT": config.EXCHANGE1_UDP_PORT,
        }
        config.IOG_PORT = TEST_IOG_PORT
        config.EXCHANGE1_FIX_PORT = TEST_EXCH1_PORT
        config.EXCHANGE2_FIX_PORT = TEST_EXCH2_PORT
        config.EXCHANGE1_UDP_HOST = TEST_HOST
        config.EXCHANGE1_UDP_PORT = pitch_port

        syms = cls.engine_symbols
        cls.ex1 = Exchange1FixHarness(
            TEST_HOST,
            TEST_EXCH1_PORT,
            allowed_symbols=syms,
        )
        cls.ex1.start()
        cls.stub2 = StubExchangeTcpDrain(TEST_HOST, TEST_EXCH2_PORT)
        cls.stub2.start()
        time.sleep(0.25)

        from internal_order_gateway import IOGServer

        cls.server = IOGServer()
        cls.iog_thread = threading.Thread(target=cls.server.run, daemon=True)
        cls.iog_thread.start()
        time.sleep(0.7)

        _drain_udp(cls._pitch_rx)

    @classmethod
    def tearDownClass(cls):
        cls.server._shutdown_requested = True
        cls.iog_thread.join(timeout=10.0)
        cls.ex1.stop()
        cls.stub2.stop()
        try:
            cls._pitch_rx.close()
        except OSError:
            pass
        for key, val in cls._orig.items():
            setattr(config, key, val)


class TestIOGExchange1Integration(IOGExchange1Fixture, unittest.TestCase):
    """Default ExchangeEngine symbols (AAPL, MSFT, TSLA, GOOG)."""

    engine_symbols = None

    def setUp(self):
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect((TEST_HOST, TEST_IOG_PORT))
        self.client.settimeout(20.0)

    def tearDown(self):
        try:
            self.client.close()
        except OSError:
            pass

    def test_limit_order_rests_client_gets_ack(self):
        o = _make_internal_order(clOrdID="REST-01", symbol="AAPL", side=SIDE_SELL, qty=5, price=200.0)
        self.client.sendall(serialize_order(o))
        r = recv_internal_response(self.client)
        self.assertEqual(r["msg_type"], RESP_ACK)
        self.assertEqual(r["clOrdID"], "REST-01")

    def test_full_match_two_limits(self):
        self.client.sendall(
            serialize_order(
                _make_internal_order(
                    clOrdID="MK-SELL",
                    symbol="GOOG",
                    side=SIDE_SELL,
                    qty=10,
                    price=175.0,
                )
            )
        )
        r1 = recv_internal_response(self.client)
        self.assertEqual(r1["msg_type"], RESP_ACK, r1)

        self.client.sendall(
            serialize_order(
                _make_internal_order(
                    clOrdID="MK-BUY",
                    symbol="GOOG",
                    side=SIDE_BUY,
                    qty=10,
                    price=175.0,
                )
            )
        )
        r2 = recv_internal_response(self.client)
        self.assertEqual(r2["msg_type"], RESP_ACK, r2)

        by_id: dict[str, list[dict]] = {}
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            try:
                self.client.settimeout(max(0.1, deadline - time.monotonic()))
                r = recv_internal_response(self.client)
            except socket.timeout:
                break
            cid = r["clOrdID"]
            by_id.setdefault(cid, []).append(r)

        self.assertIn("MK-SELL", by_id)
        self.assertIn("MK-BUY", by_id)
        sell_types = [x["msg_type"] for x in by_id["MK-SELL"]]
        buy_types = [x["msg_type"] for x in by_id["MK-BUY"]]
        self.assertIn(RESP_FILL, sell_types, f"sell reports: {by_id['MK-SELL']}")
        self.assertIn(RESP_FILL, buy_types, f"buy reports: {by_id['MK-BUY']}")

        fill_buy = next(x for x in by_id["MK-BUY"] if x["msg_type"] == RESP_FILL)
        self.assertEqual(fill_buy["cumulative_qty"], 10)
        self.assertAlmostEqual(fill_buy["fill_price"], 175.0, places=4)

    def test_cancel_resting_order_gets_cancelled(self):
        self.client.sendall(
            serialize_order(
                _make_internal_order(
                    clOrdID="CAN-01",
                    symbol="TSLA",
                    side=SIDE_BUY,
                    qty=7,
                    price=250.0,
                )
            )
        )
        ack = recv_internal_response(self.client)
        self.assertEqual(ack["msg_type"], RESP_ACK)

        cancel = InternalOrder(
            msg_type=MSG_TYPE_CANCEL,
            clOrdID="CAN-01",
            symbol="TSLA",
            side=SIDE_BUY,
            order_type=ORD_TYPE_LIMIT,
            qty=0,
            price=0.0,
            destination=DEST_EXCH1,
            strategy_id="INTTEST",
        )
        self.client.sendall(serialize_order(cancel))

        deadline = time.monotonic() + 8.0
        got_cancel = False
        while time.monotonic() < deadline:
            self.client.settimeout(max(0.1, deadline - time.monotonic()))
            try:
                r = recv_internal_response(self.client)
            except socket.timeout:
                break
            if r["clOrdID"] == "CAN-01" and r["msg_type"] == RESP_CANCELLED:
                got_cancel = True
                break
        self.assertTrue(got_cancel, "expected RESP_CANCELLED for user cancel")

    def test_pitch_add_after_resting_limit(self):
        _drain_udp(self._pitch_rx)
        self.client.sendall(
            serialize_order(
                _make_internal_order(
                    clOrdID="PITCH-1",
                    symbol="MSFT",
                    side=SIDE_SELL,
                    qty=3,
                    price=310.0,
                )
            )
        )
        ack = recv_internal_response(self.client)
        self.assertEqual(ack["msg_type"], RESP_ACK)

        self._pitch_rx.settimeout(3.0)
        data, _ = self._pitch_rx.recvfrom(4096)
        line = data.decode("utf-8")
        parts = line.split("|")
        self.assertGreaterEqual(len(parts), 4, line)
        self.assertEqual(parts[2], "A", f"expected Add order PITCH, got: {line!r}")


class TestIOGExchange1RejectUnknownSymbol(IOGExchange1Fixture, unittest.TestCase):
    """Engine only allows AAPL; MSFT passes IOG validator but exchange rejects."""

    engine_symbols = frozenset({"AAPL"})

    def setUp(self):
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect((TEST_HOST, TEST_IOG_PORT))
        self.client.settimeout(20.0)

    def tearDown(self):
        try:
            self.client.close()
        except OSError:
            pass

    def test_exchange_unknown_symbol_reject(self):
        o = _make_internal_order(clOrdID="BADSYM", symbol="MSFT", qty=1, price=100.0)
        self.client.sendall(serialize_order(o))
        r = recv_internal_response(self.client)
        self.assertEqual(r["msg_type"], RESP_REJECT)
        self.assertEqual(r["reject_reason"], REJECT_EXCH_REJECT)
        self.assertEqual(r["clOrdID"], "BADSYM")


if __name__ == "__main__":
    unittest.main()
