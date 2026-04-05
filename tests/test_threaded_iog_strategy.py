"""
Threaded integration tests: strategy_fast <-> IOG <-> fake exchange.

Three threads running simultaneously:
  1. IOG event loop — listens on TCP, accepts strategy connections, routes to exchange
  2. strategy_fast main loop — reads MDH multicast, sends orders to IOG over TCP
  3. Fake exchange — TCP server that captures FIX messages from IOG

A fake MDH publisher sends UDP packets to the multicast group to drive
the strategy's trading decisions. No mocks — real sockets, real binary framing.
"""

import socket
import struct
import threading
import time
import unittest

import config
from iog.messages import SIDE_BUY, SIDE_SELL
from iog.fix import SOH, compute_checksum
from strategy_fast import MDH_HDR_FMT, MDH_BODY_FMT

# Test ports offset from config to avoid collisions with a live system
TEST_IOG_PORT = config.IOG_PORT + 1000              # 9001
TEST_EXCH1_PORT = config.EXCHANGE1_FIX_PORT + 1000  # 7001
TEST_EXCH2_PORT = config.EXCHANGE2_FIX_PORT + 1000  # 7002
TEST_HOST = "127.0.0.1"

# MDH multicast — we send to the same group/port the strategy subscribes to
MDH_GROUP = config.MULTICAST_GROUP_1
MDH_PORT = config.MULTICAST_PORT_1


# ── MDH Packet Builder ─────────────────────────────────────────────────────

def build_mdh_packet(
    symbol="AAPL",
    side=SIDE_BUY,
    price=150.0,
    qty=100,
    update_type=config.UPDATE_NEW_ORDER,
    asset_class=config.ASSET_EQUITIES,
    seq_no=1,
    msg_type=config.MSG_TYPE_UPDATE,
):
    """Build a binary MDH packet matching the strategy's expected wire format."""
    body = struct.pack(
        MDH_BODY_FMT,
        msg_type,
        b"ORD00001",                                     # order_id (8 bytes)
        seq_no,
        asset_class,
        symbol.encode().ljust(8, b"\x00")[:8],
        side,
        update_type,
        price,
        qty,
        int(time.time() * 1_000_000_000),                # timestamp nanos
    )
    header = struct.pack(MDH_HDR_FMT, len(body))
    return header + body


def send_mdh_packet(packet, group=MDH_GROUP, port=MDH_PORT):
    """Send a single UDP packet to the multicast group."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    sock.sendto(packet, (group, port))
    sock.close()


def send_spread(symbol="AAPL", bid=150.0, ask=150.10, seq_start=1):
    """Send a bid + ask update to create a tradeable spread."""
    send_mdh_packet(build_mdh_packet(
        symbol=symbol, side=SIDE_BUY, price=bid, seq_no=seq_start,
    ))
    send_mdh_packet(build_mdh_packet(
        symbol=symbol, side=SIDE_SELL, price=ask, seq_no=seq_start + 1,
    ))


# ── Fake Exchange ───────────────────────────────────────────────────────────

class FakeExchange:
    """TCP server that accepts the IOG's outbound connection,
    captures received FIX messages, and can send exec reports back."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._server_sock = None
        self._client_sock = None
        self._running = False
        self._received: list[bytes] = []
        self._lock = threading.Lock()
        self._connected = threading.Event()

    def start(self):
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(1)
        self._server_sock.settimeout(5.0)
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        try:
            self._client_sock, _ = self._server_sock.accept()
            self._client_sock.settimeout(0.5)
            self._connected.set()
        except socket.timeout:
            return
        while self._running:
            try:
                data = self._client_sock.recv(4096)
                if not data:
                    break
                with self._lock:
                    self._received.append(data)
            except socket.timeout:
                continue
            except OSError:
                break

    def wait_connected(self, timeout=3.0):
        return self._connected.wait(timeout)

    def get_all_data(self):
        with self._lock:
            return b"".join(self._received)

    def send(self, data):
        if self._client_sock:
            self._client_sock.sendall(data)

    def stop(self):
        self._running = False
        for s in (self._client_sock, self._server_sock):
            if s:
                try:
                    s.close()
                except OSError:
                    pass


def build_fix_exec_report(cl_ord_id, exec_type, cum_qty=0, last_px=0.0,
                          last_qty=0, text=""):
    """Build a FIX ExecutionReport to send from the fake exchange."""
    fields = [
        ("8", "FIX.4.2"), ("35", "8"), ("49", "EXCH1"), ("56", "IOG"),
        ("11", cl_ord_id), ("150", exec_type), ("14", str(cum_qty)),
        ("31", str(last_px)), ("32", str(last_qty)), ("58", text),
    ]
    body = SOH.join(f"{t}={v}".encode() for t, v in fields) + SOH
    cs = compute_checksum(body)
    return body + f"10={cs}".encode() + SOH


# ── Strategy Thread ─────────────────────────────────────────────────────────

def run_strategy_in_thread(stop_event):
    """Run strategy_fast.main() in a thread, stopping when stop_event is set."""
    import strategy_fast
    import select as _select

    # Patch the strategy's select loop to check for stop
    original_select = _select.select

    def patched_select(rlist, wlist, xlist, timeout=None):
        if stop_event.is_set():
            raise KeyboardInterrupt
        return original_select(rlist, wlist, xlist, timeout)

    _select.select = patched_select
    try:
        strategy_fast.main()
    except KeyboardInterrupt:
        pass
    finally:
        _select.select = original_select


# ── Test Suite ──────────────────────────────────────────────────────────────

class TestStrategyToIOGThreaded(unittest.TestCase):
    """
    Full threaded integration:
      - Fake exchange (TCP server) started first
      - IOG started in a thread (connects to fake exchange, listens for strategies)
      - strategy_fast started in a thread (subscribes to multicast, connects to IOG)
      - Test sends MDH packets via UDP → strategy processes them → sends orders to IOG
        → IOG sends FIX to fake exchange
    """

    @classmethod
    def setUpClass(cls):
        # Patch config ports
        cls._orig = {
            "IOG_PORT": config.IOG_PORT,
            "EXCHANGE1_FIX_PORT": config.EXCHANGE1_FIX_PORT,
            "EXCHANGE2_FIX_PORT": config.EXCHANGE2_FIX_PORT,
        }
        config.IOG_PORT = TEST_IOG_PORT
        config.EXCHANGE1_FIX_PORT = TEST_EXCH1_PORT
        config.EXCHANGE2_FIX_PORT = TEST_EXCH2_PORT

        # 1. Start fake exchange
        cls.fake_exch = FakeExchange(TEST_HOST, TEST_EXCH1_PORT)
        cls.fake_exch.start()

        # 2. Start IOG
        from internal_order_gateway import IOGServer
        cls.server = IOGServer()
        cls.iog_thread = threading.Thread(target=cls.server.run, daemon=True)
        cls.iog_thread.start()
        time.sleep(0.3)
        cls.fake_exch.wait_connected(timeout=3.0)

        # 3. Start strategy_fast
        cls.strategy_stop = threading.Event()
        cls.strategy_thread = threading.Thread(
            target=run_strategy_in_thread,
            args=(cls.strategy_stop,),
            daemon=True,
        )
        cls.strategy_thread.start()
        time.sleep(0.5)  # let strategy connect to IOG

    @classmethod
    def tearDownClass(cls):
        cls.strategy_stop.set()
        cls.strategy_thread.join(timeout=3.0)
        cls.server._shutdown_requested = True
        cls.iog_thread.join(timeout=3.0)
        cls.fake_exch.stop()
        for key, val in cls._orig.items():
            setattr(config, key, val)

    def test_strategy_sends_order_on_wide_spread(self):
        """Send MDH bid+ask with spread >= threshold → strategy fires an order
        → IOG receives it → FIX NewOrderSingle arrives at fake exchange."""
        # Clear any prior data
        with self.fake_exch._lock:
            self.fake_exch._received.clear()

        # Send bid=150.00, ask=150.10  → spread=0.10 >= MIN_SPREAD_TO_TRADE (0.05)
        send_spread("AAPL", bid=150.00, ask=150.10, seq_start=100)
        time.sleep(1.0)  # give the pipeline time to process

        data = self.fake_exch.get_all_data()
        self.assertIn(b"35=D", data, "Expected FIX NewOrderSingle at exchange")
        self.assertIn(b"55=AAPL", data, "Expected symbol=AAPL in FIX message")
        self.assertIn(b"54=1", data, "Expected side=BUY in FIX message")

    def test_strategy_does_not_trade_on_narrow_spread(self):
        """Spread below threshold → strategy should NOT send an order."""
        with self.fake_exch._lock:
            self.fake_exch._received.clear()

        # Use a unique symbol so it doesn't collide with AAPL state from other tests
        # bid=200.00, ask=200.01 → spread=0.01 < MIN_SPREAD_TO_TRADE (0.05)
        send_spread("MSFT", bid=200.00, ask=200.01, seq_start=200)
        time.sleep(1.0)

        data = self.fake_exch.get_all_data()
        self.assertNotIn(b"55=MSFT", data,
                         "Strategy should NOT trade when spread < threshold")

    def test_strategy_respects_symbol_cooldown(self):
        """After one order, the strategy should wait SYMBOL_COOLDOWN_SEC before
        sending another order on the same symbol."""
        with self.fake_exch._lock:
            self.fake_exch._received.clear()

        # First spread — should trigger an order
        send_spread("TSLA", bid=300.00, ask=300.10, seq_start=300)
        time.sleep(0.15)

        # Second spread immediately — within cooldown (0.25s) so should NOT trigger
        send_spread("TSLA", bid=300.00, ask=300.10, seq_start=302)
        time.sleep(0.5)

        data = self.fake_exch.get_all_data()
        count = data.count(b"55=TSLA")
        self.assertEqual(count, 1,
                         f"Expected exactly 1 TSLA order (cooldown), got {count}")

    def test_order_arrives_with_correct_qty(self):
        """The strategy uses DEFAULT_ORDER_QTY from config."""
        with self.fake_exch._lock:
            self.fake_exch._received.clear()

        send_spread("GOOG", bid=175.00, ask=175.10, seq_start=400)
        time.sleep(1.0)

        data = self.fake_exch.get_all_data()
        expected_qty = f"38={config.DEFAULT_ORDER_QTY}".encode()
        self.assertIn(expected_qty, data,
                      f"Expected qty={config.DEFAULT_ORDER_QTY} in FIX message")

    def test_multiple_symbols_generate_separate_orders(self):
        """Different symbols each generate their own order."""
        with self.fake_exch._lock:
            self.fake_exch._received.clear()

        send_spread("NVDA", bid=180.00, ask=180.10, seq_start=500)
        time.sleep(0.5)
        send_spread("META", bid=600.00, ask=600.10, seq_start=502)
        time.sleep(1.0)

        data = self.fake_exch.get_all_data()
        self.assertIn(b"55=NVDA", data)
        self.assertIn(b"55=META", data)


if __name__ == "__main__":
    unittest.main()
