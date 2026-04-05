"""
internal_order_gateway.py

Internal Order Gateway (IOG).
  - Receives orders from strategies over TCP (internal binary protocol)
  - Validates orders (stateless field checks)
  - Risk checks orders (stateful: position limits, rate limits, kill switch)
  - Serializes to FIX and routes to the correct exchange over TCP
  - Receives execution reports from exchanges and forwards to strategies
  - Handles disconnections, reconnections, and graceful shutdown
"""

import selectors
import signal
import socket
import struct
import sys
import time

import config
from iog.messages import (
    MSG_TYPE_NEW_ORDER,
    MSG_TYPE_CANCEL,
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
    LENGTH_PREFIX_SIZE,
    LENGTH_PREFIX_FMT,
    InternalOrder,
    Deserializer,
    serialize_response,
    deserialize_order,
    ORDER_BODY_SIZE,
)
from iog.validator import validate
from iog.risk import RiskChecker
from iog.fix import (
    SOH,
    serialize_new_order,
    serialize_cancel_request,
    parse_execution_report,
)
from iog.connection import (
    StrategySession,
    ExchangeConn,
    ConnState,
    OrderBook,
    OrderState,
)
from iog.exec_handler import ExecReportHandler

# ── Risk limits (configurable) ──────────────────────────────────────────────
MAX_ORDER_QTY = 10_000
MAX_NOTIONAL = 1_000_000.0
MAX_POSITION_PER_SYMBOL = 50_000
MAX_ORDERS_PER_SEC = 100

# ── Exchange config mapping ─────────────────────────────────────────────────
EXCHANGE_CONFIG = {
    DEST_EXCH1: {
        "id": "EXCH1",
        "host": config.EXCHANGE1_FIX_HOST,
        "port": config.EXCHANGE1_FIX_PORT,
    },
    DEST_EXCH2: {
        "id": "EXCH2",
        "host": config.EXCHANGE2_FIX_HOST,
        "port": config.EXCHANGE2_FIX_PORT,
    },
}


class IOGServer:
    """The Internal Order Gateway server."""

    def __init__(self):
        self._sel = selectors.DefaultSelector()
        self._shutdown_requested = False

        # ── Core components ──────────────────────────────────────────────
        self._risk = RiskChecker(
            max_order_qty=MAX_ORDER_QTY,
            max_notional=MAX_NOTIONAL,
            max_position_per_symbol=MAX_POSITION_PER_SYMBOL,
            max_orders_per_sec=MAX_ORDERS_PER_SEC,
        )
        self._order_book = OrderBook()
        self._exec_handler = ExecReportHandler(
            order_book=self._order_book, risk_checker=self._risk
        )

        # ── Connection tracking ──────────────────────────────────────────
        self._strategy_sessions: dict[int, StrategySession] = {}  # fd → session
        self._strategy_deserializers: dict[int, Deserializer] = {}  # fd → deserializer
        self._exchange_conns: dict[int, ExchangeConn] = {}  # dest → ExchangeConn
        self._listener_sock = None

    # ── Setup ────────────────────────────────────────────────────────────────

    def _setup_listener(self):
        """Set up the TCP listener for strategy connections."""
        self._listener_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener_sock.setblocking(False)
        self._listener_sock.bind((config.IOG_HOST, config.IOG_PORT))
        self._listener_sock.listen(5)
        self._sel.register(
            self._listener_sock, selectors.EVENT_READ, data="listener"
        )
        print(
            f"[IOG] Listening for strategies on "
            f"{config.IOG_HOST}:{config.IOG_PORT}"
        )

    def _connect_exchange(self, dest: int):
        """Attempt to connect to an exchange. Non-blocking."""
        cfg = EXCHANGE_CONFIG[dest]
        conn = self._exchange_conns.get(dest)
        if conn is None:
            conn = ExchangeConn(exchange_id=cfg["id"])
            self._exchange_conns[dest] = conn

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setblocking(False)
            err = sock.connect_ex((cfg["host"], cfg["port"]))
            # connect_ex returns 0 or EINPROGRESS (36 on macOS, 115 on Linux)
            if err == 0 or err == 36 or err == 115:
                conn.mark_connected(sock)
                self._sel.register(
                    sock, selectors.EVENT_READ, data=f"exch:{dest}"
                )
                print(f"[IOG] Connected to {cfg['id']} at {cfg['host']}:{cfg['port']}")
            else:
                print(f"[IOG] Failed to connect to {cfg['id']}: errno {err}")
                sock.close()
                conn.mark_down()
                conn.bump_backoff()
                conn.reconnect_at = time.monotonic() + conn.backoff
        except OSError as e:
            print(f"[IOG] Error connecting to {cfg['id']}: {e}")
            conn.mark_down()
            conn.bump_backoff()
            conn.reconnect_at = time.monotonic() + conn.backoff

    def _setup_signals(self):
        """Register signal handlers for graceful shutdown.
        Skipped when running in a non-main thread (e.g. tests).
        """
        import threading
        if threading.current_thread() is not threading.main_thread():
            return
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        print(f"\n[IOG] Received signal {signum}, shutting down...")
        self._shutdown_requested = True

    # ── Strategy handling ────────────────────────────────────────────────────

    def _accept_strategy(self):
        """Accept a new strategy connection."""
        conn, addr = self._listener_sock.accept()
        conn.setblocking(False)
        fd = conn.fileno()
        # Strategy ID will be set from the first order's strategy_id field
        session = StrategySession(sock=conn, strategy_id=f"unknown-{fd}")
        self._strategy_sessions[fd] = session
        self._strategy_deserializers[fd] = Deserializer()
        self._sel.register(conn, selectors.EVENT_READ, data=f"strat:{fd}")
        print(f"[IOG] Strategy connected from {addr} (fd={fd})")

    def _remove_strategy(self, fd: int):
        """Clean up a disconnected strategy — cancel open orders."""
        session = self._strategy_sessions.pop(fd, None)
        self._strategy_deserializers.pop(fd, None)
        if session is None:
            return

        # Cancel all open orders for this strategy
        open_orders = self._order_book.get_open_orders(strategy_fd=fd)
        for entry in open_orders:
            dest = entry.order.destination
            conn = self._exchange_conns.get(dest)
            if conn and conn.state == ConnState.CONNECTED:
                cancel_msg = serialize_cancel_request(
                    entry.order.clOrdID,
                    entry.order.symbol,
                    entry.order.side,
                    dest,
                )
                try:
                    conn.sock.sendall(cancel_msg)
                    print(
                        f"[IOG] Sent cancel for {entry.order.clOrdID} "
                        f"to {conn.exchange_id}"
                    )
                except OSError:
                    pass
            self._order_book.update_state(entry.order.clOrdID, OrderState.CANCELLED)

        try:
            self._sel.unregister(session.sock)
        except Exception:
            pass
        try:
            session.sock.close()
        except Exception:
            pass
        print(
            f"[IOG] Strategy {session.strategy_id} disconnected "
            f"(fd={fd}, cancelled {len(open_orders)} orders)"
        )

    def _handle_strategy_data(self, fd: int):
        """Read data from a strategy connection, process complete messages."""
        session = self._strategy_sessions.get(fd)
        if session is None:
            return

        try:
            data = session.sock.recv(4096)
        except (ConnectionResetError, OSError):
            data = b""

        if not data:
            self._remove_strategy(fd)
            return

        deser = self._strategy_deserializers[fd]
        deser.feed(data)

        for order in deser.drain():
            # Update strategy ID from the order
            if session.strategy_id.startswith("unknown"):
                session.strategy_id = order.strategy_id

            session.order_count += 1
            self._process_order(order, fd)

    # ── Order processing pipeline ────────────────────────────────────────────

    def _process_order(self, order: InternalOrder, strategy_fd: int):
        """Run an order through the full pipeline: validate → risk → route."""
        clOrdID = order.clOrdID

        if order.msg_type == MSG_TYPE_CANCEL:
            self._process_cancel(order, strategy_fd)
            return

        # Book the order
        self._order_book.add(order, strategy_fd=strategy_fd)
        session = self._strategy_sessions.get(strategy_fd)
        if session:
            session.open_orders.add(clOrdID)

        # ── Validate ─────────────────────────────────────────────────────
        ok, reason = validate(order)
        if not ok:
            print(f"[IOG] REJECT {clOrdID}: validation failed — {reason}")
            self._order_book.update_state(clOrdID, OrderState.REJECTED)
            self._send_reject(strategy_fd, clOrdID, REJECT_VALIDATION)
            return
        self._order_book.update_state(clOrdID, OrderState.VALIDATED)

        # ── Risk check ───────────────────────────────────────────────────
        ok, reason = self._risk.check(order)
        if not ok:
            print(f"[IOG] REJECT {clOrdID}: risk check failed — {reason}")
            self._order_book.update_state(clOrdID, OrderState.RISK_FAIL)
            self._send_reject(strategy_fd, clOrdID, REJECT_RISK)
            return
        self._order_book.update_state(clOrdID, OrderState.RISK_CHECKED)

        # ── Route ────────────────────────────────────────────────────────
        dest = order.destination
        conn = self._exchange_conns.get(dest)
        if conn is None or conn.state != ConnState.CONNECTED:
            exch_name = EXCHANGE_CONFIG.get(dest, {}).get("id", "UNKNOWN")
            print(f"[IOG] REJECT {clOrdID}: {exch_name} is DOWN")
            self._order_book.update_state(clOrdID, OrderState.ROUTE_FAIL)
            self._send_reject(strategy_fd, clOrdID, REJECT_EXCH_DOWN)
            return
        self._order_book.update_state(clOrdID, OrderState.ROUTED)

        # ── Serialize to FIX and send ────────────────────────────────────
        fix_msg = serialize_new_order(order)
        try:
            conn.sock.sendall(fix_msg)
            self._order_book.update_state(clOrdID, OrderState.SENT)
            entry = self._order_book.get(clOrdID)
            if entry:
                entry.fix_msg = fix_msg
            print(
                f"[IOG] SENT {clOrdID} → {conn.exchange_id} "
                f"({order.symbol} {['', 'BUY', 'SELL'][order.side]} "
                f"{order.qty}@{order.price})"
            )
        except OSError as e:
            print(f"[IOG] REJECT {clOrdID}: send failed — {e}")
            self._order_book.update_state(clOrdID, OrderState.ROUTE_FAIL)
            self._send_reject(strategy_fd, clOrdID, REJECT_EXCH_DOWN)
            self._handle_exchange_disconnect(dest)

    def _process_cancel(self, order: InternalOrder, strategy_fd: int):
        """Process a cancel request."""
        clOrdID = order.clOrdID
        entry = self._order_book.get(clOrdID)
        if entry is None:
            print(f"[IOG] Cancel for unknown order {clOrdID}")
            self._send_reject(strategy_fd, clOrdID, REJECT_VALIDATION)
            return

        dest = entry.order.destination
        conn = self._exchange_conns.get(dest)
        if conn is None or conn.state != ConnState.CONNECTED:
            self._send_reject(strategy_fd, clOrdID, REJECT_EXCH_DOWN)
            return

        cancel_msg = serialize_cancel_request(
            clOrdID, entry.order.symbol, entry.order.side, dest
        )
        try:
            conn.sock.sendall(cancel_msg)
            print(f"[IOG] Cancel request sent for {clOrdID} → {conn.exchange_id}")
        except OSError:
            self._send_reject(strategy_fd, clOrdID, REJECT_EXCH_DOWN)
            self._handle_exchange_disconnect(dest)

    # ── Exchange handling ────────────────────────────────────────────────────

    def _handle_exchange_data(self, dest: int):
        """Read data from an exchange connection, parse execution reports."""
        conn = self._exchange_conns.get(dest)
        if conn is None or conn.sock is None:
            return

        try:
            data = conn.sock.recv(4096)
        except (ConnectionResetError, OSError):
            data = b""

        if not data:
            self._handle_exchange_disconnect(dest)
            return

        conn.last_heartbeat = time.time()
        conn.recv_buffer.extend(data)

        # FIX messages end with 10=XXX<SOH> — extract complete messages
        while True:
            end_marker = conn.recv_buffer.find(b"10=")
            if end_marker == -1:
                break
            # Find the SOH after the checksum (3 digits + SOH)
            end_pos = conn.recv_buffer.find(SOH, end_marker + 3)
            if end_pos == -1:
                break
            raw_msg = bytes(conn.recv_buffer[: end_pos + 1])
            del conn.recv_buffer[: end_pos + 1]

            try:
                report = parse_execution_report(raw_msg)
                result = self._exec_handler.handle(report)
                if result:
                    self._forward_exec_report(result)
            except (ValueError, KeyError) as e:
                print(f"[IOG] Error parsing exec report from {conn.exchange_id}: {e}")

    def _handle_exchange_disconnect(self, dest: int):
        """Handle an exchange connection dropping."""
        conn = self._exchange_conns.get(dest)
        if conn is None:
            return

        exch_id = conn.exchange_id
        print(f"[IOG] Exchange {exch_id} disconnected")

        try:
            self._sel.unregister(conn.sock)
        except Exception:
            pass
        conn.mark_down()
        conn.bump_backoff()
        conn.reconnect_at = time.monotonic() + conn.backoff

        # Notify strategies about pending orders on this exchange
        open_orders = self._order_book.get_open_orders_by_dest(dest)
        for entry in open_orders:
            self._order_book.update_state(entry.order.clOrdID, OrderState.ROUTE_FAIL)
            self._send_reject(entry.strategy_fd, entry.order.clOrdID, REJECT_EXCH_DOWN)
        print(
            f"[IOG] Notified strategies about {len(open_orders)} "
            f"orphaned orders on {exch_id}, reconnecting in {conn.backoff}s"
        )

    def _attempt_reconnects(self):
        """Try to reconnect to any DOWN exchanges whose backoff has elapsed."""
        now = time.monotonic()
        for dest, conn in self._exchange_conns.items():
            if conn.state == ConnState.DOWN and now >= conn.reconnect_at:
                print(f"[IOG] Attempting reconnect to {conn.exchange_id}...")
                self._connect_exchange(dest)

    # ── Response helpers ─────────────────────────────────────────────────────

    def _send_reject(self, strategy_fd: int, clOrdID: str, reason: int):
        """Send a reject response back to a strategy."""
        session = self._strategy_sessions.get(strategy_fd)
        if session is None:
            return
        resp = serialize_response(
            msg_type=RESP_REJECT,
            clOrdID=clOrdID,
            reject_reason=reason,
            filled_qty=0,
            cumulative_qty=0,
            fill_price=0.0,
            timestamp=time.time(),
        )
        try:
            session.sock.sendall(resp)
        except OSError:
            pass
        if session:
            session.open_orders.discard(clOrdID)

    def _forward_exec_report(self, result: dict):
        """Forward an execution report to the originating strategy."""
        strategy_fd = result["strategy_fd"]
        session = self._strategy_sessions.get(strategy_fd)
        if session is None:
            return

        exec_type = result["exec_type"]
        if exec_type == "0":
            msg_type = RESP_ACK
        elif exec_type == "1":
            msg_type = RESP_PARTIAL_FILL
        elif exec_type == "2":
            msg_type = RESP_FILL
        elif exec_type == "8":
            msg_type = RESP_REJECT
        else:
            return

        reject_reason = REJECT_EXCH_REJECT if exec_type == "8" else REJECT_NONE

        resp = serialize_response(
            msg_type=msg_type,
            clOrdID=result["clOrdID"],
            reject_reason=reject_reason,
            filled_qty=result.get("last_qty", 0),
            cumulative_qty=result.get("cum_qty", 0),
            fill_price=result.get("last_px", 0.0),
            timestamp=time.time(),
        )
        try:
            session.sock.sendall(resp)
        except OSError:
            pass

        # Remove from open orders if terminal
        if exec_type in ("2", "8"):
            session.open_orders.discard(result["clOrdID"])

    # ── Graceful shutdown ────────────────────────────────────────────────────

    def _graceful_shutdown(self):
        """Cancel all open orders, notify strategies, close everything."""
        print("[IOG] Graceful shutdown initiated...")
        self._risk.activate_kill_switch()

        # Cancel all open orders on each exchange
        for dest, conn in self._exchange_conns.items():
            if conn.state != ConnState.CONNECTED:
                continue
            open_orders = self._order_book.get_open_orders_by_dest(dest)
            for entry in open_orders:
                cancel_msg = serialize_cancel_request(
                    entry.order.clOrdID,
                    entry.order.symbol,
                    entry.order.side,
                    dest,
                )
                try:
                    conn.sock.sendall(cancel_msg)
                except OSError:
                    pass
                self._order_book.update_state(
                    entry.order.clOrdID, OrderState.CANCELLED
                )
            print(f"[IOG] Cancelled {len(open_orders)} orders on {conn.exchange_id}")

        # Close all strategy connections
        for fd, session in list(self._strategy_sessions.items()):
            try:
                self._sel.unregister(session.sock)
            except Exception:
                pass
            try:
                session.sock.close()
            except Exception:
                pass
        self._strategy_sessions.clear()

        # Close exchange connections
        for dest, conn in self._exchange_conns.items():
            if conn.sock:
                try:
                    self._sel.unregister(conn.sock)
                except Exception:
                    pass
                try:
                    conn.sock.close()
                except Exception:
                    pass
                conn.mark_down()

        # Close listener
        if self._listener_sock:
            try:
                self._sel.unregister(self._listener_sock)
            except Exception:
                pass
            self._listener_sock.close()

        self._sel.close()
        print("[IOG] Shutdown complete.")

    # ── Main event loop ──────────────────────────────────────────────────────

    def run(self):
        """Start the IOG."""
        self._setup_signals()
        self._setup_listener()

        # Connect to exchanges
        for dest in EXCHANGE_CONFIG:
            self._connect_exchange(dest)

        print("[IOG] Running.")

        while not self._shutdown_requested:
            try:
                events = self._sel.select(timeout=1.0)
            except OSError:
                break

            for key, mask in events:
                tag = key.data
                if tag == "listener":
                    self._accept_strategy()
                elif tag.startswith("strat:"):
                    fd = int(tag.split(":")[1])
                    self._handle_strategy_data(fd)
                elif tag.startswith("exch:"):
                    dest = int(tag.split(":")[1])
                    self._handle_exchange_data(dest)

            # Housekeeping
            self._attempt_reconnects()

        self._graceful_shutdown()


def main():
    server = IOGServer()
    server.run()


if __name__ == "__main__":
    main()
