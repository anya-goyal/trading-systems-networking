"""
In-process Exchange 1 FIX server for integration tests.

Runs ExchangeEngine with the same FIX framing as exchange1.py (SOH + checksum),
on a test-controlled TCP port. PITCH UDP uses config.EXCHANGE1_UDP_HOST /
EXCHANGE1_UDP_PORT at engine construction time — patch config before starting.
"""

from __future__ import annotations

import selectors
import socket
import threading
import time
import traceback
from typing import Any, Callable

from exchange1.engine import ExchangeEngine
from exchange1.fix import parse_fix_message, try_pop_message


class _UdpSendtoLogger:
    """Wraps a UDP socket: logs each sendto payload as UTF-8 text, then forwards."""

    __slots__ = ("_inner", "_path", "_lock")

    def __init__(self, inner: socket.socket, path: str) -> None:
        self._inner = inner
        self._path = path
        self._lock = threading.Lock()

    def sendto(self, data: bytes, addr) -> int:
        line = data.decode("utf-8", errors="replace").rstrip("\r\n")
        with self._lock:
            with open(self._path, "a", encoding="utf-8") as f:
                f.write(f"{time.time():.6f}\t{addr[0]}:{addr[1]}\t{line}\n")
                f.flush()
        return self._inner.sendto(data, addr)

    def close(self) -> None:
        self._inner.close()


class Exchange1FixHarness:
    """TCP FIX listener + ExchangeEngine in a background thread."""

    def __init__(
        self,
        host: str,
        fix_port: int,
        *,
        allowed_symbols: set[str] | frozenset[str] | None = None,
        udp_log_path: str | None = None,
    ) -> None:
        self.host = host
        self.fix_port = fix_port
        self.allowed_symbols = (
            set(allowed_symbols) if allowed_symbols is not None else None
        )
        self._udp_log_path = udp_log_path
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=5.0):
            raise RuntimeError("Exchange1FixHarness did not become ready")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def _run(self) -> None:
        udp_sock: socket.socket | _UdpSendtoLogger = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM
        )
        if self._udp_log_path:
            udp_sock = _UdpSendtoLogger(udp_sock, self._udp_log_path)
        syms = self.allowed_symbols
        engine = ExchangeEngine(
            udp_sock, allowed_symbols=syms if syms is not None else None
        )
        engine.startup_broadcast()

        fix_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        fix_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        fix_listener.bind((self.host, self.fix_port))
        fix_listener.listen(8)
        fix_listener.setblocking(False)

        sel = selectors.DefaultSelector()
        conns: dict[socket.socket, bytearray] = {}

        def read_client(key: Any, _mask: int) -> None:
            conn = key.fileobj
            assert isinstance(conn, socket.socket)
            buf = conns.get(conn)
            if buf is None:
                return
            try:
                data = conn.recv(65536)
            except (ConnectionResetError, OSError) as e:
                print(f"[Ex1Harness] recv error: {e}")
                data = b""
            if not data:
                sel.unregister(conn)
                conns.pop(conn, None)
                conn.close()
                return
            buf.extend(data)
            while True:
                raw = try_pop_message(buf)
                if raw is None:
                    break
                try:
                    fields = parse_fix_message(raw)
                except ValueError as e:
                    print(f"[Ex1Harness] bad FIX: {e}")
                    continue

                def sender(b: bytes) -> None:
                    try:
                        conn.sendall(b)
                    except OSError as e:
                        print(f"[Ex1Harness] send failed: {e}")

                try:
                    engine.handle_fix(fields, sender)
                except Exception:
                    print("[Ex1Harness] engine error:")
                    traceback.print_exc()

        def accept_fix(_key: Any, _mask: int) -> None:
            conn, addr = fix_listener.accept()
            conn.setblocking(False)
            conns[conn] = bytearray()
            sel.register(conn, selectors.EVENT_READ, data=read_client)
            print(f"[Ex1Harness] FIX client {addr[0]}:{addr[1]}")

        sel.register(fix_listener, selectors.EVENT_READ, data=accept_fix)
        self._ready.set()

        try:
            while not self._stop.is_set():
                try:
                    events = sel.select(timeout=0.2)
                except OSError:
                    break
                for key, mask in events:
                    cb: Callable[[Any, int], None] = key.data
                    cb(key, mask)
        finally:
            try:
                sel.unregister(fix_listener)
            except Exception:
                pass
            for c in list(conns):
                try:
                    sel.unregister(c)
                except Exception:
                    pass
                try:
                    c.close()
                except OSError:
                    pass
            conns.clear()
            try:
                fix_listener.close()
            except OSError:
                pass
            try:
                udp_sock.close()
            except OSError:
                pass


class StubExchangeTcpDrain:
    """Minimal TCP server: accepts connections and drains recv (for EXCH2 stub)."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._srv: socket.socket | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=5.0):
            raise RuntimeError("StubExchangeTcpDrain did not become ready")

    def stop(self) -> None:
        self._stop.set()
        if self._srv:
            try:
                self._srv.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        self._srv = None

    def _run(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv = srv
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(4)
        srv.settimeout(0.3)
        self._ready.set()
        while not self._stop.is_set():
            try:
                conn, _ = srv.accept()
            except OSError:
                break
            except socket.timeout:
                continue
            conn.settimeout(0.3)
            while not self._stop.is_set():
                try:
                    chunk = conn.recv(65536)
                except socket.timeout:
                    continue
                except OSError:
                    break
                if not chunk:
                    break
            try:
                conn.close()
            except OSError:
                pass
        try:
            srv.close()
        except OSError:
            pass
