"""
exchange1.py

Simulated exchange 1.
  - Publishes a market data feed over UDP (PITCH) to EXCHANGE1_UDP_HOST:PORT
  - Listens for FIX 4.2 order entry on EXCHANGE1_FIX_PORT (IOG)
  - Listens for FIX 35=V snapshot requests on EXCHANGE1_SNAPSHOT_PORT (data handlers)
"""

from __future__ import annotations

import selectors
import socket
import traceback
from typing import Any

import config
from exchange1.engine import ExchangeEngine
from exchange1.fix import parse_fix_message, try_pop_message


def _warn_routing(fields: dict[str, str], peer: str) -> None:
    """Log if envelope does not match expected IOG → EXCH1 routing (still process)."""
    if fields.get("49") != "IOG" or fields.get("56") != "EXCH1":
        print(
            f"[Exchange 1] WARN unexpected 49/56 from {peer}: "
            f"49={fields.get('49')} 56={fields.get('56')}"
        )


class _ConnState:
    __slots__ = ("sock", "buf", "kind", "peer")

    def __init__(self, sock: socket.socket, kind: str, peer: str) -> None:
        self.sock = sock
        self.buf = bytearray()
        self.kind = kind
        self.peer = peer


def main() -> None:
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    engine = ExchangeEngine(udp_sock)

    fix_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fix_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    fix_listener.bind((config.EXCHANGE1_FIX_HOST, config.EXCHANGE1_FIX_PORT))
    fix_listener.listen(32)
    fix_listener.setblocking(False)

    snap_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    snap_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    snap_listener.bind((config.EXCHANGE1_SNAPSHOT_HOST, config.EXCHANGE1_SNAPSHOT_PORT))
    snap_listener.listen(32)
    snap_listener.setblocking(False)

    sel = selectors.DefaultSelector()
    conns: dict[socket.socket, _ConnState] = {}

    def accept_fix(_key: Any, _mask: int) -> None:
        conn, addr = fix_listener.accept()
        conn.setblocking(False)
        peer = f"{addr[0]}:{addr[1]}"
        st = _ConnState(conn, "fix", peer)
        conns[conn] = st
        sel.register(conn, selectors.EVENT_READ, data=read_client)
        print(f"[Exchange 1] FIX client connected {peer}")

    def accept_snap(_key: Any, _mask: int) -> None:
        conn, addr = snap_listener.accept()
        conn.setblocking(False)
        peer = f"{addr[0]}:{addr[1]}"
        st = _ConnState(conn, "snap", peer)
        conns[conn] = st
        sel.register(conn, selectors.EVENT_READ, data=read_client)
        print(f"[Exchange 1] Snapshot client connected {peer}")

    def read_client(key: Any, _mask: int) -> None:
        conn = key.fileobj
        assert isinstance(conn, socket.socket)
        st = conns.get(conn)
        if st is None:
            return
        try:
            data = conn.recv(65536)
        except (ConnectionResetError, OSError) as e:
            print(f"[Exchange 1] recv error {st.peer}: {e}")
            data = b""

        if not data:
            sel.unregister(conn)
            conns.pop(conn, None)
            conn.close()
            print(f"[Exchange 1] {st.kind} client disconnected {st.peer}")
            return

        st.buf.extend(data)
        while True:
            raw = try_pop_message(st.buf)
            if raw is None:
                break
            try:
                fields = parse_fix_message(raw)
            except ValueError as e:
                print(f"[Exchange 1] bad FIX from {st.peer}: {e}")
                continue

            _warn_routing(fields, st.peer)

            def sender(b: bytes) -> None:
                try:
                    print(f"[Exchange 1] Sent FIX: {b.decode(errors='replace').strip()}")
                    conn.sendall(b)
                except OSError as e:
                    print(f"[Exchange 1] send to {st.peer} failed: {e}")

            if st.kind == "fix" and fields.get("35") == "V":
                print(f"[Exchange 1] 35=V on FIX port from {st.peer} — use snapshot port")
                continue

            try:
                engine.handle_fix(fields, sender)
            except Exception:
                print(f"[Exchange 1] engine error from {st.peer}:")
                traceback.print_exc()

    sel.register(fix_listener, selectors.EVENT_READ, data=accept_fix)
    sel.register(snap_listener, selectors.EVENT_READ, data=accept_snap)

    print(
        f"[Exchange 1] UDP PITCH → {config.EXCHANGE1_UDP_HOST}:{config.EXCHANGE1_UDP_PORT}"
    )
    print(
        f"[Exchange 1] FIX listening {config.EXCHANGE1_FIX_HOST}:{config.EXCHANGE1_FIX_PORT}"
    )
    print(
        f"[Exchange 1] Snapshot listening "
        f"{config.EXCHANGE1_SNAPSHOT_HOST}:{config.EXCHANGE1_SNAPSHOT_PORT}"
    )
    engine.startup_broadcast()
    print("[Exchange 1] Running (selector loop). Ctrl+C to stop.")

    try:
        while True:
            events = sel.select(timeout=1.0)
            for key, mask in events:
                cb = key.data
                cb(key, mask)
    except KeyboardInterrupt:
        print("[Exchange 1] Shutting down.")
    finally:
        sel.close()
        fix_listener.close()
        snap_listener.close()
        udp_sock.close()
        for c in list(conns):
            try:
                c.close()
            except OSError:
                pass


if __name__ == "__main__":
    main()
