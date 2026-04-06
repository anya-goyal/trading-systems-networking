"""Exchange 1 snapshot TCP client: FIX 35=V and line-oriented PITCH snapshot body."""

from __future__ import annotations

import re
import socket
from typing import Any

from exchange1.fix import build_fix


class SnapshotClientError(Exception):
    """Failed to fetch or parse a snapshot."""


_SNAPSHOT_SEQ_RE = re.compile(r"^SNAPSHOT_SEQ=(\d+)\s*$")


def build_market_data_request(*, symbol: str, sender_comp_id: str = "UI") -> bytes:
    """FIX MarketDataRequest (35=V) toward EXCH1."""
    sym = symbol.upper().strip() if symbol.strip() else "ALL"
    fields: list[tuple[str, str]] = [
        ("8", "FIX.4.2"),
        ("35", "V"),
        ("49", sender_comp_id),
        ("56", "EXCH1"),
        ("55", sym),
    ]
    return build_fix(fields)


def parse_snapshot_text(body: str) -> dict[str, Any]:
    """
    Parse snapshot UTF-8 body (lines joined, with or without final newline).

    Returns:
        snapshot_seq: int from SNAPSHOT_SEQ= line
        symbols: dict symbol -> { "bids": [...], "asks": [...] }
        Each level row: order_id (str), qty (int), price (float).
    """
    by_symbol: dict[str, dict[str, list[dict[str, Any]]]] = {}
    snapshot_seq: int | None = None

    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line == "END_SNAPSHOT":
            break
        m = _SNAPSHOT_SEQ_RE.match(line)
        if m:
            snapshot_seq = int(m.group(1))
            continue
        parts = line.split("|")
        if len(parts) < 9:
            continue
        if parts[2] != "A":
            continue
        try:
            order_id = parts[3]
            sym = parts[4]
            side_ch = parts[6]
            qty = int(parts[7])
            price_int = int(parts[8])
        except (IndexError, ValueError):
            continue
        price = price_int / 10000.0
        side = "bids" if side_ch == "B" else "asks"
        by_symbol.setdefault(sym, {"bids": [], "asks": []})
        by_symbol[sym][side].append(
            {"order_id": order_id, "qty": qty, "price": price}
        )

    for sym, book in by_symbol.items():
        book["bids"].sort(key=lambda r: (-r["price"], r["order_id"]))
        book["asks"].sort(key=lambda r: (r["price"], r["order_id"]))

    return {
        "snapshot_seq": snapshot_seq,
        "symbols": by_symbol,
    }


def _read_snapshot_body(sock: socket.socket, *, max_bytes: int = 8 * 1024 * 1024) -> str:
    buf = bytearray()
    while True:
        decoded = buf.decode("utf-8", errors="replace")
        if "END_SNAPSHOT" in decoded:
            return decoded.split("END_SNAPSHOT", 1)[0] + "END_SNAPSHOT\n"
        if len(buf) >= max_bytes:
            raise SnapshotClientError("snapshot response exceeded size limit")
        chunk = sock.recv(65536)
        if not chunk:
            raise SnapshotClientError("connection closed before END_SNAPSHOT")
        buf.extend(chunk)


def fetch_snapshot(
    host: str,
    port: int,
    symbol: str = "ALL",
    *,
    timeout: float = 5.0,
    sender_comp_id: str = "UI",
) -> dict[str, Any]:
    """
    Connect to snapshot port, send 35=V, read until END_SNAPSHOT, parse.

    Returns dict with snapshot_seq, symbols (no error key — use exceptions).
    """
    msg = build_market_data_request(symbol=symbol, sender_comp_id=sender_comp_id)
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            sock.sendall(msg)
            text = _read_snapshot_body(sock)
    except OSError as e:
        raise SnapshotClientError(str(e)) from e

    parsed = parse_snapshot_text(text)
    if parsed["snapshot_seq"] is None:
        raise SnapshotClientError("invalid snapshot: missing SNAPSHOT_SEQ line")
    return parsed
