"""Order book, matching, and UDP PITCH emission for Exchange 1."""

from __future__ import annotations

import socket
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import config
from exchange1.fix import execution_report

SIDE_BUY = 1
SIDE_SELL = 2
ORD_MARKET = 1
ORD_LIMIT = 2
TIF_DAY = 0
TIF_IOC = 3


def price_to_int(s: str) -> int:
    return int(round(float(s) * 10000))


def price_int_to_str(pi: int) -> str:
    if pi % 10000 == 0:
        return str(pi // 10000)
    return f"{pi / 10000:.6g}"


def _fix_side_to_pitch(side: int) -> str:
    return "B" if side == SIDE_BUY else "S"


@dataclass
class LiveOrder:
    exch_id: int
    cl_ord_id: str
    symbol: str
    side: int
    ord_type: int
    leaves: int
    orig_qty: int
    limit_price_int: int
    tif: int
    cum_qty: int = 0


@dataclass
class Book:
    bids: list[LiveOrder] = field(default_factory=list)
    asks: list[LiveOrder] = field(default_factory=list)


class ExchangeEngine:
    def __init__(
        self,
        udp_sock: socket.socket,
        allowed_symbols: set[str] | None = None,
    ) -> None:
        self._udp = udp_sock
        self._symbols = allowed_symbols or {"AAPL", "MSFT", "TSLA", "GOOG"}
        self._seq = 0
        self._next_exch_id = 1000
        self._books: dict[str, Book] = {}
        self._by_cl: dict[str, LiveOrder] = {}
        self._open: set[str] = set()
        self._udp_target = (config.EXCHANGE1_UDP_HOST, config.EXCHANGE1_UDP_PORT)

    def startup_broadcast(self) -> None:
        self._emit_system("O")

    def _emit_system(self, event_code: str) -> None:
        self._seq += 1
        ts = int(time.time() * 1000)
        line = f"{self._seq}|{ts}|S|{event_code}"
        print(f"[Exchange 1] Sent UDP: {line}")
        self._udp.sendto(line.encode("utf-8"), self._udp_target)

    def _emit_add(self, o: LiveOrder) -> None:
        self._seq += 1
        ts = int(time.time() * 1000)
        rest = (
            f"{o.exch_id}|{o.symbol}|EQ|{_fix_side_to_pitch(o.side)}|"
            f"{o.leaves}|{o.limit_price_int}"
        )
        line = f"{self._seq}|{ts}|A|{rest}"
        print(f"[Exchange 1] Sent UDP: {line}")
        self._udp.sendto(line.encode("utf-8"), self._udp_target)

    def _emit_exec(self, exch_id: int, qty: int) -> None:
        self._seq += 1
        ts = int(time.time() * 1000)
        line = f"{self._seq}|{ts}|E|{exch_id}|{qty}"
        print(f"[Exchange 1] Sent UDP: {line}")
        self._udp.sendto(line.encode("utf-8"), self._udp_target)

    def _emit_cancel_pitch(self, exch_id: int, qty: int) -> None:
        self._seq += 1
        ts = int(time.time() * 1000)
        line = f"{self._seq}|{ts}|X|{exch_id}|{qty}"
        print(f"[Exchange 1] Sent UDP: {line}")
        self._udp.sendto(line.encode("utf-8"), self._udp_target)

    def _send_er(
        self,
        send: Callable[[bytes], None],
        o: LiveOrder,
        *,
        exec_type: str,
        ord_status: str,
        last_px: str | None = None,
        last_qty: str | None = None,
        text: str | None = None,
    ) -> None:
        msg = execution_report(
            cl_ord_id=o.cl_ord_id,
            order_id=str(o.exch_id),
            exec_type=exec_type,
            ord_status=ord_status,
            cum_qty=str(o.cum_qty),
            last_px=last_px,
            last_qty=last_qty,
            text=text,
        )
        send(msg)

    def handle_fix(
        self,
        fields: dict[str, str],
        send: Callable[[bytes], None],
    ) -> None:
        mt = fields.get("35", "")
        if mt == "D":
            self._on_new_order(fields, send)
        elif mt == "F":
            self._on_cancel(fields, send)
        elif mt == "V":
            self._on_snapshot(fields, send)

    def _reject(self, send: Callable[[bytes], None], cl: str, exch_id: str, msg: str) -> None:
        send(
            execution_report(
                cl_ord_id=cl,
                order_id=exch_id,
                exec_type="8",
                ord_status="8",
                cum_qty="0",
                text=msg,
            )
        )

    def _on_new_order(self, f: dict[str, str], send: Callable[[bytes], None]) -> None:
        try:
            cl = f["11"]
            sym = f["55"]
            side = int(f["54"])
            ot = int(f["40"])
            qty = int(f["38"])
            px_s = f.get("44", "0")
        except (KeyError, ValueError):
            self._reject(send, f.get("11", "?"), "0", "MALFORMED")
            return

        if sym not in self._symbols:
            self._reject(send, cl, "0", "UNKNOWN_SYMBOL")
            return
        if side not in (SIDE_BUY, SIDE_SELL) or ot not in (ORD_MARKET, ORD_LIMIT):
            self._reject(send, cl, "0", "INVALID_SIDE_OR_TYPE")
            return
        if qty <= 0:
            self._reject(send, cl, "0", "INVALID_QTY")
            return
        lim_px = 0 if ot == ORD_MARKET else price_to_int(px_s)
        if ot == ORD_LIMIT and lim_px <= 0:
            self._reject(send, cl, "0", "LIMIT_NEEDS_PRICE")
            return
        tif = TIF_DAY
        if "59" in f:
            try:
                tif = int(f["59"])
            except ValueError:
                self._reject(send, cl, "0", "INVALID_TIF")
                return
        if tif not in (TIF_DAY, TIF_IOC):
            self._reject(send, cl, "0", "UNSUPPORTED_TIF")
            return
        if cl in self._open:
            self._reject(send, cl, "0", "DUPLICATE_CLORDID")
            return

        eid = self._assign_id()
        order = LiveOrder(
            exch_id=eid,
            cl_ord_id=cl,
            symbol=sym,
            side=side,
            ord_type=ot,
            leaves=qty,
            orig_qty=qty,
            limit_price_int=lim_px,
            tif=tif,
            cum_qty=0,
        )
        self._by_cl[cl] = order
        self._open.add(cl)

        self._send_er(send, order, exec_type="0", ord_status="0")
        self._match(order, send)

        if order.leaves > 0:
            if order.ord_type == ORD_MARKET:
                self._send_er(
                    send,
                    order,
                    exec_type="8",
                    ord_status="8",
                    text="NO_LIQUIDITY",
                )
                order.leaves = 0
                self._finalize_order(order)
            elif order.tif == TIF_IOC:
                canceled = order.leaves
                order.leaves = 0
                self._send_er(
                    send,
                    order,
                    exec_type="4",
                    ord_status="4",
                )
                self._emit_cancel_pitch(order.exch_id, canceled)
                self._finalize_order(order)
            else:
                self._book_insert(order)
                self._emit_add(order)
        else:
            self._finalize_order(order)

    def _assign_id(self) -> int:
        x = self._next_exch_id
        self._next_exch_id += 1
        return x

    def _finalize_order(self, o: LiveOrder) -> None:
        self._open.discard(o.cl_ord_id)

    def _remove_from_book(self, o: LiveOrder) -> None:
        book = self._books.get(o.symbol)
        if book is None:
            return
        lst = book.bids if o.side == SIDE_BUY else book.asks
        try:
            lst.remove(o)
        except ValueError:
            pass

    def _book_insert(self, o: LiveOrder) -> None:
        book = self._books.setdefault(o.symbol, Book())
        if o.side == SIDE_BUY:
            book.bids.append(o)
            book.bids.sort(key=lambda x: -x.limit_price_int)
        else:
            book.asks.append(o)
            book.asks.sort(key=lambda x: x.limit_price_int)

    def _match(self, taker: LiveOrder, send: Callable[[bytes], None]) -> None:
        book = self._books.setdefault(taker.symbol, Book())
        while taker.leaves > 0:
            if taker.side == SIDE_BUY:
                opps = book.asks
                if not opps:
                    break
                maker = opps[0]
                if (
                    taker.ord_type == ORD_LIMIT
                    and maker.limit_price_int > taker.limit_price_int
                ):
                    break
            else:
                opps = book.bids
                if not opps:
                    break
                maker = opps[0]
                if (
                    taker.ord_type == ORD_LIMIT
                    and maker.limit_price_int < taker.limit_price_int
                ):
                    break

            px = maker.limit_price_int
            hit = min(taker.leaves, maker.leaves)
            px_str = price_int_to_str(px)

            maker.leaves -= hit
            taker.leaves -= hit
            maker.cum_qty += hit
            taker.cum_qty += hit

            self._emit_exec(maker.exch_id, hit)

            if maker.leaves == 0:
                self._send_er(
                    send,
                    maker,
                    exec_type="2",
                    ord_status="2",
                    last_px=px_str,
                    last_qty=str(hit),
                )
                opps.pop(0)
                self._finalize_order(maker)
            else:
                self._send_er(
                    send,
                    maker,
                    exec_type="1",
                    ord_status="1",
                    last_px=px_str,
                    last_qty=str(hit),
                )

            if taker.leaves == 0:
                self._send_er(
                    send,
                    taker,
                    exec_type="2",
                    ord_status="2",
                    last_px=px_str,
                    last_qty=str(hit),
                )
            else:
                self._send_er(
                    send,
                    taker,
                    exec_type="1",
                    ord_status="1",
                    last_px=px_str,
                    last_qty=str(hit),
                )

    def _on_cancel(self, f: dict[str, str], send: Callable[[bytes], None]) -> None:
        cl = f.get("11", "")
        if not cl or cl not in self._by_cl:
            self._reject(send, cl or "?", "0", "UNKNOWN_ORDER")
            return
        o = self._by_cl[cl]
        if cl not in self._open or o.leaves == 0:
            self._reject(send, cl, str(o.exch_id), "NOT_OPEN")
            return
        self._remove_from_book(o)
        canceled = o.leaves
        o.leaves = 0
        self._emit_cancel_pitch(o.exch_id, canceled)
        self._send_er(send, o, exec_type="4", ord_status="4")
        self._finalize_order(o)

    def _on_snapshot(self, f: dict[str, str], send: Callable[[bytes], None]) -> None:
        sym = f.get("55", "ALL")
        lines: list[str] = [f"SNAPSHOT_SEQ={self._seq}\n"]
        if sym == "ALL":
            symbols = sorted(self._books.keys())
        else:
            symbols = [sym] if sym in self._books else []

        for s in symbols:
            book = self._books[s]
            for o in list(book.bids) + list(book.asks):
                if o.leaves <= 0:
                    continue
                self._seq += 1
                ts = int(time.time() * 1000)
                pitch_rest = (
                    f"{o.exch_id}|{o.symbol}|EQ|{_fix_side_to_pitch(o.side)}|"
                    f"{o.leaves}|{o.limit_price_int}"
                )
                lines.append(f"{self._seq}|{ts}|A|{pitch_rest}\n")

        lines.append("END_SNAPSHOT\n")
        send("".join(lines).encode("utf-8"))
