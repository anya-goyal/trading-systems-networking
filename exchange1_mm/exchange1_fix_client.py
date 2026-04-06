"""
Direct FIX 4.2 client for Exchange 1 order entry (debug / class demos only).

By default, acts as a minimal **market maker**: places a bid and an ask around a
theoretical price with a configurable full spread. With ``--single``, sends one
NewOrderSingle instead.

Connects to EXCHANGE1_FIX_HOST:EXCHANGE1_FIX_PORT using 49=IOG / 56=EXCH1.
Does not use the Internal Order Gateway or MDH.

Run from repo root::

    python -m scripts.exchange1_fix_client --symbol AAPL --theo 150 --spread 0.5
    python -m scripts.exchange1_fix_client --single --side buy --price 150.25
"""

from __future__ import annotations

import argparse
import socket
import sys
import time

import config
from exchange1.fix import build_fix, parse_fix_message, try_pop_message

ORD_LIMIT = 2
ORD_MARKET = 1
SIDE_BUY = 1
SIDE_SELL = 2


def _format_price(price: float) -> str:
    if price == int(price):
        return str(int(price))
    return f"{price:g}"


def build_new_order_single(
    *,
    cl_ord_id: str,
    symbol: str,
    side: int,
    ord_type: int,
    qty: int,
    price: float,
) -> bytes:
    px = _format_price(price) if ord_type == ORD_LIMIT else "0"
    fields: list[tuple[str, str]] = [
        ("8", "FIX.4.2"),
        ("35", "D"),
        ("49", "IOG"),
        ("56", "EXCH1"),
        ("11", cl_ord_id),
        ("55", symbol),
        ("54", str(side)),
        ("40", str(ord_type)),
        ("38", str(qty)),
        ("44", px),
    ]
    return build_fix(fields)


def _drain_incoming(
    sock: socket.socket,
    *,
    overall_deadline: float,
    max_messages: int,
) -> None:
    sock.settimeout(0.05)
    buf = bytearray()
    received = 0
    while time.monotonic() < overall_deadline and received < max_messages:
        try:
            chunk = sock.recv(16384)
        except TimeoutError:
            continue
        if not chunk:
            print("connection closed by peer", file=sys.stderr)
            break
        buf.extend(chunk)
        while received < max_messages:
            raw = try_pop_message(buf)
            if raw is None:
                break
            received += 1
            fields = parse_fix_message(raw)
            mt = fields.get("35", "?")
            if mt == "8":
                print(
                    "ExecutionReport "
                    f"11={fields.get('11')} 150={fields.get('150')} "
                    f"39={fields.get('39')} 14={fields.get('14')} "
                    f"31={fields.get('31', '')} 32={fields.get('32', '')} "
                    f"58={fields.get('58', '')}"
                )
            else:
                print(f"MsgType={mt} {dict(sorted(fields.items()))}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=(
            "Exchange 1 debug FIX client: default two-sided quotes around --theo "
            "and --spread, or one order with --single."
        ),
    )
    p.add_argument("--host", default=config.EXCHANGE1_FIX_HOST, help="FIX server host")
    p.add_argument("--port", type=int, default=config.EXCHANGE1_FIX_PORT, help="FIX server port")
    p.add_argument("--symbol", default="AAPL")
    p.add_argument(
        "--theo",
        type=float,
        default=150.25,
        help="theoretical mid for market-making (bid/ask anchored here)",
    )
    p.add_argument(
        "--spread",
        type=float,
        default=1.0,
        help="full bid-ask width: bid at theo - spread/2, ask at theo + spread/2",
    )
    p.add_argument("--qty", type=int, default=100)
    p.add_argument(
        "--single",
        action="store_true",
        help="send a single order using --side / --price / --market instead of two-sided quotes",
    )
    p.add_argument("--side", choices=("buy", "sell", "1", "2"), default="buy")
    p.add_argument("--price", type=float, default=150.25, help="Limit price (with --single; ignored with --market)")
    p.add_argument("--market", action="store_true", help="With --single: OrdType 1; uses 44=0")
    p.add_argument("--cl-ord-id", default="", help="prefix; MM appends -B / -S; default: DEBUG-<unix_ms>")
    p.add_argument(
        "--recv-seconds",
        type=float,
        default=5.0,
        help="keep reading until this many seconds elapse or --max-msgs reached",
    )
    p.add_argument("--max-msgs", type=int, default=32)
    args = p.parse_args(argv)

    addr = (args.host, args.port)

    if args.single:
        side = SIDE_BUY if args.side in ("buy", "1") else SIDE_SELL
        ord_type = ORD_MARKET if args.market else ORD_LIMIT
        cl = args.cl_ord_id or f"DEBUG-{int(time.time() * 1000)}"
        payloads = [
            build_new_order_single(
                cl_ord_id=cl,
                symbol=args.symbol,
                side=side,
                ord_type=ord_type,
                qty=args.qty,
                price=float(args.price),
            )
        ]
        print(f"connecting to {addr[0]}:{addr[1]} single order ClOrdID={cl}", file=sys.stderr)
    else:
        if args.spread <= 0:
            print("--spread must be positive for market-making", file=sys.stderr)
            return 2
        half = args.spread / 2.0
        bid_px = args.theo - half
        ask_px = args.theo + half
        if bid_px <= 0:
            print("bid price would be <= 0; lower --spread or raise --theo", file=sys.stderr)
            return 2
        base = args.cl_ord_id or f"DEBUG-{int(time.time() * 1000)}"
        cl_bid, cl_ask = f"{base}-B", f"{base}-S"
        payloads = [
            build_new_order_single(
                cl_ord_id=cl_bid,
                symbol=args.symbol,
                side=SIDE_BUY,
                ord_type=ORD_LIMIT,
                qty=args.qty,
                price=bid_px,
            ),
            build_new_order_single(
                cl_ord_id=cl_ask,
                symbol=args.symbol,
                side=SIDE_SELL,
                ord_type=ORD_LIMIT,
                qty=args.qty,
                price=ask_px,
            ),
        ]
        print(
            f"connecting to {addr[0]}:{addr[1]} MM theo={args.theo} spread={args.spread} "
            f"bid={_format_price(bid_px)}@{cl_bid} ask={_format_price(ask_px)}@{cl_ask}",
            file=sys.stderr,
        )

    try:
        with socket.create_connection(addr, timeout=10.0) as sock:
            for blob in payloads:
                sock.sendall(blob)
            _drain_incoming(
                sock,
                overall_deadline=time.monotonic() + args.recv_seconds,
                max_messages=args.max_msgs,
            )
    except OSError as e:
        print(f"socket error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
