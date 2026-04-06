"""
Direct FIX 4.2 client for Exchange 1 order entry (debug / class demos only).

By default, acts as a minimal **market maker loop**: every ``--interval`` seconds,
places a bid and an ask on **each** Exchange 1 symbol (AAPL, MSFT, TSLA, GOOG by
default — same set as ``ExchangeEngine``), then repeats until Ctrl+C. Each wave
uses fresh ClOrdIDs so the exchange accepts new quotes.

With ``--single``, sends one NewOrderSingle instead (no loop).

Connects to EXCHANGE1_FIX_HOST:EXCHANGE1_FIX_PORT using 49=IOG / 56=EXCH1.
Does not use the Internal Order Gateway or MDH.

Run from repo root (or any directory)::

    python exchange1_mm/exchange1_fix_client.py --theo 150 --spread 0.5 --interval 2
    python exchange1_mm/exchange1_fix_client.py --symbol MSFT --interval 2
    python exchange1_mm/exchange1_fix_client.py --single --side buy --price 150.25
"""

from __future__ import annotations

import argparse
import socket
import sys
import time
from pathlib import Path

# Allow `python exchange1_mm/exchange1_fix_client.py` from any cwd.
_repo_root = Path(__file__).resolve().parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import config
from exchange1.fix import build_fix, parse_fix_message, try_pop_message

ORD_LIMIT = 2
ORD_MARKET = 1
SIDE_BUY = 1
SIDE_SELL = 2

# Must match ExchangeEngine default allowed set (exchange1/engine.py).
EXCHANGE1_DEFAULT_SYMBOLS: tuple[str, ...] = ("AAPL", "MSFT", "TSLA", "GOOG")

# Demo mids when --theo is only used as fallback for unknown symbols.
_THEO_BY_SYMBOL: dict[str, float] = {
    "AAPL": 175.0,
    "MSFT": 380.0,
    "TSLA": 250.0,
    "GOOG": 165.0,
}


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


def _symbols_from_args(symbol: str | None, symbols_csv: str | None) -> list[str]:
    if symbol:
        return [symbol.upper()]
    if symbols_csv:
        return [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]
    return list(EXCHANGE1_DEFAULT_SYMBOLS)


def _theo_for_symbol(symbol: str, fallback_theo: float) -> float:
    return _THEO_BY_SYMBOL.get(symbol, fallback_theo)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=(
            "Exchange 1 debug FIX client: default MM loops all tickers every --interval, "
            "or one order with --single."
        ),
    )
    p.add_argument("--host", default=config.EXCHANGE1_FIX_HOST, help="FIX server host")
    p.add_argument("--port", type=int, default=config.EXCHANGE1_FIX_PORT, help="FIX server port")
    p.add_argument(
        "--symbol",
        default=None,
        help="single ticker to quote (default: all Exchange 1 symbols)",
    )
    p.add_argument(
        "--symbols",
        default=None,
        metavar="CSV",
        help="comma-separated tickers (overrides default all; --symbol wins if both set)",
    )
    p.add_argument(
        "--theo",
        type=float,
        default=150.25,
        help=(
            "fallback mid when symbol has no built-in demo price; "
            "built-ins used for AAPL/MSFT/TSLA/GOOG unless you rely on this for custom --symbols"
        ),
    )
    p.add_argument(
        "--spread",
        type=float,
        default=1.0,
        help="full bid-ask width: bid at theo - spread/2, ask at theo + spread/2",
    )
    p.add_argument("--qty", type=int, default=100)
    p.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="seconds to sleep after each full pass over all tickers (MM mode only)",
    )
    p.add_argument(
        "--wave-drain-ms",
        type=float,
        default=0.35,
        help="seconds to read ExecutionReports after each symbol pair (MM loop)",
    )
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
        if args.symbol:
            sym_single = args.symbol.upper()
        elif args.symbols:
            parts = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
            sym_single = parts[0] if parts else "AAPL"
        else:
            sym_single = "AAPL"
        cl = args.cl_ord_id or f"DEBUG-{int(time.time() * 1000)}"
        payloads = [
            build_new_order_single(
                cl_ord_id=cl,
                symbol=sym_single,
                side=side,
                ord_type=ord_type,
                qty=args.qty,
                price=float(args.price),
            )
        ]
        print(f"connecting to {addr[0]}:{addr[1]} single order ClOrdID={cl}", file=sys.stderr)
    else:
        symbols = _symbols_from_args(args.symbol, args.symbols)
        if args.spread <= 0:
            print("--spread must be positive for market-making", file=sys.stderr)
            return 2
        if args.interval <= 0:
            print("--interval must be positive", file=sys.stderr)
            return 2

    try:
        with socket.create_connection(addr, timeout=10.0) as sock:
            if args.single:
                for blob in payloads:
                    sock.sendall(blob)
                _drain_incoming(
                    sock,
                    overall_deadline=time.monotonic() + args.recv_seconds,
                    max_messages=args.max_msgs,
                )
            else:
                half = args.spread / 2.0
                print(
                    f"connecting to {addr[0]}:{addr[1]} MM loop symbols={symbols} "
                    f"interval={args.interval}s spread={args.spread} (Ctrl+C to stop)",
                    file=sys.stderr,
                )
                wave = 0
                while True:
                    wave += 1
                    ts = int(time.time() * 1000)
                    prefix = args.cl_ord_id or "MM"
                    for sym in symbols:
                        theo = _theo_for_symbol(sym, args.theo)
                        bid_px = theo - half
                        ask_px = theo + half
                        if bid_px <= 0:
                            print(
                                f"[wave {wave}] {sym}: bid would be <= 0; skip (adjust --theo/--spread)",
                                file=sys.stderr,
                            )
                            continue
                        base = f"{prefix}{ts}-w{wave}-{sym}"
                        cl_bid, cl_ask = f"{base}-B", f"{base}-S"
                        sock.sendall(
                            build_new_order_single(
                                cl_ord_id=cl_bid,
                                symbol=sym,
                                side=SIDE_BUY,
                                ord_type=ORD_LIMIT,
                                qty=args.qty,
                                price=bid_px,
                            )
                        )
                        sock.sendall(
                            build_new_order_single(
                                cl_ord_id=cl_ask,
                                symbol=sym,
                                side=SIDE_SELL,
                                ord_type=ORD_LIMIT,
                                qty=args.qty,
                                price=ask_px,
                            )
                        )
                        print(
                            f"[wave {wave}] {sym} bid={_format_price(bid_px)} ask={_format_price(ask_px)}",
                            file=sys.stderr,
                        )
                        _drain_incoming(
                            sock,
                            overall_deadline=time.monotonic() + args.wave_drain_ms,
                            max_messages=args.max_msgs,
                        )
                    time.sleep(args.interval)
    except KeyboardInterrupt:
        print("stopped by user", file=sys.stderr)
        return 0
    except OSError as e:
        print(f"socket error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
