"""
FIX 4.2 helpers for Exchange 1 (SOH-delimited, tag 10 checksum).

Checksum: sum of every byte strictly before the first ``10=`` (excludes the
``10=NNN`` field and the final SOH), modulo 256, as three decimal digits.
Matches docs/exchange1.md §3.1 and typical IOG builders.
"""

from __future__ import annotations

SOH = b"\x01"


def compute_checksum(prefix: bytes) -> int:
    return sum(prefix) % 256


def build_fix(fields: list[tuple[str, str]]) -> bytes:
    """Join tag=value pairs with SOH, append checksum tag and trailing SOH."""
    body = SOH.join(f"{tag}={val}".encode("ascii") for tag, val in fields) + SOH
    cs = compute_checksum(body)
    return body + f"10={cs:03d}".encode("ascii") + SOH


def try_pop_message(buf: bytearray) -> bytes | None:
    """Remove one complete FIX message from buf, or return None if incomplete."""
    marker = b"10="
    search = 0
    while search <= len(buf) - 7:
        pos = buf.find(marker, search)
        if pos == -1:
            return None
        if pos + 6 > len(buf):
            return None
        dig = buf[pos + 3 : pos + 6]
        if not all(48 <= b <= 57 for b in dig):
            search = pos + 3
            continue
        if buf[pos + 6] != SOH[0]:
            search = pos + 1
            continue
        end_exclusive = pos + 7
        raw = bytes(buf[:end_exclusive])
        cksum = int(dig.decode("ascii"))
        prefix = raw[:pos]
        if compute_checksum(prefix) != cksum:
            search = pos + 1
            continue
        del buf[:end_exclusive]
        return raw
    return None


def parse_fix_message(msg: bytes) -> dict[str, str]:
    """Parse tag=value pairs; message includes validated ``10=`` trailer."""
    pos = msg.rfind(b"10=")
    if pos == -1:
        raise ValueError("missing checksum field")
    body = msg[:pos]
    out: dict[str, str] = {}
    for part in body.split(SOH):
        if not part or b"=" not in part:
            continue
        tag, _, val = part.partition(b"=")
        out[tag.decode("ascii")] = val.decode("ascii")
    return out


def execution_report(
    *,
    cl_ord_id: str,
    order_id: str,
    exec_type: str,
    ord_status: str,
    cum_qty: str = "0",
    last_px: str | None = None,
    last_qty: str | None = None,
    text: str | None = None,
) -> bytes:
    """Build MsgType 8 toward IOG (49=EXCH1, 56=IOG)."""
    fields: list[tuple[str, str]] = [
        ("8", "FIX.4.2"),
        ("35", "8"),
        ("49", "EXCH1"),
        ("56", "IOG"),
        ("11", cl_ord_id),
        ("37", order_id),
        ("150", exec_type),
        ("39", ord_status),
        ("14", cum_qty),
    ]
    if last_px is not None:
        fields.append(("31", last_px))
    if last_qty is not None:
        fields.append(("32", last_qty))
    if text:
        fields.append(("58", text))
    return build_fix(fields)
