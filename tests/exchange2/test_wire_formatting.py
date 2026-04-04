# test_wire_formatting.py

import re
import time
import pytest

import exchange2 as ex
import config


# ---------- Helpers ----------

def split_fix(msg: str):
    parts = msg.split(ex.SOH)
    fields = {}
    for part in parts:
        if "=" in part:
            k, v = part.split("=", 1)
            fields[k] = v
    return fields


# ---------- FIX MESSAGE TESTS ----------

def test_build_fix_message_structure():
    tags = [("35", "D"), ("11", "123"), ("55", "AAPL")]
    msg = ex.build_fix_message(tags)

    assert msg.startswith(f"8=FIX.4.2{ex.SOH}")
    assert f"35=D{ex.SOH}" in msg
    assert f"11=123{ex.SOH}" in msg
    assert f"55=AAPL{ex.SOH}" in msg
    assert re.search(rf"10=\d{{3}}{ex.SOH}$", msg)


def test_fix_checksum_valid():
    tags = [("35", "D"), ("11", "123")]
    msg = ex.build_fix_message(tags)

    without_checksum = msg.split("10=")[0]
    extracted_checksum = int(msg.split("10=")[-1][:3])

    computed = sum(without_checksum.encode()) % 256
    assert extracted_checksum == computed


def test_parse_fix_replaces_pipe_with_soh():
    raw = "35=D|11=ABC|55=AAPL"
    parsed = ex.parse_fix(raw)

    assert parsed["35"] == "D"
    assert parsed["11"] == "ABC"
    assert parsed["55"] == "AAPL"


def test_build_exec_report_fields():
    msg = ex.build_exec_report("123", "0", "0", "ok")
    fields = split_fix(msg)

    assert fields["35"] == "8"
    assert fields["11"] == "123"
    assert fields["150"] == "0"
    assert fields["39"] == "0"
    assert fields["58"] == "ok"
    assert fields["49"] == "EXCHANGE2"
    assert fields["56"] == "CLIENT"


def test_handle_fix_new_order():
    raw = f"35=D{ex.SOH}11=XYZ{ex.SOH}55=AAPL"
    reply = ex.handle_fix_message(raw)

    fields = split_fix(reply)
    assert fields["39"] == "0"
    assert fields["150"] == "0"


def test_handle_fix_cancel():
    raw = f"35=F{ex.SOH}11=XYZ"
    reply = ex.handle_fix_message(raw)

    fields = split_fix(reply)
    assert fields["39"] == "4"
    assert fields["150"] == "4"


def test_handle_fix_unknown_type():
    raw = f"35=Z{ex.SOH}11=XYZ"
    reply = ex.handle_fix_message(raw)

    fields = split_fix(reply)
    assert fields["39"] == "8"
    assert fields["150"] == "8"


# ---------- MARKET DATA TESTS ----------

def test_build_market_update_format(monkeypatch):
    monkeypatch.setattr(ex.random, "choice", lambda x: x[0])
    monkeypatch.setattr(ex.random, "randint", lambda a, b: a)
    monkeypatch.setattr(time, "time", lambda: 1234567890.0)

    msg = ex.build_market_update(1).decode()
    parts = msg.split("|")

    assert len(parts) == 8

    asset_class, seq, update_type, symbol, side, price, qty, timestamp = parts

    # Validate against config constants
    assert int(asset_class) in [
        config.ASSET_EQUITIES,
        config.ASSET_OPTIONS,
        config.ASSET_FUTURES,
    ]

    assert seq == "1"
    assert update_type == "NEW_ORDER"
    assert symbol in ["AAPL", "MSFT", "ES"]

    assert int(side) in [config.SIDE_BUY, config.SIDE_SELL]

    assert price.isdigit()
    assert qty.isdigit()
    assert float(timestamp) == 1234567890.0


def test_market_update_bytes():
    msg = ex.build_market_update(42)

    assert isinstance(msg, bytes)

    decoded = msg.decode()
    parts = decoded.split("|")

    assert len(parts) == 8


# ---------- CONFIG SANITY TESTS ----------

def test_exchange2_udp_config():
    # Ensures tests align with actual runtime wiring
    assert config.EXCHANGE2_UDP_HOST == "127.0.0.1"
    assert isinstance(config.EXCHANGE2_UDP_PORT, int)


def test_exchange2_fix_config():
    assert config.EXCHANGE2_FIX_HOST == "127.0.0.1"
    assert isinstance(config.EXCHANGE2_FIX_PORT, int)


def test_iog_config():
    assert config.IOG_HOST == "127.0.0.1"
    assert isinstance(config.IOG_PORT, int)


def test_multicast_partitions_defined():
    assert len(config.MULTICAST_PARTITIONS) >= 5

    for group, port in config.MULTICAST_PARTITIONS:
        assert group.startswith("239.")
        assert isinstance(port, int)