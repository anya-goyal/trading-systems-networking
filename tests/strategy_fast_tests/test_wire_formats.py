import struct
import strategy_fast as sf
import config


# -----------------------------
# MDH Format Tests
# -----------------------------

def build_mdh_packet_full(
    msg_type=1,
    order_id=12345678,
    seq_no=10,
    asset_class=1,
    symbol="AAPL",
    side=1,
    update_type=1,
    price=150.25,
    qty=100,
    timestamp=1710000000,
):
    symbol_bytes = symbol.encode().ljust(8, b"\x00")

    body = struct.pack(
        sf.MDH_BODY_FMT,
        msg_type,
        order_id,
        seq_no,
        asset_class,
        symbol_bytes,
        side,
        update_type,
        price,
        qty,
        timestamp,
    )

    header = struct.pack(sf.MDH_HDR_FMT, len(body))
    return header + body


def test_mdh_length_prefix_correct():
    packet = build_mdh_packet_full()
    body_len = struct.unpack_from("!H", packet, 0)[0]

    assert body_len == sf.MDH_BODY_SIZE


def test_mdh_symbol_padding_and_decoding():
    packet = build_mdh_packet_full(symbol="GOOG")
    msg = sf.parse_mdh_packet(packet)

    assert msg.symbol == "GOOG"


def test_mdh_order_id_is_int():
    packet = build_mdh_packet_full(order_id=987654321)
    msg = sf.parse_mdh_packet(packet)

    assert isinstance(msg.order_id, int)
    assert msg.order_id == 987654321


def test_mdh_fixed_body_size():
    assert sf.MDH_BODY_SIZE == struct.calcsize(sf.MDH_BODY_FMT)


def test_mdh_unpack_field_alignment():
    packet = build_mdh_packet_full(symbol="MSFT", price=200.75, qty=42)
    msg = sf.parse_mdh_packet(packet)

    assert msg.price == 200.75
    assert msg.qty == 42
    assert msg.symbol == "MSFT"
    assert isinstance(msg.order_id, int)


# -----------------------------
# Strategy → IOG Format Tests
# -----------------------------

def test_iog_order_fixed_size():
    _, raw = sf.build_new_order("AAPL", config.SIDE_BUY, 10, 100.5)

    assert len(raw) == sf.IOG_ORDER_SIZE


def test_iog_length_prefix():
    _, raw = sf.build_new_order("AAPL", config.SIDE_BUY, 10, 100.5)

    (body_len,) = struct.unpack_from("!H", raw, 0)
    expected_body_len = sf.IOG_ORDER_SIZE - 2

    assert body_len == expected_body_len


def test_iog_message_type():
    _, raw = sf.build_new_order("AAPL", config.SIDE_BUY, 10, 100.5)

    (_, msg_type) = struct.unpack_from("!HB", raw, 0)
    assert msg_type == sf.IOG_NEW_ORDER


def test_iog_client_order_id_padding():
    cl_id, raw = sf.build_new_order("AAPL", config.SIDE_BUY, 10, 100.5)

    client_id_bytes = struct.unpack_from("16s", raw, 3)[0]

    assert cl_id.encode() == client_id_bytes.rstrip(b"\x00")
    assert len(client_id_bytes) == 16


def test_iog_symbol_padding():
    _, raw = sf.build_new_order("AAPL", config.SIDE_BUY, 10, 100.5)

    symbol_bytes = struct.unpack_from("8s", raw, 19)[0]
    assert symbol_bytes.rstrip(b"\x00") == b"AAPL"
    assert len(symbol_bytes) == 8


def test_iog_side_and_order_type():
    _, raw = sf.build_new_order("AAPL", config.SIDE_BUY, 10, 100.5)

    side = struct.unpack_from("B", raw, 27)[0]
    order_type = struct.unpack_from("B", raw, 28)[0]

    assert side == config.SIDE_BUY
    assert order_type == sf.ORD_LIMIT


def test_iog_quantity_and_price():
    _, raw = sf.build_new_order("AAPL", config.SIDE_BUY, 123, 99.75)

    qty = struct.unpack_from("!I", raw, 29)[0]
    price = struct.unpack_from("!d", raw, 33)[0]

    assert qty == 123
    assert price == 99.75


def test_iog_destination_and_strategy_id():
    _, raw = sf.build_new_order("AAPL", config.SIDE_BUY, 10, 100.5)

    dest = struct.unpack_from("B", raw, 41)[0]
    strategy_id = struct.unpack_from("8s", raw, 42)[0]

    assert dest == sf.DEST_EXCHANGE_1
    assert strategy_id == sf.STRATEGY_ID


def test_iog_symbol_truncation():
    long_symbol = "TOOLONGSYM"
    _, raw = sf.build_new_order(long_symbol, config.SIDE_BUY, 10, 100.5)

    symbol_bytes = struct.unpack_from("8s", raw, 19)[0]

    # Should be truncated to 8 bytes
    assert len(symbol_bytes) == 8
    assert symbol_bytes == long_symbol.encode()[:8]