import strategy_fast as sf
import config


def make_msg(
    symbol="AAPL",
    side=config.SIDE_BUY,
    update_type=config.UPDATE_NEW_ORDER,
    price=100.0,
    qty=10,
    seq_no=1,
):
    return sf.MDHMessage(
        msg_type=1,
        order_id=123,
        seq_no=seq_no,
        asset_class=1,
        symbol=symbol,
        side=side,
        update_type=update_type,
        price=price,
        qty=qty,
    )


def test_update_snapshot_initial_creation():
    store = {}
    msg = make_msg()

    snap = sf.update_snapshot(store, msg)

    assert msg.symbol in store
    assert snap.symbol == "AAPL"
    assert snap.best_bid == 100.0
    assert snap.bid_qty == 10


def test_update_snapshot_bid_updates():
    store = {}
    
    msg1 = make_msg(symbol="AAPL", side=config.SIDE_BUY, price=100.0, qty=10)
    sf.update_snapshot(store, msg1)

    msg2 = make_msg(symbol="AAPL", side=config.SIDE_BUY, price=101.0, qty=20, seq_no=2)
    snap = sf.update_snapshot(store, msg2)

    assert snap.best_bid == 101.0
    assert snap.bid_qty == 20


def test_update_snapshot_ask_updates():
    store = {}

    msg1 = make_msg(symbol="AAPL", side=config.SIDE_SELL, price=105.0, qty=5)
    snap = sf.update_snapshot(store, msg1)

    assert snap.best_ask == 105.0
    assert snap.ask_qty == 5


def test_update_snapshot_bid_and_ask_coexist():
    store = {}

    bid_msg = make_msg(symbol="AAPL", side=config.SIDE_BUY, price=100.0, qty=10)
    ask_msg = make_msg(symbol="AAPL", side=config.SIDE_SELL, price=110.0, qty=5)

    sf.update_snapshot(store, bid_msg)
    snap = sf.update_snapshot(store, ask_msg)

    assert snap.best_bid == 100.0
    assert snap.best_ask == 110.0


def test_update_snapshot_cancel_bid():
    store = {}

    # Set initial bid
    msg1 = make_msg(symbol="AAPL", side=config.SIDE_BUY, price=100.0, qty=10)
    sf.update_snapshot(store, msg1)

    # Cancel bid
    cancel_msg = make_msg(
        symbol="AAPL",
        side=config.SIDE_BUY,
        update_type=config.UPDATE_CANCEL,
    )

    snap = sf.update_snapshot(store, cancel_msg)

    assert snap.best_bid == 0.0
    assert snap.bid_qty == 0


def test_update_snapshot_cancel_ask():
    store = {}

    # Set initial ask
    msg1 = make_msg(symbol="AAPL", side=config.SIDE_SELL, price=110.0, qty=5)
    sf.update_snapshot(store, msg1)

    # Cancel ask
    cancel_msg = make_msg(
        symbol="AAPL",
        side=config.SIDE_SELL,
        update_type=config.UPDATE_CANCEL,
    )

    snap = sf.update_snapshot(store, cancel_msg)

    assert snap.best_ask == 0.0
    assert snap.ask_qty == 0


def test_update_snapshot_does_not_affect_other_symbol():
    store = {}

    msg1 = make_msg(symbol="AAPL", side=config.SIDE_BUY, price=100.0, qty=10)
    msg2 = make_msg(symbol="MSFT", side=config.SIDE_BUY, price=200.0, qty=20)

    sf.update_snapshot(store, msg1)
    sf.update_snapshot(store, msg2)

    assert "AAPL" in store
    assert "MSFT" in store
    assert store["AAPL"].best_bid == 100.0
    assert store["MSFT"].best_bid == 200.0


def test_update_snapshot_seq_no_tracking():
    store = {}

    msg1 = make_msg(symbol="AAPL", seq_no=10)
    msg2 = make_msg(symbol="AAPL", seq_no=20)

    snap = sf.update_snapshot(store, msg1)
    assert snap.last_seq == 10

    snap = sf.update_snapshot(store, msg2)
    assert snap.last_seq == 20