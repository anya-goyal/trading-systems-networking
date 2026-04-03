# ── Exchange 1 ───────────────────────────────────────────────────────────────
EXCHANGE1_UDP_HOST = "127.0.0.1"
EXCHANGE1_UDP_PORT = 5001          # exchange1 publishes market feed here

EXCHANGE1_FIX_HOST = "127.0.0.1"
EXCHANGE1_FIX_PORT = 6001          # exchange1 listens for FIX orders here

# ── Exchange 2 ───────────────────────────────────────────────────────────────
EXCHANGE2_UDP_HOST = "127.0.0.1"
EXCHANGE2_UDP_PORT = 5002          # exchange2 publishes market feed here

EXCHANGE2_FIX_HOST = "127.0.0.1"
EXCHANGE2_FIX_PORT = 6002          # exchange2 listens for FIX orders here

MULTICAST_PARTITIONS = [
    ("239.1.1.1", 7001),  # Equities A–F
    ("239.1.1.2", 7002),  # Equities G–M
    ("239.1.1.3", 7003),  # Equities N–Z
    ("239.1.2.1", 7004),  # Options
    ("239.1.2.2", 7005),  # Futures
]

MULTICAST_GROUP_1, MULTICAST_PORT_1 = MULTICAST_PARTITIONS[0]
MULTICAST_GROUP_2, MULTICAST_PORT_2 = MULTICAST_PARTITIONS[1]
MULTICAST_GROUP_3, MULTICAST_PORT_3 = MULTICAST_PARTITIONS[2]
MULTICAST_GROUP_4, MULTICAST_PORT_4 = MULTICAST_PARTITIONS[3]
MULTICAST_GROUP_5, MULTICAST_PORT_5 = MULTICAST_PARTITIONS[4]

ASSET_EQUITIES = 1
ASSET_OPTIONS  = 2
ASSET_FUTURES  = 3

MSG_TYPE_SNAPSHOT = 0x01
MSG_TYPE_UPDATE   = 0x02

SIDE_NA   = 0
SIDE_BUY  = 1
SIDE_SELL = 2

UPDATE_NEW_ORDER = 1
UPDATE_CANCEL    = 2
UPDATE_FILL      = 3

# ── Internal Order Gateway ────────────────────────────────────────────────────
IOG_HOST = "127.0.0.1"
IOG_PORT = 8001                    # strategies send FIX orders here

# ── Strategy Fast (Simple) ───────────────────────────────────────────────────
SUBSCRIBED_GROUPS = [
    (MULTICAST_GROUP_1, MULTICAST_PORT_1),
    (MULTICAST_GROUP_2, MULTICAST_PORT_2),
]

SUBSCRIBED_ASSET_CLASSES = [ASSET_EQUITIES]
SUBSCRIBED_SYMBOLS = []             # empty list = all symbols in subscribed partitions

UDP_BUF_SIZE = 4096
IOG_BUF_SIZE = 4096

DEFAULT_ORDER_QTY = 10
MAX_ORDER_RATE = 25                # max outgoing orders per second
SYMBOL_COOLDOWN_SEC = 0.25         # minimum delay between orders per symbol

MIN_SPREAD_TO_TRADE = 0.05         # place order when (best_ask - best_bid) >= this value
