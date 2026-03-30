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

# ── Multicast groups (market data handler → strategies) ──────────────────────
MULTICAST_GROUP_1 = "239.0.0.1"    # TODO: define asset class partition
MULTICAST_GROUP_2 = "239.0.0.2"    # TODO: define asset class partition
MULTICAST_PORT_1  = 7001
MULTICAST_PORT_2  = 7002

# ── Internal Order Gateway ────────────────────────────────────────────────────
IOG_HOST = "127.0.0.1"
IOG_PORT = 8001                    # strategies send FIX orders here
