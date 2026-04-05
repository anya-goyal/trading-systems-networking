# Internal Order Gateway — Architecture

## 1. Position in the System

The IOG sits between strategies and exchanges. It receives orders from strategies over TCP (internal format), validates them, checks risk, serializes to FIX, and routes to the correct exchange. It also receives execution reports back from exchanges and forwards them to the originating strategy.

**Inbound:** strategies connect to the IOG on `IOG_HOST:IOG_PORT` (TCP).  
**Outbound:** the IOG maintains persistent TCP connections to each exchange's FIX port.  
**Return path:** execution reports flow back from exchanges through the IOG to the correct strategy.

---

## 2. Order Processing Pipeline

Every order passes through these stages in sequence. Each stage either passes the order forward or rejects it back to the strategy with a reason code.

| Stage | Type | Description |
|-------|------|-------------|
| **Receive** | I/O | Read bytes from strategy TCP connection |
| **Deserialize** | Stateless | Parse raw bytes into an `InternalOrder` struct. Handle length-prefixed framing, partial reads, and malformed data |
| **Validate** | Stateless | Check that all required fields are present and sane (see §5) |
| **Risk Check** | Stateful | Check against position limits, rate limits, kill switch (see §6) |
| **Serialize to FIX** | Stateless | Convert `InternalOrder` → FIX `NewOrderSingle` message bytes |
| **Route** | Stateful | Look up destination exchange, verify connection is alive, send |
| **Book** | Stateful | Record the order in the internal order book for tracking |

---

## 3. Order State Machine

Every order tracked by the IOG has one of these states:

| State | Meaning | Transitions to |
|-------|---------|----------------|
| `RECEIVED` | Bytes received, not yet parsed | `VALIDATED`, `MALFORMED` |
| `VALIDATED` | Passed deserialization and field validation | `RISK_CHECKED`, `RISK_FAIL` |
| `RISK_CHECKED` | Passed all risk checks | `ROUTED`, `ROUTE_FAIL` |
| `ROUTED` | Assigned to an exchange, ready to send | `SENT` |
| `SENT` | FIX message transmitted to exchange | `PARTIALLY_FILLED`, `FILLED`, `REJECTED` |
| `PARTIALLY_FILLED` | Some quantity filled, remainder open | `FILLED`, `CANCELLED` |
| `FILLED` | Fully filled (terminal) | — |
| `REJECTED` | Rejected by IOG or exchange (terminal) | — |
| `CANCELLED` | Cancelled by strategy or IOG (terminal) | — |
| `MALFORMED` | Could not parse (terminal) | — |
| `RISK_FAIL` | Failed risk check (terminal) | — |
| `ROUTE_FAIL` | Exchange down at time of routing (terminal) | — |

**Terminal states** require no further action. Non-terminal states have associated timeouts — if an order sits in `SENT` for longer than a configurable threshold without an execution report, the IOG should log a warning and optionally attempt a status query.

---

## 4. Message Formats

### 4.1 Strategy → IOG (Internal Order Message)

This is the internal protocol between strategies and the IOG. It uses a **length-prefixed binary frame** for efficient parsing and zero ambiguity on message boundaries.

**Frame layout:**

| Offset | Size | Field | Type | Description |
|--------|------|-------|------|-------------|
| 0 | 2 | `msg_length` | uint16 (big-endian) | Total byte length of everything after this field |
| 2 | 1 | `msg_type` | uint8 | Message type: `0x01` = NewOrder, `0x02` = CancelRequest |
| 3 | 16 | `clOrdID` | bytes (utf-8, null-padded) | Client order ID, unique per strategy |
| 19 | 8 | `symbol` | bytes (utf-8, null-padded) | Instrument symbol, e.g. `AAPL` |
| 27 | 1 | `side` | uint8 | `1` = BUY, `2` = SELL |
| 28 | 1 | `order_type` | uint8 | `1` = MARKET, `2` = LIMIT |
| 29 | 4 | `qty` | uint32 (big-endian) | Number of shares/contracts |
| 33 | 8 | `price` | float64 (big-endian) | Limit price. Ignored if MARKET. Set to 0.0 |
| 41 | 1 | `destination` | uint8 | `1` = EXCH1, `2` = EXCH2 |
| 42 | 8 | `strategy_id` | bytes (utf-8, null-padded) | Identifies the sending strategy |

**Total NewOrder size:** 2 (length prefix) + 48 (body) = **50 bytes fixed**.

**`struct` format string:** `!HB16s8sBBIdB8s`

**Notes:**
- The 2-byte length prefix enables the receiver to handle partial TCP reads: read 2 bytes first, then read exactly that many more bytes.
- Null-padded fixed-width strings avoid the need for delimiters or nested length prefixes.
- All multi-byte integers use network byte order (big-endian, `!` prefix in `struct`).

### 4.2 IOG → Strategy (Execution Report / Reject)

Sent back to the strategy when an order is rejected (by IOG or exchange) or filled.

| Offset | Size | Field | Type | Description |
|--------|------|-------|------|-------------|
| 0 | 2 | `msg_length` | uint16 | Length of everything after this field |
| 2 | 1 | `msg_type` | uint8 | `0x10` = Ack, `0x11` = Fill, `0x12` = PartialFill, `0x13` = Reject, `0x14` = Cancelled |
| 3 | 16 | `clOrdID` | bytes (utf-8, null-padded) | References the original order |
| 19 | 1 | `reject_reason` | uint8 | `0` = n/a, `1` = validation fail, `2` = risk fail, `3` = exchange down, `4` = exchange reject, `5` = kill switch |
| 20 | 4 | `filled_qty` | uint32 | Quantity filled in this report (0 if reject) |
| 24 | 4 | `cumulative_qty` | uint32 | Total filled so far |
| 28 | 8 | `fill_price` | float64 | Price of this fill (0.0 if reject) |
| 36 | 8 | `timestamp` | float64 | IOG timestamp (seconds since epoch) |

**Total size:** 2 + 42 = **44 bytes fixed**.

**`struct` format string:** `!HB16sBIIdd`

### 4.3 IOG → Exchange (FIX NewOrderSingle)

The IOG converts internal orders to FIX 4.2 tag-value format. FIX messages use `SOH` (`\x01`) as the delimiter between fields.

**Key FIX tags used:**

| Tag | Name | Maps from |
|-----|------|-----------|
| 8 | BeginString | Always `FIX.4.2` |
| 35 | MsgType | `D` (NewOrderSingle) or `F` (CancelRequest) |
| 49 | SenderCompID | `IOG` |
| 56 | TargetCompID | `EXCH1` or `EXCH2` |
| 11 | ClOrdID | `clOrdID` from internal message |
| 55 | Symbol | `symbol` from internal message |
| 54 | Side | `1` (Buy) or `2` (Sell) — same encoding |
| 40 | OrdType | `1` (Market) or `2` (Limit) — same encoding |
| 38 | OrderQty | `qty` from internal message |
| 44 | Price | `price` from internal message |
| 10 | CheckSum | Computed: sum of all bytes mod 256, zero-padded to 3 digits |

**Example FIX message (pipes shown instead of SOH for readability):**
```
8=FIX.4.2|35=D|49=IOG|56=EXCH1|11=ORD001|55=AAPL|54=1|40=2|38=100|44=150.25|10=078|
```

### 4.4 Exchange → IOG (FIX Execution Report)

The exchange sends back FIX `ExecutionReport` messages (MsgType `8`).

**Key FIX tags parsed:**

| Tag | Name | Used for |
|-----|------|----------|
| 35 | MsgType | `8` = ExecutionReport |
| 11 | ClOrdID | Route back to correct strategy |
| 150 | ExecType | `0` = New (ACK), `1` = PartialFill, `2` = Fill, `8` = Reject |
| 14 | CumQty | Total filled |
| 31 | LastPx | Price of this fill |
| 32 | LastQty | Quantity of this fill |
| 58 | Text | Reject reason (free text) |

---

## 5. Validation Layer (Stateless)

These checks run before any stateful risk check. They are pure functions of the order message.

| Check | Reject condition | Reject reason code |
|-------|------------------|--------------------|
| Required fields | Any of `clOrdID`, `symbol`, `side`, `qty`, `destination` is zero/empty | `VALIDATION_FAIL` |
| Side | Not `1` (BUY) or `2` (SELL) | `VALIDATION_FAIL` |
| Order type | Not `1` (MARKET) or `2` (LIMIT) | `VALIDATION_FAIL` |
| Quantity | `qty == 0` | `VALIDATION_FAIL` |
| Price | `order_type == LIMIT` and `price <= 0.0` | `VALIDATION_FAIL` |
| Destination | Not `1` (EXCH1) or `2` (EXCH2) | `VALIDATION_FAIL` |
| Duplicate clOrdID | `clOrdID` already exists in the order book from this strategy | `VALIDATION_FAIL` |
| Symbol | Symbol not in the known symbol set | `VALIDATION_FAIL` |

---

## 6. Risk Check Layer (Stateful)

These checks depend on the current state of positions, order counts, and global flags.

| Check | State consulted | Reject condition | Reject reason code |
|-------|----------------|-------------------|--------------------|
| Kill switch | `kill_switch: bool` | `kill_switch == True` | `KILL_SWITCH` |
| Max order size | Config: `MAX_ORDER_QTY` | `qty > MAX_ORDER_QTY` | `RISK_FAIL` |
| Max notional | Config: `MAX_NOTIONAL` | `price × qty > MAX_NOTIONAL` | `RISK_FAIL` |
| Position limit | `positions: {symbol → net_qty}` | Projected position exceeds `MAX_POSITION_PER_SYMBOL` | `RISK_FAIL` |
| Rate limit | `order_timestamps: {strategy_id → deque}` | More than `MAX_ORDERS_PER_SEC` in the last 1 second | `RISK_FAIL` |

**Position projection logic:**
- BUY: `projected = positions[symbol] + qty`
- SELL: `projected = positions[symbol] - qty`
- Reject if `abs(projected) > MAX_POSITION_PER_SYMBOL`

**Position updates happen on fills, not on order sends** — this prevents phantom position inflation from unfilled orders.

---

## 7. Connection Management

### 7.1 Strategy Connections (Inbound)

| Event | IOG action |
|-------|-----------|
| New connection on `:8001` | `accept()`, register with `selectors`, create `StrategySession` |
| `recv()` returns data | Feed bytes to deserializer, process complete messages |
| `recv()` returns `b""` | Strategy disconnected. Cancel all open orders for this strategy on exchanges. Remove session. |
| `ConnectionResetError` | Same as `b""` — treat as disconnect |

### 7.2 Exchange Connections (Outbound)

| Event | IOG action |
|-------|-----------|
| Startup | `connect()` to each exchange. If fail, mark `DOWN`, start backoff |
| `send()` succeeds | Normal path |
| `send()` raises `BrokenPipeError` | Mark exchange `DOWN`. Reject all pending orders for this exchange. Start reconnect backoff |
| No heartbeat received in `HB_TIMEOUT` seconds | Mark exchange `STALE`, then `DOWN` if still no response |
| Reconnect timer fires | Attempt `connect()`. If success, mark `CONNECTED`, reset backoff. If fail, double backoff (cap at 30s) |

### 7.3 Connection States

| State | Meaning | Orders accepted? |
|-------|---------|-----------------|
| `CONNECTING` | TCP handshake in progress | No |
| `CONNECTED` | Active, heartbeats OK | Yes |
| `STALE` | Heartbeat overdue, not yet timed out | Yes (with warning log) |
| `DOWN` | Confirmed dead, reconnect scheduled | No |

---

## 8. Error Handling

### 8.1 Strategy Disconnect with Open Orders

When a strategy TCP connection drops, the IOG must:

1. Identify all orders from this strategy that are in a non-terminal state (`SENT`, `PARTIALLY_FILLED`)
2. Send a FIX `OrderCancelRequest` (MsgType `F`) for each to the relevant exchange
3. Update each order's state to `CANCELLED` upon receiving cancel acknowledgment
4. Remove the `StrategySession` and deregister the socket from `selectors`

### 8.2 Exchange Goes Down

When an exchange TCP connection is lost:

1. Mark exchange as `DOWN`
2. For every order in `SENT` state destined for that exchange: mark as `ROUTE_FAIL`, notify the originating strategy with a reject (reason: `EXCH_DOWN`)
3. Reject all new orders targeting that exchange
4. Begin reconnection attempts with exponential backoff: 1s → 2s → 4s → 8s → 16s → 30s (cap)
5. On successful reconnect: mark `CONNECTED`, reset backoff, resume accepting orders

### 8.3 Malformed Message from Strategy

1. Log the raw bytes for debugging
2. Send a reject back to the strategy (reason: `VALIDATION_FAIL`)
3. **Do not** close the connection — one bad message should not kill the session

### 8.4 Graceful Shutdown (SIGINT / SIGTERM)

1. Set `kill_switch = True` (prevent new orders immediately)
2. Send cancel requests for all open orders on all exchanges
3. Wait briefly for cancel acknowledgments
4. Send session-termination messages to all connected strategies
5. Close all sockets
6. Log final state (positions, order counts, any orders that didn't get cancel-acked)

### 8.5 Kill Switch Activation

The kill switch can be activated programmatically (e.g., on detecting a runaway strategy) or externally (e.g., a management command):

1. Set `kill_switch = True` in risk state
2. Cancel all open orders across all exchanges
3. Reject all new orders with reason `KILL_SWITCH`
4. Remain running — strategies stay connected, but no orders pass through
5. Requires explicit reset to resume

---

## 9. Data Structures

### 9.1 InternalOrder
Parsed from the binary message received from a strategy.

| Field | Type | Description |
|-------|------|-------------|
| `clOrdID` | `str` | Unique order ID from the strategy |
| `symbol` | `str` | Instrument symbol |
| `side` | `int` | `1` = BUY, `2` = SELL |
| `order_type` | `int` | `1` = MARKET, `2` = LIMIT |
| `qty` | `int` | Order quantity |
| `price` | `float` | Limit price (0.0 for MARKET) |
| `destination` | `int` | `1` = EXCH1, `2` = EXCH2 |
| `strategy_id` | `str` | Originating strategy |

### 9.2 OrderEntry
Stored in the IOG's in-memory order book, keyed by `clOrdID`.

| Field | Type | Description |
|-------|------|-------------|
| `order` | `InternalOrder` | The original order |
| `state` | `OrderState` | Current state in the state machine |
| `strategy_fd` | `int` | Socket fd to route replies back to the strategy |
| `filled_qty` | `int` | Cumulative quantity filled |
| `avg_fill_px` | `float` | Volume-weighted average fill price |
| `timestamps` | `dict` | `{state_name: time}` for each transition |
| `fix_msg` | `bytes` | The FIX message that was sent to the exchange |

### 9.3 StrategySession
One per accepted strategy TCP connection.

| Field | Type | Description |
|-------|------|-------------|
| `socket` | `socket` | The TCP connection |
| `strategy_id` | `str` | Identifier (set after first message or handshake) |
| `connected_at` | `float` | `time.time()` at accept |
| `recv_buffer` | `bytearray` | Accumulates partial reads |
| `open_orders` | `set[str]` | `clOrdID`s in non-terminal states |
| `order_count` | `int` | Total orders received this session |

### 9.4 ExchangeConn
One per exchange.

| Field | Type | Description |
|-------|------|-------------|
| `socket` | `socket` | The TCP connection |
| `exchange_id` | `str` | `"EXCH1"` or `"EXCH2"` |
| `state` | `ConnState` | `CONNECTING`, `CONNECTED`, `STALE`, or `DOWN` |
| `last_heartbeat` | `float` | Timestamp of last heartbeat received |
| `reconnect_at` | `float` | When to attempt next reconnect |
| `backoff` | `float` | Current backoff interval (1 → 2 → 4 → ... → 30) |
| `recv_buffer` | `bytearray` | Accumulates partial FIX message reads |

### 9.5 RiskState
Global mutable state consulted during risk checks.

| Field | Type | Description |
|-------|------|-------------|
| `positions` | `dict[str, int]` | `{symbol: net_qty}` — positive = long, negative = short |
| `order_timestamps` | `dict[str, deque]` | `{strategy_id: deque_of_timestamps}` for rate limiting |
| `kill_switch` | `bool` | If `True`, reject all new orders |

---

## 10. Component Map

The IOG is organized into these logical components. Each can be a class or a module depending on preference.

### ConnectionManager
- `accept_strategy(listener_sock)` → registers new strategy connection
- `remove_strategy(fd)` → cleans up session, deregisters from selector
- `connect_exchange(exchange_id)` → establishes outbound TCP to exchange
- `reconnect_exchange(exchange_id)` → called on backoff timer, re-establishes connection
- `get_exchange_conn(exchange_id)` → returns `ExchangeConn` or `None` if down

### Deserializer
- `feed_bytes(session, raw_bytes)` → appends to `recv_buffer`, extracts complete frames
- `next_message(session)` → returns `InternalOrder` or `None` if incomplete

### Validator
- `validate(order)` → returns `(True, None)` or `(False, reason_code)`

### RiskChecker
- `check(order)` → returns `(True, None)` or `(False, reason_code)`
- `update_position(symbol, side, qty)` → called on fills
- `activate_kill_switch()` → sets flag, triggers cancel-all
- `reset_kill_switch()` → clears flag, resumes normal operation

### FIXSerializer
- `to_fix_new_order(order)` → returns FIX `NewOrderSingle` bytes
- `to_fix_cancel(clOrdID, symbol, side, dest)` → returns FIX `OrderCancelRequest` bytes
- `from_fix_exec_report(raw_bytes)` → returns parsed `ExecReport` dict

### Router
- `route(order, fix_bytes)` → sends to correct exchange or returns reject

### ExecReportHandler
- `handle(exchange_id, report)` → updates order book, updates risk state, forwards to strategy

### OrderBook
- `add(clOrdID, order_entry)` → stores new order
- `update_state(clOrdID, new_state)` → transitions state, records timestamp
- `get_open_orders(strategy_fd)` → all non-terminal orders for a given strategy
- `get(clOrdID)` → returns `OrderEntry` or `None`

### ShutdownHandler
- `graceful_shutdown()` → cancel-all, notify strategies, close sockets, log

---

## 11. Event Loop Structure

The IOG uses Python's `selectors` module to multiplex all I/O in a single thread.

**Registered sockets and their handlers:**

| Socket | Event | Handler |
|--------|-------|---------|
| Listener (`:8001`) | `EVENT_READ` | `ConnectionManager.accept_strategy()` |
| Strategy conn (each) | `EVENT_READ` | `Deserializer.feed_bytes()` → pipeline |
| Exchange 1 conn | `EVENT_READ` | `ExecReportHandler.handle()` |
| Exchange 2 conn | `EVENT_READ` | `ExecReportHandler.handle()` |

**Timeout:** The `select()` call uses a 1-second timeout. On each timeout (or after processing events), the loop runs housekeeping tasks:

- Check exchange heartbeat freshness
- Attempt reconnects for `DOWN` exchanges whose backoff timer has elapsed
- Clean up rate-limit sliding windows
- Detect stale strategy connections

---

## 12. What the IOG Must Not Do

| Anti-pattern | Why |
|---|---|
| Queue orders for later retry | Strategy must know immediately if an order was not sent. Buffering creates stale-order risk. |
| Modify order fields | The IOG translates format, it does not change price, quantity, or side. That is the strategy's responsibility. |
| Make trading decisions | No smart order routing, no order splitting, no held-order logic. Keep it simple and fast. |
| Block on one socket | One stuck exchange connection must not prevent orders to the other exchange. The `selectors` loop prevents this. |
| Silently drop messages | Every received order must result in either a send to an exchange or a reject back to the strategy. No silent failures. |
