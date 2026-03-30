# Trading System

A simulated trading system demonstrating real-world network architecture patterns used in production trading infrastructure. Built for educational purposes — the design mirrors how actual exchanges, market data handlers, and order gateways are structured.

## Architecture

```
[Exchange 1] ──UDP feed──► ┐
                           ├──► [Market Data Handler / Normalizer]
[Exchange 2] ──UDP feed──► ┘         │              │
                                     │ UDP multicast │ UDP multicast
                                     ▼              ▼
                               [Group 1]       [Group 2]
                                     │              │
                            ┌────────┴──────────────┘
                            │
                  ┌─────────┴──────────┐
                  ▼                    ▼
          [Slow Strategy]      [Fast Strategy]
         (gap detection,       (no gap handling,
          complex logic)        reactive logic)
                  │                    │
                  └─────────┬──────────┘
                            │ TCP (FIX)
                            ▼
              [Internal Order Gateway (IOG)]
               (validates, converts, routes)
                  │                    │
            TCP (FIX)            TCP (FIX)
                  │                    │
                  ▼                    ▼
           [Exchange 1]          [Exchange 2]
```

## Components

| File | Role |
|------|------|
| `exchange1.py` | Simulated exchange 1 — publishes UDP feed, receives FIX orders |
| `exchange2.py` | Simulated exchange 2 — publishes UDP feed, receives FIX orders |
| `market_data_handler.py` | Reads both UDP feeds, normalizes data, publishes to multicast groups |
| `strategy_slow.py` | Subscribes to multicast, handles dropped packets, sends orders to IOG |
| `strategy_fast.py` | Subscribes to multicast, no gap handling, sends orders to IOG |
| `internal_order_gateway.py` | Receives orders from strategies, validates, routes to correct exchange |
| `config.py` | All hostnames, ports, and multicast group addresses |

## Key Networking Concepts

- **UDP** for market data — low latency, no backpressure on the publisher. Packet loss is the subscriber's problem.
- **Multicast** for normalized feed distribution — one publisher, many subscribers. Adding a new strategy requires zero changes to the handler.
- **TCP (FIX)** for order entry — guaranteed delivery. FIX is an application layer protocol implemented on top of TCP.
- **Gap detection** — the slow strategy tracks sequence numbers and detects missing UDP packets. So does the market data handler. The fast strategy skips this to minimize processing time.

## Configuration

All addresses and ports are defined in `config.py`. Update multicast group partitioning there before running.

## Running

Start each component in a separate terminal in this order:

```bash
# 1. Start exchanges first (they need to be listening before IOG connects)
python exchange1.py
python exchange2.py

# 2. Start the IOG (connects to exchanges, listens for strategies)
python internal_order_gateway.py

# 3. Start the market data handler
python market_data_handler.py

# 4. Start strategies (connect to IOG, subscribe to multicast)
python strategy_slow.py
python strategy_fast.py
```

## Dependencies

Currently uses Python standard library only. If requirements arise, add to `requirements.txt`.

```bash
python --version  # Python 3.8+ recommended
```
