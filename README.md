# trading-system

[![CI](https://github.com/damian1000/trading-system/actions/workflows/ci.yml/badge.svg)](https://github.com/damian1000/trading-system/actions/workflows/ci.yml)
[![CodeQL](https://github.com/damian1000/trading-system/actions/workflows/codeql.yml/badge.svg)](https://github.com/damian1000/trading-system/actions/workflows/codeql.yml)
[![codecov](https://codecov.io/gh/damian1000/trading-system/graph/badge.svg)](https://codecov.io/gh/damian1000/trading-system)

Post-trade integration over the [orderbook](https://github.com/damian1000/orderbook) fill stream:
consumes fills off Kafka, maintains net positions in an Oracle Autonomous Database, reprices the
book through the [risk-engine](https://github.com/damian1000/risk-engine) library, and serves a
live positions/risk/PnL dashboard.

**▶ Live: https://trading.damianhoward.com** — submit an order that crosses on
[the live order book](https://orderbook.damianhoward.com) and watch the position, valuation,
VaR, and day PnL update here as the fill arrives.

```
orderbook.fills (Kafka) ──┬─► FillConsumer (trading-system.positions) ──► TradeCapture ──► PositionBook ──► PositionStore (Oracle, Flyway)
                          │        │                                           │
                          │        ▼ on poison / exhausted retries             ▼ every fill
                          │  orderbook.fills.DLT                      RiskGateway (risk-engine)
                          │                                                    │
                          │                                     DashboardServer ──► SSE ──► browser
                          │                                                    ▲
                          └─► FillConsumer (trading-system.limits) ──► LimitsChecker
```

Matching and pricing are versioned library dependencies, not code in this repo:

```groovy
implementation 'com.github.damian1000:orderbook:v1.0.0'
implementation 'com.github.damian1000:risk-engine:v1.0.0'
```

## Fill consumption and failure handling

`FillConsumer` runs a plain `kafka-clients` poll loop on its own thread and commits offsets only
after a batch is fully processed, so delivery is at-least-once. Each record passes through
`RetryingHandler`, which is the retry→DLT mechanism written out rather than annotation-driven:

- A record that fails to parse goes straight to `orderbook.fills.DLT` — malformed input never
  heals, so retrying it only stalls the stream.
- A record whose handling fails (the database blinked) is retried 3 times, 500 ms apart, then
  dead-lettered on exhaustion.
- Dead-lettered records carry the untouched original payload plus `dlt.error.*` and
  `dlt.source.*` headers (exception, source topic/partition/offset) for inspection and replay.
- Either way the consumer thread survives; a poison record never stops the stream.

The fill schema is orderbook's versioned egress JSON (`v`, `symbol`, `price`, `size`,
`makerOrderId`, `takerOrderId`, `aggressor`, `ts`), parsed strictly — an unknown schema version
or a wrongly-typed field is poison, not a guess.

## Positions

`PositionBook` books the taker side of each fill — a BID aggressor bought, an OFFER aggressor
sold — into a net signed quantity per symbol. Every update is persisted through
`JdbcPositionStore` (plain JDBC, Oracle `MERGE`), so replaying a batch after a crash rewrites the
same rows rather than duplicating them, and the book warms from the store at startup. Schema is
managed by Flyway.

## Limits

A second `FillConsumer` runs in its own consumer group (`trading-system.limits`) over the same
`orderbook.fills` topic, so each fill is delivered to both the positions pipeline and
`LimitsChecker` independently. The checker derives its own net position per symbol from the
stream — it never reads the position book — and checks two ceilings on every fill: absolute net
position and notional (|net quantity| × last price). Crossing a ceiling in either direction
records a breach or clear event, stamped with the fill's own timestamp; the dashboard shows
current utilisation per symbol and the recent event history.

Malformed records on this path are counted and skipped rather than dead-lettered — the positions
consumer owns the DLT, and a second publisher would duplicate every poison record. Limits state
is in-memory: the group reads from `earliest`, so a restart rebuilds the same exposures and
events from the retained stream. Because both groups push a snapshot per fill, the dashboard's
update counter ticks roughly twice per fill; every frame is a complete snapshot.

## Risk and PnL

On every fill, `RiskGateway` rebuilds a risk-engine `Portfolio` from the net position, marks it at
the last traded price, and asks `RiskReportAssembler` for the report: mark-to-market valuation,
bump-and-reprice Greeks, parametric and historical-simulation VaR/ES, and — once a session-open
mark exists (the first fill seen) — the day's PnL attribution with its explicit residual. Every
number comes from risk-engine's validated calculators; this repo adds no pricing maths.

Slice 1 trades a single instrument, so `TradeCapture` reports risk for the first (only) position;
`PositionBook` and `LimitsChecker` already track state per symbol, and the risk report generalises
to the full book in a later slice.

## Dashboard

A dependency-free JDK `HttpServer`: `GET /api/state` returns the current snapshot as JSON, `GET
/api/stream` is an SSE feed pushing a fresh snapshot on every fill, and `/healthz` gates deploys.
The front end is a thin renderer of the snapshot JSON, in the same terminal-style UI as the
orderbook and risk-engine live sites.

## Configuration

| Variable                             | Default                          | Purpose                            |
| ------------------------------------ | -------------------------------- | ---------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS`            | `127.0.0.1:9092`                 | Broker to consume from             |
| `FILLS_TOPIC` / `FILLS_DLT_TOPIC`    | `orderbook.fills` / `…fills.DLT` | Source and dead-letter topics      |
| `KAFKA_GROUP_ID`                     | `trading-system.positions`       | Positions consumer group           |
| `LIMITS_GROUP_ID`                    | `trading-system.limits`          | Limits consumer group              |
| `LIMIT_MAX_POSITION`                 | `50`                             | Absolute net position ceiling      |
| `LIMIT_MAX_NOTIONAL`                 | `5000`                           | Notional exposure ceiling          |
| `DB_URL` / `DB_USER` / `DB_PASSWORD` | required                         | Oracle connection (wallet TNS URL) |
| `PORT`                               | `8082`                           | Dashboard HTTP port                |

## Build

```bash
./gradlew clean check
```

JDK 25 via Gradle toolchain. The suite includes real-broker tests (fills in, positions out,
poison to the DLT; two consumer groups fanning out over one topic) and a real-database store
test (Oracle Free), all via Testcontainers — a local Docker daemon is required for the full
`check`. Coverage is gated at 90% instruction with only the process entry point excluded.
