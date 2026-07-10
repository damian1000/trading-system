# trading-system

[![CI](https://github.com/damian1000/trading-system/actions/workflows/ci.yml/badge.svg)](https://github.com/damian1000/trading-system/actions/workflows/ci.yml)
[![CodeQL](https://github.com/damian1000/trading-system/actions/workflows/codeql.yml/badge.svg)](https://github.com/damian1000/trading-system/actions/workflows/codeql.yml)

Post-trade integration over the [orderbook](https://github.com/damian1000/orderbook) fill stream:
consumes fills off Kafka, maintains net positions in an Oracle Autonomous Database, reprices the
book through the [risk-engine](https://github.com/damian1000/risk-engine) library, and serves a
live positions/risk/PnL dashboard.

```
orderbook.fills (Kafka) ──► FillConsumer ──► TradeCapture ──► PositionBook ──► PositionStore (Oracle, Flyway)
                                 │                                  │
                                 ▼ on poison / exhausted retries    ▼ every fill
                          orderbook.fills.DLT              RiskGateway (risk-engine)
                                                                    │
                                                     DashboardServer ──► SSE ──► browser
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

## Risk and PnL

On every fill, `RiskGateway` rebuilds a risk-engine `Portfolio` from the net position, marks it at
the last traded price, and asks `RiskReportAssembler` for the report: mark-to-market valuation,
bump-and-reprice Greeks, parametric and historical-simulation VaR/ES, and — once a session-open
mark exists (the first fill seen) — the day's PnL attribution with its explicit residual. Every
number comes from risk-engine's validated calculators; this repo adds no pricing maths.

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
| `KAFKA_GROUP_ID`                     | `trading-system.positions`       | Consumer group                     |
| `DB_URL` / `DB_USER` / `DB_PASSWORD` | required                         | Oracle connection (wallet TNS URL) |
| `PORT`                               | `8082`                           | Dashboard HTTP port                |

## Build

```bash
./gradlew clean check
```

JDK 25 via Gradle toolchain. The suite includes a real-broker pipeline test (fills in, positions
out, poison to the DLT) and a real-database store test (Oracle Free), both via Testcontainers —
a local Docker daemon is required for the full `check`. Coverage is gated at 90% instruction with
only the process entry point excluded.
