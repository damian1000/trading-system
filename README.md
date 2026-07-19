# trading-system

[![CI](https://github.com/damian1000/trading-system/actions/workflows/ci.yml/badge.svg)](https://github.com/damian1000/trading-system/actions/workflows/ci.yml)
[![CodeQL](https://github.com/damian1000/trading-system/actions/workflows/codeql.yml/badge.svg)](https://github.com/damian1000/trading-system/actions/workflows/codeql.yml)
[![codecov](https://codecov.io/gh/damian1000/trading-system/graph/badge.svg)](https://codecov.io/gh/damian1000/trading-system)

Post-trade integration over the [orderbook](https://github.com/damian1000/orderbook) fill stream:
consumes fills off Kafka into a durable fill ledger in an Oracle Autonomous Database, derives net
positions in the same transaction, reprices the book through the
[risk-engine](https://github.com/damian1000/risk-engine) library, and serves a live
positions/risk/PnL dashboard.

**▶ Live: https://trading.damianhoward.com** — submit an order that crosses on
[the live order book](https://orderbook.damianhoward.com) and watch the position, valuation,
VaR, and day PnL update here as the fill arrives.

```
orderbook.fills (Kafka) ──┬─► FillConsumer (trading-system.positions) ──► TradeCapture ──► fill ledger + positions (Oracle, one txn)
                          │        │                                           │
                          │        ▼ on poison / exhausted retries             ▼ every applied fill
                          │  orderbook.fills.DLT (confirmed)          RiskGateway (risk-engine)
                          │                                                    │
                          │                                     DashboardServer ──► SSE ──► browser
                          │                                                    ▲
                          └─► FillConsumer (seek from ledger) ──► LimitsChecker
```

Matching and pricing are versioned library dependencies, not code in this repo:

```groovy
implementation 'com.github.damian1000:orderbook:v1.0.0'
implementation 'com.github.damian1000:risk-engine:v1.0.0'
```

## The book of record: idempotent fill application

Delivery off Kafka is at-least-once, so the same fill can arrive more than once — a consumer
retry, a redelivery after a crash, a replay after a restart. Applying it more than once would
corrupt the positions, so application is idempotent at the database:

- Every fill is inserted into a `fills` ledger keyed by its stream coordinates
  `(source_topic, source_partition, source_offset)`, and the `positions` row moves by the fill's
  signed size **in the same transaction**. A crash can never persist one without the other.
- A replayed record hits the ledger's primary key (ORA-00001), is reported as a duplicate, and
  changes nothing. The dashboard counts these under "Replays dropped".
- In-memory state updates only from the committed row, after the transaction — a retried
  handler cannot move a position twice, because the second attempt is a duplicate by then.

`JdbcPositionStoreTest` proves the sequences against a real Oracle (Testcontainers): replayed
coordinates are dropped, and a simulated failure between the ledger insert and the position
merge rolls the whole transaction back, leaving the coordinates free for the retry.

## Fill consumption and failure handling

`FillConsumer` runs a plain `kafka-clients` poll loop on its own thread and commits offsets only
after a batch is fully processed. Each record passes through `RetryingHandler`, the retry→DLT
mechanism written out rather than annotation-driven:

- A record that fails to parse goes straight to `orderbook.fills.DLT` — malformed input never
  heals, so retrying it only stalls the stream.
- A record whose handling fails (the database blinked) is retried 3 times, 500 ms apart, then
  dead-lettered on exhaustion. Retrying the whole handler is safe because application is
  idempotent.
- Dead-letter publication is **confirmed**: `publish` blocks until the broker acknowledges the
  send. An unacknowledged send throws instead of being counted and forgotten — the process exits
  rather than commit past a record that is in neither stream, and systemd restarts it into a
  replay the ledger makes safe. A record is either applied, on the DLT, or ahead of the
  committed offset; never in none of those places.
- Dead-lettered records carry the untouched original payload plus `dlt.error.*` and
  `dlt.source.*` headers (exception, source topic/partition/offset) for inspection and replay.

The fill schema is orderbook's versioned egress JSON (`v`, `symbol`, `price`, `size`,
`makerOrderId`, `takerOrderId`, `aggressor`, `ts`), parsed strictly — an unknown schema version
or a wrongly-typed field is poison, not a guess.

## Positions

`PositionBook` holds the net signed quantity per symbol — a BID aggressor bought, an OFFER
aggressor sold — as an in-memory mirror of the store's committed truth: rows enter it only from
committed transactions, so it can never run ahead of the database. At startup it warms from the
`positions` table, which the ledger keeps consistent by construction. Schema is managed by
Flyway.

## Limits

A second `FillConsumer` feeds `LimitsChecker`, an independent exposure view over the same fill
stream — it never reads the position book — checking two ceilings on every fill: absolute net
position and notional (|net quantity| × last price). Crossing a ceiling in either direction
records a breach or clear event, stamped with the fill's own timestamp; the dashboard shows
current utilisation per symbol and the recent event history.

Limits state is restart-safe through the fill ledger, not Kafka group offsets: at startup the
checker replays the persisted fills (rebuilding exposures **and** the breach history — events
carry each fill's own time), and the consumer then attaches to the live stream by assigning
partitions and seeking just past the ledger's high-water mark. Both views therefore resume from
the same durable truth after a restart, and the dashboard's `sync` block says whether they have
read to the same stream position since.

Malformed records on this path are counted and skipped rather than dead-lettered — the positions
consumer owns the DLT, and a second publisher would duplicate every poison record.

## Risk and PnL

On every applied fill, `RiskGateway` rebuilds a risk-engine `Portfolio` from the net position,
marks it at the last traded price, and asks `RiskReportAssembler` for the report: mark-to-market
valuation, bump-and-reprice Greeks, parametric and historical-simulation VaR/ES, and — once a
session-open mark exists (the first fill applied by this process) — the day's PnL attribution
with its explicit residual. Every number comes from risk-engine's validated calculators; this
repo adds no pricing maths.

Slice 1 trades a single instrument, so `TradeCapture` reports risk for the first (only) position;
`PositionBook` and `LimitsChecker` already track state per symbol, and the risk report generalises
to the full book in a later slice.

## Dashboard and operational truth

A dependency-free JDK `HttpServer`: `GET /api/state` returns the current snapshot as JSON and
`GET /api/stream` is an SSE feed pushing a fresh snapshot on every fill. The front end is a thin
renderer of the snapshot JSON, in the same terminal-style UI as the orderbook and risk-engine
live sites.

Two endpoints report health at different depths:

- `/healthz` — liveness: the web process answers.
- `/readyz` — readiness: every consumer thread alive, assigned, and recently polling; the
  database answering; dead-letter publish/failure counters. Returns 503 with the failing
  component named when the pipeline is broken, whatever the web process says. Deploys gate on
  this, so a deploy whose consumers cannot attach fails instead of going green.

The snapshot itself carries a `sync` block — each consumer path's last stream offset and fill
timestamp, whether the two views are coherent, and how many replays the ledger dropped. The
status bar renders it, along with the age of the current mark, so a quiet stream shows as an
ageing mark rather than passing for fresh.

A consumer that dies on an unexpected exception is never a silent zombie: readiness reports the
fatal error, and the process exits so systemd restarts it into a safe replay.

## Configuration

| Variable                             | Default                          | Purpose                            |
| ------------------------------------ | -------------------------------- | ---------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS`            | `127.0.0.1:9092`                 | Broker to consume from             |
| `FILLS_TOPIC` / `FILLS_DLT_TOPIC`    | `orderbook.fills` / `…fills.DLT` | Source and dead-letter topics      |
| `KAFKA_GROUP_ID`                     | `trading-system.positions`       | Positions consumer group           |
| `LIMIT_MAX_POSITION`                 | `50`                             | Absolute net position ceiling      |
| `LIMIT_MAX_NOTIONAL`                 | `5000`                           | Notional exposure ceiling          |
| `DB_URL` / `DB_USER` / `DB_PASSWORD` | required                         | Oracle connection (wallet TNS URL) |
| `PORT`                               | `8082`                           | Dashboard HTTP port                |

The limits consumer takes no group id — it derives its start position from the fill ledger.

## Build

```bash
./gradlew clean check
```

JDK 25 via Gradle toolchain. The suite includes real-broker tests (fills in, positions out,
poison to the DLT; the seek-mode limits consumer fanning out over one topic) and real-database
store tests (Oracle Free) covering replay, retry, and mid-transaction failure — all via
Testcontainers; a local Docker daemon is required for the full `check`. Coverage is gated at 90%
instruction with only the process entry point excluded.
