# trading-system

[![CI](https://github.com/damian1000/trading-system/actions/workflows/ci.yml/badge.svg)](https://github.com/damian1000/trading-system/actions/workflows/ci.yml)
[![CodeQL](https://github.com/damian1000/trading-system/actions/workflows/codeql.yml/badge.svg)](https://github.com/damian1000/trading-system/actions/workflows/codeql.yml)

Post-trade integration over the [orderbook](https://github.com/damian1000/orderbook) fill stream:
consumes fills off Kafka, maintains net positions in an Oracle Autonomous Database, reprices the
book through the [risk-engine](https://github.com/damian1000/risk-engine) library, and serves a
live positions/risk/PnL dashboard.

Matching and pricing are versioned library dependencies, not code in this repo:

```groovy
implementation 'com.github.damian1000:orderbook:v1.0.0'
implementation 'com.github.damian1000:risk-engine:v1.0.0'
```

## Build

```bash
./gradlew clean check
```

JDK 25 via Gradle toolchain. Tests include real-broker (Kafka) and real-database (Oracle Free)
integration suites via Testcontainers, so a local Docker daemon is required for the full `check`.
