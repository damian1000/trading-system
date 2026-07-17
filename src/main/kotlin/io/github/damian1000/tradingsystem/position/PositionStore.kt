package io.github.damian1000.tradingsystem.position

import java.sql.Connection

/** Durable home of the position book; the database outlives the process, the book warms from it. */
interface PositionStore {
    fun save(position: Position)

    fun loadAll(): List<Position>
}

/**
 * Plain JDBC over the `positions` table (Flyway `V1`), one connection per operation — fills
 * arrive at the live site's human rate, not a hot path, and a connection that is opened, used
 * and closed cannot go stale between fills. Oracle `MERGE` makes the save an upsert keyed by
 * symbol — one row per symbol, updated in place with the latest aggregate.
 */
class JdbcPositionStore(
    private val connect: () -> Connection,
) : PositionStore {
    override fun save(position: Position) {
        connect().use { connection ->
            connection.prepareStatement(MERGE).use { statement ->
                statement.setString(1, position.symbol)
                statement.setLong(2, position.quantity)
                statement.setBigDecimal(3, position.lastPrice)
                statement.setLong(4, position.lastTimeMillis)
                statement.setLong(5, position.quantity)
                statement.setBigDecimal(6, position.lastPrice)
                statement.setLong(7, position.lastTimeMillis)
                statement.executeUpdate()
            }
        }
    }

    override fun loadAll(): List<Position> =
        connect().use { connection ->
            connection.prepareStatement(SELECT_ALL).use { statement ->
                statement.executeQuery().use { rows ->
                    buildList {
                        while (rows.next()) {
                            add(
                                Position(
                                    symbol = rows.getString("symbol"),
                                    quantity = rows.getLong("quantity"),
                                    lastPrice = rows.getBigDecimal("last_price"),
                                    lastTimeMillis = rows.getLong("last_time_millis"),
                                ),
                            )
                        }
                    }
                }
            }
        }

    companion object {
        private const val MERGE =
            "MERGE INTO positions p USING (SELECT ? AS symbol FROM dual) src ON (p.symbol = src.symbol) " +
                "WHEN MATCHED THEN UPDATE SET quantity = ?, last_price = ?, last_time_millis = ? " +
                "WHEN NOT MATCHED THEN INSERT (symbol, quantity, last_price, last_time_millis) " +
                "VALUES (src.symbol, ?, ?, ?)"
        private const val SELECT_ALL =
            "SELECT symbol, quantity, last_price, last_time_millis FROM positions ORDER BY symbol"
    }
}
