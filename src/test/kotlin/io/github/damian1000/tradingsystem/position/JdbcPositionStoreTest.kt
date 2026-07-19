package io.github.damian1000.tradingsystem.position

import io.github.damian1000.orderbook.model.Side
import io.github.damian1000.tradingsystem.consume.Fill
import io.github.damian1000.tradingsystem.consume.FillSource
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.oracle.OracleContainer
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException

/**
 * The store against a real Oracle, schema applied by the same Flyway migrations production runs —
 * an in-memory stand-in can't fail on Oracle's types, MERGE semantics, ORA-00001, or the
 * migrations themselves. Skips only where Docker is absent; always runs in CI.
 */
@Testcontainers(disabledWithoutDocker = true)
class JdbcPositionStoreTest {
    companion object {
        @Container
        @JvmField
        val oracle: OracleContainer = OracleContainer("gvenzl/oracle-free:23-slim-faststart")
    }

    private val store = JdbcPositionStore { DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password) }

    @BeforeEach
    fun cleanSchema() {
        Flyway
            .configure()
            .dataSource(oracle.jdbcUrl, oracle.username, oracle.password)
            .cleanDisabled(false)
            .load()
            .apply { clean() }
            .migrate()
    }

    private fun fill(
        symbol: String = "SIM",
        side: Side = Side.BID,
        size: Long = 5,
        price: String = "101.00000000",
        ts: Long = 1000,
    ) = Fill(symbol, BigDecimal(price), size, 11, 22, side, ts)

    private fun source(offset: Long) = FillSource("orderbook.fills", 0, offset)

    @Test
    fun `an empty table loads an empty book`() {
        assertTrue(store.loadAll().isEmpty())
        assertTrue(store.loadLedger("orderbook.fills").fills.isEmpty())
    }

    @Test
    fun `fills accumulate a net position through the ledger`() {
        store.record(fill(size = 5), source(1))
        val outcome = store.record(fill(side = Side.OFFER, size = 8, price = "102.00000000", ts = 2000), source(2))

        val position = (outcome as RecordOutcome.Applied).position
        assertEquals(-3, position.quantity)
        assertEquals(0, BigDecimal("102.00000000").compareTo(position.lastPrice))
        assertEquals(2000, position.lastTimeMillis)
        assertEquals(position.quantity, store.loadAll().single().quantity, "the returned row is the committed row")
    }

    @Test
    fun `replayed source coordinates are dropped — retry, redelivery, and restart cannot double-apply`() {
        store.record(fill(size = 5), source(1))
        val replay = store.record(fill(size = 5), source(1))

        assertEquals(RecordOutcome.Duplicate, replay)
        assertEquals(5, store.loadAll().single().quantity, "the position is unchanged by the replay")
        assertEquals(1, store.loadLedger("orderbook.fills").fills.size, "the ledger keeps one row per record")
    }

    @Test
    fun `a failure after the ledger insert rolls the whole transaction back`() {
        val failing =
            JdbcPositionStore {
                FailOnMerge(DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password))
            }

        assertThrows(SQLException::class.java) { failing.record(fill(size = 5), source(1)) }

        assertTrue(store.loadLedger("orderbook.fills").fills.isEmpty(), "the ledger row must not survive alone")
        assertTrue(store.loadAll().isEmpty())
        // The coordinates are still free: the consumer's retry applies cleanly.
        assertTrue(store.record(fill(size = 5), source(1)) is RecordOutcome.Applied)
        assertEquals(5, store.loadAll().single().quantity)
    }

    @Test
    fun `the ledger replays in stream order with high-water marks and faithful fills`() {
        store.record(fill(size = 5, ts = 1000), source(1))
        store.record(fill(side = Side.OFFER, size = 2, ts = 2000), source(2))
        store.record(fill(symbol = "ABC", size = 1, ts = 3000), source(3))

        val ledger = store.loadLedger("orderbook.fills")
        assertEquals(3, ledger.fills.size)
        assertEquals(mapOf(0 to 3L), ledger.highWaterOffsets)
        val second = ledger.fills[1]
        assertEquals(Side.OFFER, second.aggressor)
        assertEquals(-2, second.signedSize)
        assertEquals(11, second.makerOrderId)
        assertEquals(22, second.takerOrderId)
        assertTrue(ledger.fills.map { it.timeMillis } == listOf(1000L, 2000L, 3000L), "stream order, not insertion luck")
    }

    @Test
    fun `saves and reloads positions, preserving the 8-decimal price exactly`() {
        store.record(fill(symbol = "SIM", side = Side.OFFER, size = 42, price = "101.00000000", ts = 1720620000000), source(1))
        store.record(fill(symbol = "ABC", size = 7, price = "0.00000001", ts = 1), source(2))

        val loaded = store.loadAll()
        assertEquals(listOf("ABC", "SIM"), loaded.map { it.symbol }, "loadAll orders by symbol")
        assertEquals(0, BigDecimal("0.00000001").compareTo(loaded[0].lastPrice))
        assertEquals(-42, loaded[1].quantity)
    }

    @Test
    fun `ping answers true against a live database and false against a dead one`() {
        assertTrue(store.ping())
        val dead = JdbcPositionStore { throw SQLException("no route to database") }
        assertFalse(dead.ping())
    }

    /** A connection that fails exactly between the ledger insert and the position merge. */
    private class FailOnMerge(
        private val real: Connection,
    ) : Connection by real {
        override fun prepareStatement(sql: String): PreparedStatement =
            if (sql.startsWith("MERGE")) throw SQLException("simulated failure mid-transaction") else real.prepareStatement(sql)
    }
}
