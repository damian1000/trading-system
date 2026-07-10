package io.github.damian1000.tradingsystem.position

import org.flywaydb.core.Flyway
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.oracle.OracleContainer
import java.math.BigDecimal
import java.sql.DriverManager

/**
 * The store against a real Oracle, schema applied by the same Flyway migration production runs —
 * an in-memory stand-in can't fail on Oracle's types, MERGE semantics, or the migration itself.
 * Skips only where Docker is absent; always runs in CI.
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

    @Test
    fun `an empty table loads an empty book`() {
        assertTrue(store.loadAll().isEmpty())
    }

    @Test
    fun `saves and reloads positions, preserving the 8-decimal price exactly`() {
        val sim = Position("SIM", -42, BigDecimal("101.00000000"), 1720620000000)
        val other = Position("ABC", 7, BigDecimal("0.00000001"), 1)
        store.save(sim)
        store.save(other)

        val loaded = store.loadAll()
        assertEquals(listOf("ABC", "SIM"), loaded.map { it.symbol }, "loadAll orders by symbol")
        assertEquals(other, loaded[0].copy(lastPrice = loaded[0].lastPrice.setScale(8)))
        assertEquals(sim, loaded[1].copy(lastPrice = loaded[1].lastPrice.setScale(8)))
    }

    @Test
    fun `saving the same symbol again updates the row — replaying a batch cannot duplicate it`() {
        store.save(Position("SIM", 5, BigDecimal("100.00"), 1000))
        store.save(Position("SIM", 8, BigDecimal("102.00"), 2000))

        val loaded = store.loadAll().single()
        assertEquals(8, loaded.quantity)
        assertEquals(0, BigDecimal("102.00").compareTo(loaded.lastPrice))
        assertEquals(2000, loaded.lastTimeMillis)
    }
}
