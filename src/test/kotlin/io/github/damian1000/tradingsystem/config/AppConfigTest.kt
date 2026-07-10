package io.github.damian1000.tradingsystem.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class AppConfigTest {
    private val db = mapOf("DB_URL" to "jdbc:oracle:thin:@db", "DB_USER" to "app", "DB_PASSWORD" to "secret")

    @Test
    fun `defaults match the box-2 runbook`() {
        val config = AppConfig.fromEnv(db)
        assertEquals("127.0.0.1:9092", config.kafkaBootstrapServers)
        assertEquals("orderbook.fills", config.fillsTopic)
        assertEquals("orderbook.fills.DLT", config.deadLetterTopic)
        assertEquals("trading-system.positions", config.groupId)
        assertEquals(8082, config.port)
    }

    @Test
    fun `every setting is overridable from the environment`() {
        val config =
            AppConfig.fromEnv(
                db +
                    mapOf(
                        "KAFKA_BOOTSTRAP_SERVERS" to "10.0.0.91:9094",
                        "FILLS_TOPIC" to "fills",
                        "FILLS_DLT_TOPIC" to "fills.dead",
                        "KAFKA_GROUP_ID" to "g1",
                        "PORT" to "9000",
                    ),
            )
        assertEquals("10.0.0.91:9094", config.kafkaBootstrapServers)
        assertEquals("fills", config.fillsTopic)
        assertEquals("fills.dead", config.deadLetterTopic)
        assertEquals("g1", config.groupId)
        assertEquals(9000, config.port)
        assertEquals("jdbc:oracle:thin:@db", config.dbUrl)
    }

    @Test
    fun `the database triple is required — a misconfigured process fails at startup`() {
        listOf("DB_URL", "DB_USER", "DB_PASSWORD").forEach { missing ->
            val e = assertThrows(IllegalArgumentException::class.java) { AppConfig.fromEnv(db - missing) }
            assertEquals("$missing must be set", e.message)
        }
        assertThrows(IllegalArgumentException::class.java) { AppConfig.fromEnv(db + ("DB_URL" to " ")) }
    }

    @Test
    fun `a malformed port is rejected`() {
        assertThrows(IllegalArgumentException::class.java) { AppConfig.fromEnv(db + ("PORT" to "web")) }
        assertThrows(IllegalArgumentException::class.java) { AppConfig.fromEnv(db + ("PORT" to "70000")) }
    }
}
