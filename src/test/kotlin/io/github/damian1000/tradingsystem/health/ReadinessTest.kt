package io.github.damian1000.tradingsystem.health

import io.github.damian1000.tradingsystem.consume.ConsumerHealth
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset

class ReadinessTest {
    private class SteppingClock(
        private var now: Instant = Instant.parse("2026-07-19T10:00:00Z"),
    ) : Clock() {
        override fun instant(): Instant = now

        override fun getZone(): ZoneOffset = ZoneOffset.UTC

        override fun withZone(zone: java.time.ZoneId): Clock = this

        fun advance(duration: Duration) {
            now = now.plus(duration)
        }
    }

    private val clock = SteppingClock()
    private val consumer = ConsumerHealth("fills", clock)

    private fun readiness(databaseOk: Boolean = true) =
        Readiness(
            consumers = listOf(consumer),
            databaseOk = { databaseOk },
            deadLettersPublished = { 3 },
            deadLettersFailed = { 1 },
        )

    private fun healthyConsumer() {
        consumer.started()
        consumer.assigned(1)
        consumer.polled()
    }

    @Test
    fun `ready only when every consumer is alive, assigned, and polling, and the database answers`() {
        healthyConsumer()
        val probe = readiness().probe()
        assertTrue(probe.ready)
        assertTrue(probe.json.contains(""""fills":{"ok":true"""), probe.json)
        assertTrue(probe.json.contains(""""deadLetters":{"published":3,"failed":1}"""), probe.json)
    }

    @Test
    fun `a consumer that has never polled is not ready`() {
        consumer.started()
        consumer.assigned(1)
        val probe = readiness().probe()
        assertFalse(probe.ready)
        assertTrue(probe.json.contains(""""pollAgeMillis":null"""), probe.json)
    }

    @Test
    fun `a consumer that stopped polling goes not-ready once the poll age passes the ceiling`() {
        healthyConsumer()
        clock.advance(Duration.ofSeconds(29))
        assertTrue(readiness().probe().ready, "29s is within the 30s ceiling")

        clock.advance(Duration.ofSeconds(2))
        val probe = readiness().probe()
        assertFalse(probe.ready, "a silent consumer is a broken consumer, whatever the thread state says")
        assertEquals(31_000L, consumer.pollAgeMillis())
    }

    @Test
    fun `a dead consumer thread reports its fatal error`() {
        healthyConsumer()
        consumer.failed(IllegalStateException("poll exploded"))
        val probe = readiness().probe()
        assertFalse(probe.ready)
        assertTrue(probe.json.contains("poll exploded"), probe.json)
        assertTrue(probe.json.contains(""""threadAlive":false"""), probe.json)
    }

    @Test
    fun `a dead database fails readiness even with healthy consumers`() {
        healthyConsumer()
        val probe = readiness(databaseOk = false).probe()
        assertFalse(probe.ready)
        assertTrue(probe.json.contains(""""database":{"ok":false}"""), probe.json)
    }

    @Test
    fun `a cleanly stopped consumer is not ready`() {
        healthyConsumer()
        consumer.stopped()
        assertFalse(readiness().probe().ready)
    }
}
