package io.github.damian1000.tradingsystem.consume

import io.github.damian1000.tradingsystem.capture.FillHandler
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets
import java.time.Duration

class RetryingHandlerTest {
    private val goodJson =
        """{"v":1,"symbol":"SIM","price":"101.00000000","size":5,""" +
            """"makerOrderId":12,"takerOrderId":34,"aggressor":"BID","ts":1000}"""

    private fun record(value: String) = ConsumerRecord("orderbook.fills", 0, 7L, "SIM", value)

    private class CountingHandler(
        private val failures: Int,
    ) : FillHandler {
        val fills = mutableListOf<Fill>()
        var calls = 0

        override fun onFill(fill: Fill) {
            calls++
            if (calls <= failures) throw IllegalStateException("transient failure $calls")
            fills.add(fill)
        }
    }

    private fun publisher(producer: MockProducer<String, String>) = DeadLetterPublisher(producer, "orderbook.fills.DLT")

    private fun mockProducer() = MockProducer(true, null, StringSerializer(), StringSerializer())

    @Test
    fun `a healthy record is handled once, no retries, no dead letters`() {
        val producer = mockProducer()
        val handler = CountingHandler(failures = 0)
        val sleeps = mutableListOf<Duration>()

        RetryingHandler(handler, publisher(producer), attempts = 3, sleep = sleeps::add).handle(record(goodJson))

        assertEquals(1, handler.calls)
        assertEquals("SIM", handler.fills.single().symbol)
        assertTrue(sleeps.isEmpty())
        assertTrue(producer.history().isEmpty())
    }

    @Test
    fun `a transient failure is retried with backoff and recovers`() {
        val producer = mockProducer()
        val handler = CountingHandler(failures = 2)
        val sleeps = mutableListOf<Duration>()
        val backoff = Duration.ofMillis(250)

        RetryingHandler(handler, publisher(producer), attempts = 3, backoff = backoff, sleep = sleeps::add).handle(record(goodJson))

        assertEquals(3, handler.calls)
        assertEquals(1, handler.fills.size, "the third attempt should have succeeded")
        assertEquals(listOf(backoff, backoff), sleeps)
        assertTrue(producer.history().isEmpty(), "a recovered record must not be dead-lettered")
    }

    @Test
    fun `exhausted retries dead-letter the record with the final error`() {
        val producer = mockProducer()
        val handler = CountingHandler(failures = 99)

        RetryingHandler(handler, publisher(producer), attempts = 3, sleep = {}).handle(record(goodJson))

        assertEquals(3, handler.calls, "no attempts beyond the configured bound")
        val sent = producer.history().single()
        assertEquals("orderbook.fills.DLT", sent.topic())
        assertEquals(goodJson, sent.value())
        assertEquals(
            "transient failure 3",
            String(sent.headers().lastHeader("dlt.error.message").value(), StandardCharsets.UTF_8),
        )
    }

    @Test
    fun `a malformed record is dead-lettered immediately, never retried`() {
        val producer = mockProducer()
        val handler = CountingHandler(failures = 0)
        val sleeps = mutableListOf<Duration>()

        RetryingHandler(handler, publisher(producer), attempts = 3, sleep = sleeps::add).handle(record("not json"))

        assertEquals(0, handler.calls, "malformed input never reaches the handler")
        assertTrue(sleeps.isEmpty(), "malformed input never heals, so retrying it is pure stall")
        assertEquals("not json", producer.history().single().value())
    }

    @Test
    fun `attempts below one are rejected`() {
        assertThrows(IllegalArgumentException::class.java) {
            RetryingHandler(CountingHandler(0), publisher(mockProducer()), attempts = 0)
        }
    }
}
