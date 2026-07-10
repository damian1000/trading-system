package io.github.damian1000.tradingsystem.consume

import io.github.damian1000.riskengine.report.RiskReportAssembler
import io.github.damian1000.tradingsystem.capture.TradeCapture
import io.github.damian1000.tradingsystem.position.Position
import io.github.damian1000.tradingsystem.position.PositionBook
import io.github.damian1000.tradingsystem.position.PositionStore
import io.github.damian1000.tradingsystem.pricing.MarketAssumptions
import io.github.damian1000.tradingsystem.pricing.RiskGateway
import io.github.damian1000.tradingsystem.web.Broadcaster
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Properties
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * The whole consume pipeline against a real broker: fills produced onto the topic land as
 * positions, and a poison record lands on the DLT with its error headers while the stream keeps
 * moving. A stub cannot fail the way a broker fails (rebalancing, offsets, wakeup semantics), so
 * this runs against the real thing; it skips only where Docker is absent and always runs in CI.
 */
@Testcontainers(disabledWithoutDocker = true)
class FillPipelineIntegrationTest {
    companion object {
        @Container
        @JvmField
        val kafka: KafkaContainer = KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"))
    }

    private class CollectingStore : PositionStore {
        val saved = ConcurrentLinkedQueue<Position>()

        override fun save(position: Position) {
            saved.add(position)
        }

        override fun loadAll(): List<Position> = saved.toList()
    }

    private class SilentBroadcaster : Broadcaster {
        override fun startHeartbeat(periodSeconds: Long) {}

        override fun broadcast(json: String) {}

        override fun stream(
            exchange: com.sun.net.httpserver.HttpExchange,
            initialJson: String,
        ) = throw UnsupportedOperationException()
    }

    private fun fillJson(
        size: Long,
        aggressor: String = "BID",
        ts: Long = 1000,
    ): String =
        """{"v":1,"symbol":"SIM","price":"101.00000000","size":$size,""" +
            """"makerOrderId":1,"takerOrderId":2,"aggressor":"$aggressor","ts":$ts}"""

    @Test
    fun `fills off the topic become positions and a poison record is dead-lettered, not fatal`() {
        val topic = "orderbook.fills"
        val dlt = "orderbook.fills.DLT"
        val store = CollectingStore()
        val book = PositionBook()
        val capture =
            TradeCapture(book, store, RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default()), SilentBroadcaster())
        val deadLetters = DeadLetterPublisher.create(kafka.bootstrapServers, dlt)
        val consumer =
            FillConsumer.create(
                bootstrapServers = kafka.bootstrapServers,
                groupId = "trading-system-it",
                topic = topic,
                handler = RetryingHandler(capture, deadLetters, backoff = Duration.ofMillis(10)),
            )

        val producerProps =
            Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            }
        KafkaProducer(producerProps, StringSerializer(), StringSerializer()).use { producer ->
            producer.send(ProducerRecord(topic, "SIM", fillJson(size = 5)))
            producer.send(ProducerRecord(topic, "SIM", """{"not":"a fill"}"""))
            producer.send(ProducerRecord(topic, "SIM", fillJson(size = 2, aggressor = "OFFER", ts = 2000)))
            producer.flush()
        }

        consumer.start()
        try {
            awaitTrue("both healthy fills should be booked") { book.positionOf("SIM")?.quantity == 3L }
            assertEquals(2, store.saved.size, "one persisted position per healthy fill")

            // The poison record must be on the DLT with its provenance, original payload untouched.
            val dead = consumeOne(dlt)
            assertEquals("""{"not":"a fill"}""", dead.second)
            assertEquals("missing field: v", dead.first)
        } finally {
            consumer.close()
            deadLetters.close()
        }
    }

    private fun awaitTrue(
        message: String,
        condition: () -> Boolean,
    ) {
        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        while (!condition()) {
            assertTrue(System.nanoTime() < deadline, message)
            Thread.sleep(100)
        }
    }

    /** The first DLT record's (error message header, value). */
    private fun consumeOne(topic: String): Pair<String, String> {
        val props =
            Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
                put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-inspector")
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            }
        KafkaConsumer(props, StringDeserializer(), StringDeserializer()).use { consumer ->
            consumer.subscribe(listOf(topic))
            val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
            while (System.nanoTime() < deadline) {
                val records = consumer.poll(Duration.ofMillis(500))
                if (!records.isEmpty) {
                    val record = records.first()
                    val error = String(record.headers().lastHeader("dlt.error.message").value(), StandardCharsets.UTF_8)
                    return error to record.value()
                }
            }
        }
        throw AssertionError("nothing arrived on $topic within 30s")
    }
}
