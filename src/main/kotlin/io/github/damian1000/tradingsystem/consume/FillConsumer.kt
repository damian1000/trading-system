package io.github.damian1000.tradingsystem.consume

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

/**
 * The poll loop over the fills topic, on its own thread. Offsets are committed after each fully
 * processed batch, so delivery is at-least-once: a crash between the position write and the
 * commit replays the batch on restart. Per-record failure never reaches this loop — the
 * [RecordHandler] (retry→DLT) owns it — so the loop only ever stops on [close].
 */
class FillConsumer(
    private val consumer: Consumer<String, String>,
    private val topic: String,
    private val handler: RecordHandler,
    private val pollTimeout: Duration = Duration.ofMillis(500),
) : AutoCloseable {
    private val thread = Thread(::run, "fill-consumer").apply { isDaemon = true }

    @Volatile
    private var running = false

    fun start() {
        running = true
        thread.start()
    }

    private fun run() {
        try {
            consumer.subscribe(listOf(topic))
            while (running) {
                val records = consumer.poll(pollTimeout)
                if (records.isEmpty) continue
                records.forEach(handler::handle)
                consumer.commitSync()
            }
        } catch (_: WakeupException) {
            // close() woke a blocked poll — fall through to cleanup.
        } finally {
            consumer.close()
        }
    }

    /**
     * Stops the loop and waits for it to finish. [Consumer] is single-threaded by contract;
     * `wakeup()` is its one thread-safe method and the documented way to abort a blocked poll,
     * so the consumer itself is always closed on the poll thread.
     */
    override fun close() {
        running = false
        consumer.wakeup()
        thread.join(CLOSE_TIMEOUT.toMillis())
    }

    companion object {
        private val CLOSE_TIMEOUT = Duration.ofSeconds(10)

        /** A consumer over a real [KafkaConsumer], reading the topic from its start on first attach. */
        fun create(
            bootstrapServers: String,
            groupId: String,
            topic: String,
            handler: RecordHandler,
        ): FillConsumer {
            val props =
                Properties().apply {
                    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                    put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                    put(ConsumerConfig.CLIENT_ID_CONFIG, "trading-system-fills")
                    // Offsets commit only after the batch is processed and persisted (see class doc).
                    put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                    // A brand-new group starts from the beginning: positions are built from the
                    // whole retained fill history, not just fills after first deploy.
                    put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                }
            return FillConsumer(KafkaConsumer(props, StringDeserializer(), StringDeserializer()), topic, handler)
        }
    }
}
