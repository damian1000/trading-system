package io.github.damian1000.tradingsystem.consume

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

/**
 * The poll loop over the fills topic, on its own thread, reporting into [ConsumerHealth].
 * Offsets are committed after each fully processed batch, so delivery is at-least-once; the
 * store's fill ledger makes the replay a batch of duplicates rather than a double-count.
 *
 * An exception the [RecordHandler] lets through is fatal by design: a valid record exhausted
 * its retries or a dead-letter send went unacknowledged — either way the record is in neither
 * stream, so continuing would commit past it. The loop marks the health record failed and hands
 * the error to [onFatal] — production exits the process and systemd restarts it into a safe
 * replay.
 */
class FillConsumer(
    private val consumer: Consumer<String, String>,
    private val topic: String,
    private val handler: RecordHandler,
    val health: ConsumerHealth,
    private val pollTimeout: Duration = Duration.ofMillis(500),
    private val commitOffsets: Boolean = true,
    private val startOffsets: Map<Int, Long>? = null,
    private val onFatal: (Exception) -> Unit = {},
    threadName: String = "fill-consumer",
) : AutoCloseable {
    private val thread = Thread(::run, threadName).apply { isDaemon = true }

    @Volatile
    private var running = false

    fun start() {
        running = true
        thread.start()
    }

    private fun run() {
        health.started()
        try {
            attach()
            while (running) {
                val records = consumer.poll(pollTimeout)
                health.polled()
                health.assigned(consumer.assignment().size)
                if (records.isEmpty) continue
                records.forEach(handler::handle)
                if (commitOffsets) consumer.commitSync()
            }
            health.stopped()
        } catch (_: WakeupException) {
            // close() woke a blocked poll — fall through to cleanup.
            health.stopped()
        } catch (e: Exception) {
            health.failed(e)
            onFatal(e)
        } finally {
            consumer.close()
        }
    }

    /**
     * Group mode subscribes and lets the group's committed offsets drive the start position.
     * Seek mode ([startOffsets] non-null) assigns every partition directly and starts each one
     * after its recorded offset — the durable ledger, not the group commit, is the truth of how
     * far this path has read. Partitions without a recorded offset start from the beginning.
     */
    private fun attach() {
        val offsets = startOffsets
        if (offsets == null) {
            consumer.subscribe(listOf(topic))
            return
        }
        val partitions = consumer.partitionsFor(topic).map { TopicPartition(topic, it.partition()) }
        consumer.assign(partitions)
        partitions.forEach { partition ->
            val applied = offsets[partition.partition()]
            if (applied != null) {
                consumer.seek(partition, applied + 1)
            } else {
                consumer.seekToBeginning(listOf(partition))
            }
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

        /**
         * A group-mode consumer over a real [KafkaConsumer], reading the topic from its start on
         * first attach. [clientId] must be unique per consumer in the process — it names the
         * poll thread and the JMX metrics, and two consumers sharing one collide on registration.
         */
        fun create(
            bootstrapServers: String,
            groupId: String,
            topic: String,
            handler: RecordHandler,
            clientId: String = "trading-system-fills",
            onFatal: (Exception) -> Unit = {},
        ): FillConsumer =
            FillConsumer(
                consumer = kafkaConsumer(bootstrapServers, groupId, clientId),
                topic = topic,
                handler = handler,
                health = ConsumerHealth(clientId),
                onFatal = onFatal,
                threadName = clientId,
            )

        /**
         * A seek-mode consumer: no group membership, offsets never committed, start position
         * supplied by the caller (the ledger's high-water marks). The limits path uses this so
         * its in-memory state and its stream position always come from the same durable truth.
         */
        fun createSeeking(
            bootstrapServers: String,
            topic: String,
            handler: RecordHandler,
            startOffsets: Map<Int, Long>,
            clientId: String,
            onFatal: (Exception) -> Unit = {},
        ): FillConsumer =
            FillConsumer(
                consumer = kafkaConsumer(bootstrapServers, groupId = null, clientId = clientId),
                topic = topic,
                handler = handler,
                health = ConsumerHealth(clientId),
                commitOffsets = false,
                startOffsets = startOffsets,
                onFatal = onFatal,
                threadName = clientId,
            )

        private fun kafkaConsumer(
            bootstrapServers: String,
            groupId: String?,
            clientId: String,
        ): Consumer<String, String> {
            val props =
                Properties().apply {
                    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                    if (groupId != null) put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                    put(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
                    // Offsets commit only after the batch is processed and persisted (see class doc).
                    put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                    // A brand-new group starts from the beginning: positions are built from the
                    // whole retained fill history, not just fills after first deploy.
                    put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                }
            return KafkaConsumer(props, StringDeserializer(), StringDeserializer())
        }
    }
}
