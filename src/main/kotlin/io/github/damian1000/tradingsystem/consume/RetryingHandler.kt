package io.github.damian1000.tradingsystem.consume

import io.github.damian1000.tradingsystem.capture.FillHandler
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Duration

/** What the consumer hands each raw record to; [RetryingHandler] is the production policy. */
fun interface RecordHandler {
    fun handle(record: ConsumerRecord<String, String>)
}

/**
 * The retry→DLT policy around fill processing, with the mechanism in the open. A record that
 * fails to parse goes straight to the dead-letter topic — malformed input never heals, so
 * retrying it only stalls the stream. A record whose *handling* fails (the database blinked, the
 * broker hiccuped) is retried up to [attempts] times, [backoff] apart, and dead-lettered on
 * exhaustion. Retrying the whole handler is safe because application is idempotent: the fill
 * ledger's unique key means an attempt that half-succeeded cannot apply again on the next try.
 *
 * A [DeadLetterPublishException] propagates: an unacknowledged dead-letter send means the record
 * is in neither stream, so the batch must not commit — the process fails fast and the committed
 * offset replays it, which idempotent application makes safe.
 *
 * [sleep] is injectable so tests drive the backoff without real waiting.
 */
class RetryingHandler(
    private val handler: FillHandler,
    private val deadLetters: DeadLetterPublisher,
    private val attempts: Int = DEFAULT_ATTEMPTS,
    private val backoff: Duration = DEFAULT_BACKOFF,
    private val sleep: (Duration) -> Unit = { Thread.sleep(it) },
) : RecordHandler {
    init {
        require(attempts >= 1) { "attempts must be at least 1, got $attempts" }
    }

    override fun handle(record: ConsumerRecord<String, String>) {
        val fill =
            try {
                Fill.parse(record.value())
            } catch (e: IllegalArgumentException) {
                deadLetters.publish(record, e)
                return
            }
        val source = FillSource(record.topic(), record.partition(), record.offset())
        var attempt = 1
        while (true) {
            try {
                handler.onFill(fill, source)
                return
            } catch (e: DeadLetterPublishException) {
                throw e
            } catch (e: Exception) {
                if (attempt == attempts) {
                    deadLetters.publish(record, e)
                    return
                }
                attempt++
                sleep(backoff)
            }
        }
    }

    companion object {
        const val DEFAULT_ATTEMPTS = 3
        val DEFAULT_BACKOFF: Duration = Duration.ofMillis(500)
    }
}
