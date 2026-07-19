package io.github.damian1000.tradingsystem.capture

import io.github.damian1000.tradingsystem.consume.ConsumerProgress
import io.github.damian1000.tradingsystem.consume.Fill
import io.github.damian1000.tradingsystem.consume.FillSource
import io.github.damian1000.tradingsystem.limits.LimitsView
import io.github.damian1000.tradingsystem.position.PositionBook
import io.github.damian1000.tradingsystem.position.PositionStore
import io.github.damian1000.tradingsystem.position.RecordOutcome
import io.github.damian1000.tradingsystem.pricing.RiskGateway
import io.github.damian1000.tradingsystem.view.DashboardSnapshot
import io.github.damian1000.tradingsystem.web.Broadcaster
import java.util.concurrent.atomic.AtomicLong

/** What consumes parsed fills; [TradeCapture] is the production pipeline. */
fun interface FillHandler {
    fun onFill(
        fill: Fill,
        source: FillSource,
    )
}

/**
 * The application service on the consumer thread. Each fill goes to [PositionStore.record]
 * first — the transactional ledger insert plus position move — and memory changes only from the
 * committed result, so a retried or redelivered record can never move a position twice: the
 * ledger's unique key reports it as a duplicate and the book is left alone. Failures propagate
 * to the caller — the consumer's retry→DLT policy decides what happens next, not this class.
 *
 * Day PnL measures from [SessionOpens] — per-symbol opens derived from fill event time, warmed
 * from the ledger at startup — so positions *and* the PnL clock survive a restart together.
 */
class TradeCapture(
    private val book: PositionBook,
    private val store: PositionStore,
    private val risk: RiskGateway,
    private val broadcaster: Broadcaster,
    private val limitsView: LimitsView,
    private val opens: SessionOpens = SessionOpens(),
    initialProgress: ConsumerProgress? = null,
    private val deadLetters: () -> Long = { 0 },
) : FillHandler {
    /** Where this view sits on the stream — the readiness probe compares it with the limits view. */
    @Volatile
    var progress: ConsumerProgress? = initialProgress
        private set

    private val duplicateCount = AtomicLong()

    /** Replayed records the ledger rejected — nonzero after a retry, redelivery, or restart replay. */
    val duplicates: Long get() = duplicateCount.get()

    override fun onFill(
        fill: Fill,
        source: FillSource,
    ) {
        when (val outcome = store.record(fill, source)) {
            is RecordOutcome.Applied -> {
                opens.observe(fill)
                book.put(outcome.position)
                progress = ConsumerProgress(source.offset, fill.timeMillis)
                broadcaster.broadcast(snapshot().toJson())
            }
            RecordOutcome.Duplicate -> {
                duplicateCount.incrementAndGet()
                progress = ConsumerProgress(source.offset, fill.timeMillis)
            }
        }
    }

    /** The current state, repriced on request — every position, each in its own market. */
    fun snapshot(): DashboardSnapshot {
        val positions = book.all()
        return DashboardSnapshot(
            positions = positions,
            book = risk.bookReport(positions, opens::openFor),
            limits = limitsView.report(),
            positionsProgress = progress,
            duplicates = duplicates,
            deadLetters = deadLetters(),
        )
    }
}
