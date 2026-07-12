package io.github.damian1000.tradingsystem.capture

import io.github.damian1000.tradingsystem.consume.Fill
import io.github.damian1000.tradingsystem.limits.LimitsView
import io.github.damian1000.tradingsystem.position.PositionBook
import io.github.damian1000.tradingsystem.position.PositionStore
import io.github.damian1000.tradingsystem.pricing.RiskGateway
import io.github.damian1000.tradingsystem.view.DashboardSnapshot
import io.github.damian1000.tradingsystem.web.Broadcaster
import java.math.BigDecimal

/** What consumes parsed fills; [TradeCapture] is the production pipeline. */
fun interface FillHandler {
    fun onFill(fill: Fill)
}

/**
 * The application service on the consumer thread: books each fill into the [PositionBook],
 * persists the updated position, reprices the book through the [RiskGateway], and hands the
 * fresh snapshot to the [Broadcaster]. Failures propagate to the caller — the consumer's
 * retry→DLT policy decides what happens next, not this class.
 *
 * The first fill this process sees marks the session open; day PnL measures from it. A restart
 * therefore restarts the PnL clock — positions survive (restored from the store), the open mark
 * does not.
 */
class TradeCapture(
    private val book: PositionBook,
    private val store: PositionStore,
    private val risk: RiskGateway,
    private val broadcaster: Broadcaster,
    private val limitsView: LimitsView,
) : FillHandler {
    @Volatile
    private var openPrice: BigDecimal? = null

    override fun onFill(fill: Fill) {
        if (openPrice == null) openPrice = fill.price
        store.save(book.apply(fill))
        broadcaster.broadcast(snapshot().toJson())
    }

    /**
     * The current state, repriced on request. Slice 1 trades a single instrument, so the report
     * covers the first (only) position; the multi-symbol book generalises in a later slice.
     */
    fun snapshot(): DashboardSnapshot {
        val positions = book.all()
        return DashboardSnapshot(positions, openPrice, risk.report(positions.firstOrNull(), openPrice), limitsView.report())
    }
}
