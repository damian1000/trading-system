package io.github.damian1000.tradingsystem.capture

import com.sun.net.httpserver.HttpExchange
import io.github.damian1000.orderbook.model.Side
import io.github.damian1000.riskengine.report.RiskReportAssembler
import io.github.damian1000.tradingsystem.consume.Fill
import io.github.damian1000.tradingsystem.position.Position
import io.github.damian1000.tradingsystem.position.PositionBook
import io.github.damian1000.tradingsystem.position.PositionStore
import io.github.damian1000.tradingsystem.pricing.MarketAssumptions
import io.github.damian1000.tradingsystem.pricing.RiskGateway
import io.github.damian1000.tradingsystem.web.Broadcaster
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class TradeCaptureTest {
    private class InMemoryStore : PositionStore {
        val saved = mutableListOf<Position>()
        var failNext = false

        override fun save(position: Position) {
            if (failNext) throw IllegalStateException("database blinked")
            saved.add(position)
        }

        override fun loadAll(): List<Position> = saved.toList()
    }

    private class RecordingBroadcaster : Broadcaster {
        val frames = mutableListOf<String>()

        override fun startHeartbeat(periodSeconds: Long) {}

        override fun broadcast(json: String) {
            frames.add(json)
        }

        override fun stream(
            exchange: HttpExchange,
            initialJson: String,
        ) = throw UnsupportedOperationException("not used in this test")
    }

    private val store = InMemoryStore()
    private val broadcaster = RecordingBroadcaster()
    private val capture =
        TradeCapture(
            book = PositionBook(),
            store = store,
            risk = RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default()),
            broadcaster = broadcaster,
        )

    private fun fill(
        side: Side = Side.BID,
        size: Long = 5,
        price: String = "100.00",
        ts: Long = 1000,
    ) = Fill("SIM", BigDecimal(price), size, 1, 2, side, ts)

    @Test
    fun `a fill is booked, persisted, and broadcast as a fresh snapshot`() {
        capture.onFill(fill())

        assertEquals(5, store.saved.single().quantity)
        val frame = broadcaster.frames.single()
        assertTrue(frame.contains(""""quantity":5"""), frame)
        assertTrue(frame.contains(""""report":{"""), "the broadcast snapshot carries the repriced report")
    }

    @Test
    fun `the first fill marks the session open and later fills measure PnL from it`() {
        capture.onFill(fill(price = "100.00"))
        capture.onFill(fill(price = "103.00", ts = 2000))

        val snapshot = capture.snapshot()
        assertEquals(BigDecimal("100.00"), snapshot.openPrice, "the open is the first fill's price, not the latest")
        assertTrue(snapshot.toJson().contains(""""openPrice":100.00"""), snapshot.toJson())
        assertTrue(snapshot.report!!.pnl != null, "with an open mark the report attributes day PnL")
    }

    @Test
    fun `before any fill the snapshot is explicitly empty`() {
        val snapshot = capture.snapshot()
        assertTrue(snapshot.positions.isEmpty())
        assertNull(snapshot.openPrice)
        assertNull(snapshot.report)
        assertTrue(broadcaster.frames.isEmpty(), "nothing to push until something trades")
    }

    @Test
    fun `a store failure propagates to the caller and nothing is broadcast`() {
        store.failNext = true
        assertThrows(IllegalStateException::class.java) { capture.onFill(fill()) }
        assertTrue(broadcaster.frames.isEmpty(), "a fill that didn't persist must not be announced")
    }
}
