package io.github.damian1000.tradingsystem.position

import io.github.damian1000.tradingsystem.consume.Fill
import java.math.BigDecimal
import java.util.concurrent.ConcurrentHashMap

/**
 * A net holding built from the fill stream: signed [quantity] (positive long, negative short —
 * zero is a flat symbol that has traded), and the [lastPrice]/[lastTimeMillis] it marked at.
 */
data class Position(
    val symbol: String,
    val quantity: Long,
    val lastPrice: BigDecimal,
    val lastTimeMillis: Long,
)

/**
 * Net position per symbol, booked from the taker side of each fill: a BID aggressor bought
 * [Fill.size], an OFFER aggressor sold it. In-memory view of the [PositionStore]'s truth —
 * [restore] warms it from the store at startup. Thread-safe: the consumer thread writes, web
 * threads read.
 */
class PositionBook {
    private val positions = ConcurrentHashMap<String, Position>()

    /** Books the fill and returns the updated position. */
    fun apply(fill: Fill): Position =
        positions.compute(fill.symbol) { _, current ->
            Position(
                symbol = fill.symbol,
                quantity = (current?.quantity ?: 0L) + fill.signedSize,
                lastPrice = fill.price,
                lastTimeMillis = fill.timeMillis,
            )
        }!!

    fun positionOf(symbol: String): Position? = positions[symbol]

    /** Every symbol that has traded, stably ordered for rendering. */
    fun all(): List<Position> = positions.values.sortedBy { it.symbol }

    /** Loads persisted positions, replacing any in-memory state — startup only. */
    fun restore(persisted: List<Position>) {
        positions.clear()
        persisted.forEach { positions[it.symbol] = it }
    }
}
