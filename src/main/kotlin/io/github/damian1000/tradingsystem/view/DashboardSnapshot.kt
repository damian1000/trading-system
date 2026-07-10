package io.github.damian1000.tradingsystem.view

import io.github.damian1000.riskengine.report.RiskReport
import io.github.damian1000.tradingsystem.position.Position
import java.math.BigDecimal

/**
 * Everything the dashboard renders, at one moment: the booked [positions], the session-open mark
 * day PnL measures from, and risk-engine's [report] over the book (null before anything has
 * traded). [toJson] is the wire contract `/api/state` and the SSE stream both carry.
 */
data class DashboardSnapshot(
    val positions: List<Position>,
    val openPrice: BigDecimal?,
    val report: RiskReport?,
) {
    fun toJson(): String =
        """{"v":1,"positions":[${positions.joinToString(",", transform = ::positionJson)}],""" +
            """"openPrice":${openPrice?.toPlainString() ?: "null"},"report":${report?.toJson() ?: "null"}}"""

    // Prices are exact JSON numbers via their plain-string form, matching RiskReport's convention.
    private fun positionJson(position: Position): String =
        """{"symbol":${quote(position.symbol)},"quantity":${position.quantity},""" +
            """"lastPrice":${position.lastPrice.toPlainString()},"lastTimeMillis":${position.lastTimeMillis}}"""

    private fun quote(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
