package io.github.damian1000.tradingsystem.view

import io.github.damian1000.riskengine.report.RiskReport
import io.github.damian1000.tradingsystem.consume.ConsumerProgress
import io.github.damian1000.tradingsystem.limits.LimitsReport
import io.github.damian1000.tradingsystem.position.Position
import java.math.BigDecimal

/**
 * Everything the dashboard renders, at one moment: the booked [positions], the session-open mark
 * day PnL measures from, risk-engine's [report] over the book (null before anything has
 * traded), the [limits] consumer's exposure view, and where each consumer path sits on the
 * stream. The two paths are independent consumers, so the `sync` block states whether they
 * describe the same stream position instead of leaving the reader to assume it. [toJson] is the
 * wire contract `/api/state` and the SSE stream both carry.
 */
data class DashboardSnapshot(
    val positions: List<Position>,
    val openPrice: BigDecimal?,
    val report: RiskReport?,
    val limits: LimitsReport,
    val positionsProgress: ConsumerProgress? = null,
    val duplicates: Long = 0,
) {
    /** True when both paths have read to the same offset (or neither has seen a fill). */
    val coherent: Boolean get() = positionsProgress?.offset == limits.progress?.offset

    fun toJson(): String =
        """{"v":1,"positions":[${positions.joinToString(",", transform = ::positionJson)}],""" +
            """"openPrice":${openPrice?.toPlainString() ?: "null"},"report":${report?.toJson() ?: "null"},""" +
            """"limits":${limits.toJson()},""" +
            """"sync":{"positions":${progressJson(positionsProgress)},"limits":${progressJson(limits.progress)},""" +
            """"coherent":$coherent,"duplicatesDropped":$duplicates}}"""

    private fun progressJson(p: ConsumerProgress?): String =
        if (p == null) "null" else """{"offset":${p.offset},"fillTs":${p.fillTimeMillis}}"""

    // Prices are exact JSON numbers via their plain-string form, matching RiskReport's convention.
    private fun positionJson(position: Position): String =
        """{"symbol":${quote(position.symbol)},"quantity":${position.quantity},""" +
            """"lastPrice":${position.lastPrice.toPlainString()},"lastTimeMillis":${position.lastTimeMillis}}"""

    private fun quote(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
