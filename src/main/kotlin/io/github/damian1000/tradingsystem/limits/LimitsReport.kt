package io.github.damian1000.tradingsystem.limits

import java.math.BigDecimal

/** Which ceiling a breach event refers to. */
enum class LimitKind { POSITION, NOTIONAL }

/**
 * One transition of a symbol's standing against a single limit: [breached] is true when the
 * limit was crossed, false when exposure dropped back under. [timeMillis] is the fill's own
 * timestamp, so replaying the stream rebuilds the same events.
 */
data class BreachEvent(
    val symbol: String,
    val kind: LimitKind,
    val breached: Boolean,
    val value: BigDecimal,
    val limit: BigDecimal,
    val timeMillis: Long,
)

/** A symbol's current exposure against both limits. Utilisations are 4-dp fractions of the limit. */
data class SymbolLimits(
    val symbol: String,
    val netQuantity: Long,
    val lastPrice: BigDecimal,
    val notional: BigDecimal,
    val positionUtilisation: BigDecimal,
    val notionalUtilisation: BigDecimal,
    val breached: Boolean,
)

/**
 * The limits consumer's whole view at one moment: per-symbol exposures, the bounded breach/clear
 * history (newest first), and how many malformed records were skipped. [toJson] follows the
 * dashboard's wire convention — one line, exact decimals as plain JSON numbers.
 */
data class LimitsReport(
    val limits: RiskLimits,
    val symbols: List<SymbolLimits>,
    val events: List<BreachEvent>,
    val malformed: Long,
) {
    fun toJson(): String =
        """{"maxPosition":${limits.maxAbsPosition},"maxNotional":${limits.maxNotional.toPlainString()},""" +
            """"symbols":[${symbols.joinToString(",", transform = ::symbolJson)}],""" +
            """"events":[${events.joinToString(",", transform = ::eventJson)}],"malformed":$malformed}"""

    private fun symbolJson(s: SymbolLimits): String =
        """{"symbol":${quote(s.symbol)},"netQuantity":${s.netQuantity},"lastPrice":${s.lastPrice.toPlainString()},""" +
            """"notional":${s.notional.toPlainString()},"positionUtilisation":${s.positionUtilisation.toPlainString()},""" +
            """"notionalUtilisation":${s.notionalUtilisation.toPlainString()},"breached":${s.breached}}"""

    private fun eventJson(e: BreachEvent): String =
        """{"symbol":${quote(e.symbol)},"kind":"${e.kind}","breached":${e.breached},""" +
            """"value":${e.value.toPlainString()},"limit":${e.limit.toPlainString()},"ts":${e.timeMillis}}"""

    private fun quote(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
