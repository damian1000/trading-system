package io.github.damian1000.tradingsystem.view

import io.github.damian1000.riskengine.report.RiskReportAssembler
import io.github.damian1000.tradingsystem.limits.LimitsReport
import io.github.damian1000.tradingsystem.limits.RiskLimits
import io.github.damian1000.tradingsystem.position.Position
import io.github.damian1000.tradingsystem.pricing.MarketAssumptions
import io.github.damian1000.tradingsystem.pricing.RiskGateway
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class DashboardSnapshotTest {
    private val emptyLimits = LimitsReport(RiskLimits(50, BigDecimal("5000")), emptyList(), emptyList(), 0)

    @Test
    fun `an untraded system serialises to an explicitly empty state`() {
        assertEquals(
            """{"v":1,"positions":[],"openPrice":null,"report":null,""" +
                """"limits":{"maxPosition":50,"maxNotional":5000,"symbols":[],"events":[],"malformed":0}}""",
            DashboardSnapshot(emptyList(), null, null, emptyLimits).toJson(),
        )
    }

    @Test
    fun `positions serialise with exact prices and the report embeds risk-engine's own JSON`() {
        val position = Position("SIM", -42, BigDecimal("101.00000000"), 1720620000000)
        val report =
            RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default())
                .report(position, BigDecimal("100.00"))!!

        val json = DashboardSnapshot(listOf(position), BigDecimal("100.00"), report, emptyLimits).toJson()

        assertTrue(
            json.startsWith(
                """{"v":1,"positions":[{"symbol":"SIM","quantity":-42,""" +
                    """"lastPrice":101.00000000,"lastTimeMillis":1720620000000}],"openPrice":100.00,"report":{""",
            ),
            json,
        )
        assertTrue(json.contains(""""greeks":{"""), "the report JSON is embedded as-is")
        assertTrue(json.contains(""""pnl":{"""), "a session-open mark yields day PnL")
        assertTrue(json.contains(""""limits":{"maxPosition":50,"""), "the limits JSON is embedded as-is")
    }

    @Test
    fun `symbols are JSON-escaped`() {
        val position = Position("""A"B\C""", 1, BigDecimal("1"), 0)
        val json = DashboardSnapshot(listOf(position), null, null, emptyLimits).toJson()
        assertTrue(json.contains(""""symbol":"A\"B\\C""""), json)
    }
}
