package io.github.damian1000.tradingsystem.pricing

import io.github.damian1000.riskengine.report.RiskReportAssembler
import io.github.damian1000.tradingsystem.position.Position
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.closeTo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class RiskGatewayTest {
    private val gateway = RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default())

    private fun position(
        quantity: Long,
        lastPrice: String,
    ) = Position("SIM", quantity, BigDecimal(lastPrice), 1000)

    @Test
    fun `no position yet means no report — nothing to mark against`() {
        assertNull(gateway.report(null, null))
    }

    @Test
    fun `a long equity position values to quantity times spot with delta equal to quantity`() {
        val report = gateway.report(position(100, "50.00"), openPrice = null)!!
        assertThat(report.valuation.amount.toDouble(), closeTo(5000.0, 1e-6))
        assertThat(report.greeks.delta, closeTo(100.0, 1e-4))
        assertNull(report.pnl, "no session-open mark, no day PnL")
    }

    @Test
    fun `a short position carries negative valuation and delta`() {
        val report = gateway.report(position(-40, "25.00"), openPrice = null)!!
        assertThat(report.valuation.amount.toDouble(), closeTo(-1000.0, 1e-6))
        assertThat(report.greeks.delta, closeTo(-40.0, 1e-4))
    }

    @Test
    fun `a flat book reports zero valuation but still carries the report shape`() {
        val report = gateway.report(position(0, "50.00"), openPrice = null)!!
        assertEquals(0.0, report.valuation.amount.toDouble())
        assertEquals(0.0, report.greeks.delta)
        assertEquals(
            0.0,
            report.parametric.valueAtRisk.amount
                .toDouble(),
        )
    }

    @Test
    fun `with a session-open mark the report attributes day PnL from it`() {
        val report = gateway.report(position(100, "51.00"), openPrice = BigDecimal("50.00"))!!
        val pnl = report.pnl
        assertNotNull(pnl)
        // Equity PnL is linear: 100 shares up 1.00. Theta on cash equity is zero, so the move
        // explains itself and the residual vanishes.
        assertThat(pnl!!.actual.amount.toDouble(), closeTo(100.0, 1e-6))
        assertThat(pnl.residual.amount.toDouble(), closeTo(0.0, 1e-2))
    }

    @Test
    fun `both VaR methods produce a positive loss estimate for a held position`() {
        val report = gateway.report(position(100, "50.00"), openPrice = null)!!
        assertTrue(
            report.parametric.valueAtRisk.amount
                .toDouble() > 0,
        )
        assertTrue(
            report.historical.valueAtRisk.amount
                .toDouble() > 0,
        )
        assertEquals(0.99, report.confidence)
    }

    @Test
    fun `the default assumptions are reproducible across instances`() {
        assertEquals(MarketAssumptions.default().scenarioReturns, MarketAssumptions.default().scenarioReturns)
        assertEquals(250, MarketAssumptions.default().scenarioReturns.size)
    }
}
