package io.github.damian1000.tradingsystem.web

import io.github.damian1000.orderbook.model.Side
import io.github.damian1000.riskengine.report.RiskReportAssembler
import io.github.damian1000.tradingsystem.capture.TradeCapture
import io.github.damian1000.tradingsystem.consume.Fill
import io.github.damian1000.tradingsystem.limits.LimitsReport
import io.github.damian1000.tradingsystem.limits.RiskLimits
import io.github.damian1000.tradingsystem.position.Position
import io.github.damian1000.tradingsystem.position.PositionBook
import io.github.damian1000.tradingsystem.position.PositionStore
import io.github.damian1000.tradingsystem.pricing.MarketAssumptions
import io.github.damian1000.tradingsystem.pricing.RiskGateway
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.BufferedReader
import java.math.BigDecimal
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/** The server against real loopback HTTP: routing, content types, the state JSON, and the SSE push. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DashboardServerTest {
    private class NoopStore : PositionStore {
        override fun save(position: Position) {}

        override fun loadAll(): List<Position> = emptyList()
    }

    private val broadcaster = SseBroadcaster()
    private val capture =
        TradeCapture(
            book = PositionBook(),
            store = NoopStore(),
            risk = RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default()),
            broadcaster = broadcaster,
            limitsView = { LimitsReport(RiskLimits(50, BigDecimal("5000")), emptyList(), emptyList(), 0) },
        )
    private val server = DashboardServer(capture, broadcaster, WebAssets.load(), port = 0)
    private val client = HttpClient.newHttpClient()

    @BeforeAll
    fun start() {
        server.start()
    }

    @AfterAll
    fun stop() {
        server.stop()
        broadcaster.close()
    }

    private fun get(path: String): HttpResponse<String> =
        client.send(
            HttpRequest.newBuilder(URI("http://127.0.0.1:${server.boundPort}$path")).GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    @Test
    fun `healthz answers ok for the deploy gate`() {
        val response = get("/healthz")
        assertEquals(200, response.statusCode())
        assertEquals("ok", response.body())
    }

    @Test
    fun `serves the UI with its content types`() {
        val index = get("/")
        assertEquals(200, index.statusCode())
        assertEquals("text/html; charset=utf-8", index.headers().firstValue("Content-Type").get())
        assertTrue(index.body().contains("TRADING SYSTEM"))

        assertEquals("text/css; charset=utf-8", get("/app.css").headers().firstValue("Content-Type").get())
        assertEquals("text/javascript; charset=utf-8", get("/app.js").headers().firstValue("Content-Type").get())
    }

    @Test
    fun `api state returns the current snapshot as JSON`() {
        val response = get("/api/state")
        assertEquals(200, response.statusCode())
        assertEquals("application/json", response.headers().firstValue("Content-Type").get())
        assertTrue(response.body().startsWith("""{"v":1,"positions":["""), response.body())
        assertTrue(response.body().contains(""""limits":{"""), response.body())
    }

    @Test
    fun `unknown paths are 404`() {
        assertEquals(404, get("/nope").statusCode())
    }

    @Test
    fun `non-GET methods are 405 with Allow`() {
        val response =
            client.send(
                HttpRequest
                    .newBuilder(URI("http://127.0.0.1:${server.boundPort}/api/state"))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        assertEquals(405, response.statusCode())
        assertEquals("GET", response.headers().firstValue("Allow").get())
    }

    @Test
    fun `the SSE stream sends the current snapshot then pushes on each fill`() {
        val lines = mutableListOf<String>()
        val initial = CountDownLatch(1)
        val pushed = CountDownLatch(1)
        val reader =
            Thread {
                val request = HttpRequest.newBuilder(URI("http://127.0.0.1:${server.boundPort}/api/stream")).GET().build()
                client.send(request, HttpResponse.BodyHandlers.ofInputStream()).body().bufferedReader().use { body: BufferedReader ->
                    while (true) {
                        val line = body.readLine() ?: break
                        synchronized(lines) { lines.add(line) }
                        if (line.startsWith("data: ")) {
                            initial.countDown()
                            if (line.contains(""""quantity":3""")) pushed.countDown()
                        }
                    }
                }
            }.apply {
                isDaemon = true
                start()
            }

        assertTrue(initial.await(5, TimeUnit.SECONDS), "initial snapshot frame")
        capture.onFill(Fill("SIM", BigDecimal("100.00"), 3, 1, 2, Side.BID, 1000))
        assertTrue(pushed.await(5, TimeUnit.SECONDS), "a fill pushes a fresh snapshot: ${synchronized(lines) { lines.toList() }}")
        reader.interrupt()
    }
}
