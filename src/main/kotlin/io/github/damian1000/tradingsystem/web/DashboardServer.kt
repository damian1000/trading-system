package io.github.damian1000.tradingsystem.web

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.github.damian1000.tradingsystem.capture.TradeCapture
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * HTTP transport for the dashboard: the static UI, the current state as JSON, and an SSE stream
 * that pushes a fresh snapshot on every fill. Plumbing only — every number comes from
 * [TradeCapture]'s snapshot over risk-engine's calculators, and the front end is a thin renderer
 * of [io.github.damian1000.tradingsystem.view.DashboardSnapshot.toJson]. JDK [HttpServer] on a
 * cached pool, no web framework.
 */
class DashboardServer(
    private val capture: TradeCapture,
    private val broadcaster: Broadcaster,
    private val assets: WebAssets,
    private val port: Int,
) {
    private lateinit var server: HttpServer
    private lateinit var executor: ExecutorService

    /** Binds and starts serving; requesting port 0 binds an ephemeral port (see [boundPort]). */
    fun start() {
        server = HttpServer.create(InetSocketAddress(port), 0)
        executor = Executors.newCachedThreadPool { Thread(it).apply { isDaemon = true } }
        server.executor = executor
        server.createContext("/", ::route)
        server.start()
        println("Trading system dashboard listening on :$boundPort")
    }

    /** The port actually bound — differs from the requested one when 0 (ephemeral) was asked for. */
    val boundPort: Int get() = server.address.port

    /** Stops accepting connections and shuts down the request pool this server created. */
    fun stop() {
        server.stop(0)
        executor.shutdownNow()
    }

    private fun route(exchange: HttpExchange) {
        when (exchange.requestURI.path) {
            "/healthz" -> get(exchange) { respond(exchange, 200, "text/plain", "ok") }
            "/" -> get(exchange) { respond(exchange, 200, "text/html; charset=utf-8", assets.indexHtml) }
            "/app.css" -> get(exchange) { respond(exchange, 200, "text/css; charset=utf-8", assets.appCss) }
            "/app.js" -> get(exchange) { respond(exchange, 200, "text/javascript; charset=utf-8", assets.appJs) }
            "/api/state" -> get(exchange) { respond(exchange, 200, "application/json", capture.snapshot().toJson()) }
            "/api/stream" -> get(exchange) { broadcaster.stream(exchange, capture.snapshot().toJson()) }
            else -> respond(exchange, 404, "text/plain", "not found")
        }
    }

    private inline fun get(
        exchange: HttpExchange,
        handler: () -> Unit,
    ) {
        if (exchange.requestMethod == "GET") {
            handler()
        } else {
            exchange.responseHeaders.add("Allow", "GET")
            respond(exchange, 405, "text/plain", "method not allowed")
        }
    }

    private fun respond(
        exchange: HttpExchange,
        status: Int,
        contentType: String,
        body: String,
    ) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.responseHeaders.add("Content-Type", contentType)
        exchange.sendResponseHeaders(status, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }
}
