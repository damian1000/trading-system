package io.github.damian1000.tradingsystem

import io.github.damian1000.riskengine.report.RiskReportAssembler
import io.github.damian1000.tradingsystem.capture.TradeCapture
import io.github.damian1000.tradingsystem.config.AppConfig
import io.github.damian1000.tradingsystem.consume.DeadLetterPublisher
import io.github.damian1000.tradingsystem.consume.FillConsumer
import io.github.damian1000.tradingsystem.consume.RetryingHandler
import io.github.damian1000.tradingsystem.position.JdbcPositionStore
import io.github.damian1000.tradingsystem.position.PositionBook
import io.github.damian1000.tradingsystem.pricing.MarketAssumptions
import io.github.damian1000.tradingsystem.pricing.RiskGateway
import io.github.damian1000.tradingsystem.web.DashboardServer
import io.github.damian1000.tradingsystem.web.SseBroadcaster
import io.github.damian1000.tradingsystem.web.WebAssets
import org.flywaydb.core.Flyway
import java.sql.DriverManager

/**
 * Composition root: reads config, migrates the schema, warms the position book from the store,
 * and wires fills → positions → risk → dashboard. Plumbing only — every collaborator is
 * constructed here and tested elsewhere.
 */
fun main() {
    val config = AppConfig.fromEnv(System.getenv())

    Flyway
        .configure()
        .dataSource(config.dbUrl, config.dbUser, config.dbPassword)
        .load()
        .migrate()

    val store = JdbcPositionStore { DriverManager.getConnection(config.dbUrl, config.dbUser, config.dbPassword) }
    val book = PositionBook().apply { restore(store.loadAll()) }
    val broadcaster = SseBroadcaster().apply { startHeartbeat() }
    val risk = RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default())
    val capture = TradeCapture(book, store, risk, broadcaster)

    val deadLetters = DeadLetterPublisher.create(config.kafkaBootstrapServers, config.deadLetterTopic)
    val consumer =
        FillConsumer.create(
            bootstrapServers = config.kafkaBootstrapServers,
            groupId = config.groupId,
            topic = config.fillsTopic,
            handler = RetryingHandler(capture, deadLetters),
        )
    val server = DashboardServer(capture, broadcaster, WebAssets.load(), config.port)

    Runtime.getRuntime().addShutdownHook(
        Thread {
            consumer.close()
            deadLetters.close()
            broadcaster.close()
            server.stop()
        },
    )

    server.start()
    consumer.start()
}
