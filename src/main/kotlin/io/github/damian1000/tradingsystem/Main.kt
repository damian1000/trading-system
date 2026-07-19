package io.github.damian1000.tradingsystem

import io.github.damian1000.riskengine.report.RiskReportAssembler
import io.github.damian1000.tradingsystem.capture.TradeCapture
import io.github.damian1000.tradingsystem.config.AppConfig
import io.github.damian1000.tradingsystem.consume.ConsumerProgress
import io.github.damian1000.tradingsystem.consume.DeadLetterPublisher
import io.github.damian1000.tradingsystem.consume.FillConsumer
import io.github.damian1000.tradingsystem.consume.RetryingHandler
import io.github.damian1000.tradingsystem.health.Readiness
import io.github.damian1000.tradingsystem.limits.LimitsChecker
import io.github.damian1000.tradingsystem.limits.RiskLimits
import io.github.damian1000.tradingsystem.position.JdbcPositionStore
import io.github.damian1000.tradingsystem.position.PositionBook
import io.github.damian1000.tradingsystem.pricing.MarketAssumptions
import io.github.damian1000.tradingsystem.pricing.RiskGateway
import io.github.damian1000.tradingsystem.web.DashboardServer
import io.github.damian1000.tradingsystem.web.SseBroadcaster
import io.github.damian1000.tradingsystem.web.WebAssets
import org.flywaydb.core.Flyway
import java.sql.DriverManager
import kotlin.system.exitProcess

/**
 * Composition root: reads config, migrates the schema, warms both views from the fill ledger,
 * and wires fills → positions → risk → dashboard plus the independent limits view over the same
 * topic. Plumbing only — every collaborator is constructed here and tested elsewhere.
 *
 * A consumer that dies on an unexpected exception exits the process: continuing would mean
 * committing past records that are in neither the ledger nor the DLT, and systemd's
 * `Restart=on-failure` brings the process back into a replay the ledger makes idempotent.
 */
fun main() {
    val config = AppConfig.fromEnv(System.getenv())

    Flyway
        .configure()
        .dataSource(config.dbUrl, config.dbUser, config.dbPassword)
        .load()
        .migrate()

    val store = JdbcPositionStore { DriverManager.getConnection(config.dbUrl, config.dbUser, config.dbPassword) }
    val ledger = store.loadLedger(config.fillsTopic)
    val ledgerProgress =
        ledger.highWaterOffsets.values.maxOrNull()?.let { offset ->
            ConsumerProgress(offset, ledger.fills.last().timeMillis)
        }

    val book = PositionBook().apply { restore(store.loadAll()) }
    val broadcaster = SseBroadcaster().apply { startHeartbeat() }
    val risk = RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default())
    val limits = LimitsChecker(RiskLimits(config.limitMaxPosition, config.limitMaxNotional))
    limits.warm(ledger.fills, ledgerProgress)
    val capture = TradeCapture(book, store, risk, broadcaster, limits, ledgerProgress)
    limits.onChange { broadcaster.broadcast(capture.snapshot().toJson()) }

    val fatal: (Exception) -> Unit = { error ->
        System.err.println("fill consumer failed: $error")
        exitProcess(1)
    }
    val deadLetters = DeadLetterPublisher.create(config.kafkaBootstrapServers, config.deadLetterTopic)
    val consumer =
        FillConsumer.create(
            bootstrapServers = config.kafkaBootstrapServers,
            groupId = config.groupId,
            topic = config.fillsTopic,
            handler = RetryingHandler(capture, deadLetters),
            onFatal = fatal,
        )
    val limitsConsumer =
        FillConsumer.createSeeking(
            bootstrapServers = config.kafkaBootstrapServers,
            topic = config.fillsTopic,
            handler = limits,
            startOffsets = ledger.highWaterOffsets,
            clientId = "trading-system-limits",
            onFatal = fatal,
        )

    val readiness =
        Readiness(
            consumers = listOf(consumer.health, limitsConsumer.health),
            databaseOk = store::ping,
            deadLettersPublished = deadLetters::published,
            deadLettersFailed = deadLetters::failed,
        )
    val server = DashboardServer(capture, broadcaster, WebAssets.load(), config.port, readiness)

    Runtime.getRuntime().addShutdownHook(
        Thread {
            consumer.close()
            limitsConsumer.close()
            deadLetters.close()
            broadcaster.close()
            server.stop()
        },
    )

    server.start()
    consumer.start()
    limitsConsumer.start()
}
