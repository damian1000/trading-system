package io.github.damian1000.tradingsystem.health

import io.github.damian1000.tradingsystem.consume.ConsumerHealth
import java.time.Duration

/**
 * The service's operational truth, aggregated for `/readyz`: every consumer thread alive,
 * assigned, and polling recently; the database answering; the dead-letter counters in the open.
 * `/healthz` proves the web process answers — this proves the system is doing its job. Not
 * ready means ingestion or persistence is broken *now*; past dead-letter failures are reported
 * but do not fail the probe (an unacknowledged send already fails fast at the moment it
 * happens).
 */
class Readiness(
    private val consumers: List<ConsumerHealth>,
    private val databaseOk: () -> Boolean,
    private val deadLettersPublished: () -> Long,
    private val deadLettersFailed: () -> Long,
    private val maxPollAge: Duration = MAX_POLL_AGE,
) {
    data class Probe(
        val ready: Boolean,
        val json: String,
    )

    fun probe(): Probe {
        val consumerStates = consumers.map { consumerState(it) }
        val database = databaseOk()
        val ready = database && consumerStates.all { it.second }
        val json =
            """{"ready":$ready,"consumers":{${consumerStates.joinToString(",") { it.first }}},""" +
                """"database":{"ok":$database},""" +
                """"deadLetters":{"published":${deadLettersPublished()},"failed":${deadLettersFailed()}}}"""
        return Probe(ready, json)
    }

    private fun consumerState(health: ConsumerHealth): Pair<String, Boolean> {
        val pollAge = health.pollAgeMillis()
        val polling = pollAge != null && pollAge <= maxPollAge.toMillis()
        val ok = health.threadAlive && health.assigned && polling && health.fatal == null
        val json =
            """"${health.name}":{"ok":$ok,"threadAlive":${health.threadAlive},"assigned":${health.assigned},""" +
                """"pollAgeMillis":${pollAge ?: "null"},"fatal":${health.fatal?.let { quote(it) } ?: "null"}}"""
        return json to ok
    }

    private fun quote(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

    companion object {
        private val MAX_POLL_AGE: Duration = Duration.ofSeconds(30)
    }
}
