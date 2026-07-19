package io.github.damian1000.tradingsystem.health

import io.github.damian1000.tradingsystem.consume.ConsumerHealth
import io.github.damian1000.tradingsystem.consume.ConsumerProgress
import java.time.Clock
import java.time.Duration

/**
 * The service's operational truth, aggregated for `/readyz`: every consumer thread alive,
 * assigned, and polling recently; the database answering; the two views describing the same
 * stream position; the dead-letter counters in the open. `/healthz` proves the web process
 * answers — this proves the system is doing its job, so a server whose projections disagree is
 * not safe to serve, whatever the process state says.
 *
 * View coherence gets a grace window: the positions and limits consumers are independent, so
 * mid-burst they legitimately sit a few records apart for well under a second. Offsets that stay
 * unequal past [coherenceGrace] mean a view is stuck, and the probe goes 503 naming both
 * positions. Past dead-letter failures are reported but do not fail the probe (an unacknowledged
 * send already fails fast at the moment it happens).
 */
class Readiness(
    private val consumers: List<ConsumerHealth>,
    private val databaseOk: () -> Boolean,
    private val deadLettersPublished: () -> Long,
    private val deadLettersFailed: () -> Long,
    private val positionsView: () -> ConsumerProgress?,
    private val limitsView: () -> ConsumerProgress?,
    private val maxPollAge: Duration = MAX_POLL_AGE,
    private val coherenceGrace: Duration = COHERENCE_GRACE,
    private val clock: Clock = Clock.systemUTC(),
) {
    data class Probe(
        val ready: Boolean,
        val json: String,
    )

    @Volatile
    private var incoherentSinceMillis: Long? = null

    fun probe(): Probe {
        val consumerStates = consumers.map { consumerState(it) }
        val database = databaseOk()
        val views = viewsState()
        val ready = database && views.second && consumerStates.all { it.second }
        val json =
            """{"ready":$ready,"consumers":{${consumerStates.joinToString(",") { it.first }}},""" +
                """"database":{"ok":$database},"views":${views.first},""" +
                """"deadLetters":{"published":${deadLettersPublished()},"failed":${deadLettersFailed()}}}"""
        return Probe(ready, json)
    }

    private fun viewsState(): Pair<String, Boolean> {
        val positions = positionsView()?.offset
        val limits = limitsView()?.offset
        val coherent = positions == limits
        val incoherentFor =
            if (coherent) {
                incoherentSinceMillis = null
                null
            } else {
                val since = incoherentSinceMillis ?: clock.millis().also { incoherentSinceMillis = it }
                clock.millis() - since
            }
        val ok = coherent || incoherentFor!! <= coherenceGrace.toMillis()
        val json =
            """{"ok":$ok,"positionsOffset":${positions ?: "null"},"limitsOffset":${limits ?: "null"},""" +
                """"coherent":$coherent,"incoherentForMillis":${incoherentFor ?: "null"}}"""
        return json to ok
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
        private val COHERENCE_GRACE: Duration = Duration.ofSeconds(30)
    }
}
