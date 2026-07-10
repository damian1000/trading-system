package io.github.damian1000.tradingsystem.config

/**
 * Process configuration, read once from the environment at startup. Kafka and web settings have
 * defaults that match the box-2 runbook (broker on localhost, orderbook's topic names); the
 * database triple is required — Slice 1 is DB-backed by design, so a misconfigured process
 * fails at startup, not on the first fill.
 */
data class AppConfig(
    val kafkaBootstrapServers: String,
    val fillsTopic: String,
    val deadLetterTopic: String,
    val groupId: String,
    val dbUrl: String,
    val dbUser: String,
    val dbPassword: String,
    val port: Int,
) {
    companion object {
        fun fromEnv(env: Map<String, String>): AppConfig =
            AppConfig(
                kafkaBootstrapServers = env["KAFKA_BOOTSTRAP_SERVERS"] ?: "127.0.0.1:9092",
                fillsTopic = env["FILLS_TOPIC"] ?: "orderbook.fills",
                deadLetterTopic = env["FILLS_DLT_TOPIC"] ?: "orderbook.fills.DLT",
                groupId = env["KAFKA_GROUP_ID"] ?: "trading-system.positions",
                dbUrl = required(env, "DB_URL"),
                dbUser = required(env, "DB_USER"),
                dbPassword = required(env, "DB_PASSWORD"),
                port = port(env["PORT"] ?: "8082"),
            )

        private fun required(
            env: Map<String, String>,
            name: String,
        ): String = env[name]?.takeUnless { it.isBlank() } ?: throw IllegalArgumentException("$name must be set")

        private fun port(raw: String): Int {
            val port = raw.toIntOrNull() ?: throw IllegalArgumentException("PORT is not a number: '$raw'")
            require(port in 0..65535) { "PORT out of range: $port" }
            return port
        }
    }
}
