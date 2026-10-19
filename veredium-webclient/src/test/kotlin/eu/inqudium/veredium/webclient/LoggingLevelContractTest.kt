package eu.inqudium.veredium.webclient

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.io.ClassPathResource

/**
 * Contract test for the logging-related blocks of the test fixture `application-test.yml` (bound instead of the
 * shipped `src/main/resources/application.yml`, so an operational edit to the shipped file cannot break it).
 *
 * 1. What is tested: that the exchange stream legatium-webclient-logging writes is switched on and audible -
 *    the `adapter-http-exchange` logger at INFO or below, the library's own logger at WARN or below - and that
 *    the `adapter-logging` block sets the values this module relies on: headers logged and masked with an
 *    explicit plaintext allowlist, bodies on failure only, the 16 KB capture cap.
 * 2. How it is deemed successful: the bound `logging.level` map carries both logger names with a permitted
 *    level, and the `adapter-logging` keys bind to the expected values.
 * 3. Why it matters: the logger names are literals in a YAML file and in the library; a typo in either leaves
 *    the level addressing nothing, silently. And the `adapter-logging` values are what the runbook's queries
 *    assume - the Host header readable, the token a fingerprint, the error body present - so a drift here is a
 *    runbook that stops answering, not a build that fails.
 */
class LoggingLevelContractTest {
    private val levels: Map<String, String> = bind("logging.level")
    private val adapterLogging: Map<String, String> = bind("adapter-logging")

    @Nested
    inner class `The logger levels` {
        @Test
        fun `should keep the exchange stream at a level that shows every exchange`() {
            // Given / When: the fixture's logging.level block
            // Then: the library's exchange logger, under exactly its default name, at INFO or below. Unlike the
            // former diary, a higher level here FILTERS (WARN keeps failures, timeouts, cancellations and slow
            // calls) rather than switching the feature off - so this pins the operational choice, not a trap.
            assertThat(levels).containsKey(EXCHANGE_LOGGER)
            assertThat(levels.getValue(EXCHANGE_LOGGER)).isIn("INFO", "DEBUG", "TRACE")
        }

        @Test
        fun `should keep the library's own logger audible for its breadcrumb and fail-open lines`() {
            // Given / When / Then: WARN or lower - silencing it would hide the immediate WARN breadcrumb when a
            // call throws, and the lines that say the logging itself failed open.
            assertThat(levels).containsKey(LIBRARY_LOGGER)
            assertThat(levels.getValue(LIBRARY_LOGGER)).isIn("WARN", "INFO", "DEBUG", "TRACE")
        }

        @Test
        fun `should keep the root level free of framework debug noise`() {
            // Given / When / Then: root at DEBUG turns on full framework logging in the shipped artefact.
            assertThat(levels["root"]).isEqualTo("INFO")
        }

        @Test
        fun `should not address the former diary logger any more`() {
            // Given / When / Then: nothing writes to that name since the diary was replaced; an entry for it
            // would be a level pointing at nothing.
            assertThat(levels).doesNotContainKey("eu.inqudium.veredium.webclient.HttpExchange")
        }
    }

    @Nested
    inner class `The adapter-logging block` {
        @Test
        fun `should keep the library switched on by an explicit entry`() {
            // Given / When / Then: explicit although it is the default - it is a context-start switch, so its
            // presence in the file is what makes an intentional `false` reviewable.
            assertThat(adapterLogging).containsEntry("enabled", "true")
        }

        @Test
        fun `should log every header, masked by default, with the Host header in plaintext`() {
            // Given / When / Then: includes "*" on both directions, and Host on the request plaintext allowlist
            // - the runbook reads the backend off it next to adapter_name.
            assertThat(adapterLogging).containsEntry("request-headers.includes.0", "*")
            assertThat(adapterLogging).containsEntry("response-headers.includes.0", "*")
            assertThat(unmaskedRequestHeaders()).contains("Host")
            // And the token stays a fingerprint: it must not be on the allowlist, whatever else is.
            assertThat(unmaskedRequestHeaders()).noneMatch { it.startsWith("X-Access-Token", ignoreCase = true) }
        }

        @Test
        fun `should log bodies on failure only, capped at 16 KB`() {
            // Given / When / Then: the diary logged every body of every call - customer data on every line.
            // `on-failure` keeps the error body the runbook needs and drops the rest; 16384 is the library
            // default, written out because it replaces the diary's 256 KB.
            assertThat(adapterLogging).containsEntry("log-request-body", "on-failure")
            assertThat(adapterLogging).containsEntry("log-response-body", "on-failure")
            assertThat(adapterLogging).containsEntry("max-body-bytes", "16384")
        }

        private fun unmaskedRequestHeaders(): List<String> = adapterLogging.filterKeys { it.startsWith("request-headers.unmasked.") }.values.toList()
    }

    /** Bind one prefix of the test fixture the way Spring Boot binds a config file, flattened to strings. */
    private fun bind(prefix: String): Map<String, String> {
        val sources = YamlPropertySourceLoader().load("application-test.yml", ClassPathResource("application-test.yml"))
        return Binder(ConfigurationPropertySources.from(sources))
            .bind(prefix, Bindable.mapOf(String::class.java, String::class.java))
            .orElseThrow { AssertionError("$prefix did not bind from application-test.yml") }
    }

    private companion object {
        /** legatium's default exchange logger; the module keeps the default so the library's docs apply. */
        const val EXCHANGE_LOGGER = "adapter-http-exchange"
        const val LIBRARY_LOGGER = "eu.inqudium.legatium.webclient.logging"
    }
}
