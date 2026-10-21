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
 * Contract test for the logging-related blocks of the test fixture `application-test.yml` - bound instead of
 * the module's reference configuration (`src/test/resources/application.yml`, the documented example a host
 * copies from; a library ships no `application.yml`), so an edit to the reference cannot break it.
 *
 * 1. What is tested: that the exchange stream legatium-webclient-logging writes is switched on and audible -
 *    the `adapter-http-exchange` logger at INFO or below, the library's own logger at WARN or below - that this
 *    library's own loggers are addressed under their real package `eu.inqudium.veredium`, and that the
 *    `adapter-logging` block sets the values this module relies on: headers logged and masked with an explicit
 *    plaintext allowlist, bodies on failure only, the 16 KB capture cap.
 * 2. Success criteria: the bound `logging.level` map carries the logger names with a permitted level,
 *    and the `adapter-logging` keys bind to the expected values.
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
            // What is tested: the level of legatium's exchange logger, under exactly its default name.
            // Success criteria: `adapter-http-exchange` is present in logging.level at INFO, DEBUG or TRACE.
            // Why it matters: unlike the former diary, a higher level here FILTERS (WARN keeps failures,
            //   timeouts, cancellations and slow calls) rather than switching the feature off - so this pins the
            //   operational choice of seeing every exchange, and a typo in the name would address nothing.
            //
            // Given / When: the fixture's logging.level block
            // Then
            assertThat(levels).containsKey(EXCHANGE_LOGGER)
            assertThat(levels.getValue(EXCHANGE_LOGGER)).isIn("INFO", "DEBUG", "TRACE")
        }

        @Test
        fun `should keep the library's own logger audible for its breadcrumb and fail-open lines`() {
            // What is tested: the level of legatium's own logger.
            // Success criteria: `eu.inqudium.legatium.webclient.logging` is present at WARN or lower.
            // Why it matters: silencing it would hide the immediate WARN breadcrumb when a call throws, and the
            //   lines that say the logging itself failed open - the one place a broken exchange log reports itself.
            //
            // Given / When / Then
            assertThat(levels).containsKey(LIBRARY_LOGGER)
            assertThat(levels.getValue(LIBRARY_LOGGER)).isIn("WARN", "INFO", "DEBUG", "TRACE")
        }

        @Test
        fun `should keep the root level free of framework debug noise`() {
            // What is tested: the root level of the fixture.
            // Success criteria: root is INFO.
            // Why it matters: root at DEBUG turns on full framework logging - Netty, Reactor, Spring - which
            //   drowns the lines the other tests and an operator look for.
            //
            // Given / When / Then
            assertThat(levels["root"]).isEqualTo("INFO")
        }

        @Test
        fun `should address this library's own loggers under their real package`() {
            // What is tested: the "this module's own classes stay verbose" entry of the logging.level block.
            // Success criteria: `eu.inqudium.veredium` is set to DEBUG, and the `com.ing` entry inherited from the
            //   project this code was ported from is gone.
            // Why it matters: a level for a package nothing lives in is silently a no-op - the comment above it
            //   promised verbose library classes while the entry addressed the origin project's package and
            //   changed nothing.
            //
            // Given / When / Then
            assertThat(levels).containsEntry("eu.inqudium.veredium", "DEBUG")
            assertThat(levels.keys).noneMatch { it.startsWith("com.ing") }
        }

        @Test
        fun `should not address the former diary logger any more`() {
            // What is tested: that the fixture carries no level for the removed diary's logger.
            // Success criteria: `eu.inqudium.veredium.webclient.HttpExchange` is absent from logging.level.
            // Why it matters: nothing writes to that name since the diary was replaced; an entry for it would be
            //   a level pointing at nothing, and would suggest to a reader that the diary still exists.
            //
            // Given / When / Then
            assertThat(levels).doesNotContainKey("eu.inqudium.veredium.webclient.HttpExchange")
        }
    }

    @Nested
    inner class `The adapter-logging block` {
        @Test
        fun `should keep the library switched on by an explicit entry`() {
            // What is tested: `adapter-logging.enabled`.
            // Success criteria: the key is present and `true`.
            // Why it matters: explicit although it is the default - it is a context-start switch that removes
            //   the filter and its customizer, so its presence in the file is what makes an intentional `false`
            //   reviewable.
            //
            // Given / When / Then
            assertThat(adapterLogging).containsEntry("enabled", "true")
        }

        @Test
        fun `should log every header, masked by default, with the Host header in plaintext`() {
            // What is tested: the header selection and the plaintext allowlist.
            // Success criteria: includes "*" on both directions; Host on the request plaintext allowlist; no
            //   X-Access-Token on it.
            // Why it matters: the runbook reads the backend off the Host header next to adapter_name, and the
            //   token must stay a fingerprint whatever else is added to the allowlist.
            //
            // Given / When / Then
            assertThat(adapterLogging).containsEntry("request-headers.includes.0", "*")
            assertThat(adapterLogging).containsEntry("response-headers.includes.0", "*")
            assertThat(unmaskedRequestHeaders()).contains("Host")
            // And the token stays a fingerprint: it must not be on the allowlist, whatever else is.
            assertThat(unmaskedRequestHeaders()).noneMatch { it.startsWith("X-Access-Token", ignoreCase = true) }
        }

        @Test
        fun `should log bodies on failure only, capped at 16 KB`() {
            // What is tested: the body modes and the capture cap of the fixture.
            // Success criteria: `on-failure` for both directions and `max-body-bytes` 16384.
            // Why it matters: the diary logged every body of every call - customer data on every line.
            //   `on-failure` keeps the error body the runbook needs and drops the rest; 16384 is the library
            //   default, written out because it replaces the diary's 256 KB.
            //
            // Given / When / Then
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
