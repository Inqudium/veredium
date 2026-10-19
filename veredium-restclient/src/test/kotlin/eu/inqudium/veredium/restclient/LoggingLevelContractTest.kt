package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.restclient.ConfigurationFiles.FIXTURE
import eu.inqudium.veredium.restclient.ConfigurationFiles.SHIPPED
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * Contract test for the logging-related blocks of the configuration. Which file a case binds follows the rule
 * in [ConfigurationFiles]: the logger IDENTITIES in both the shipped `application.yml` and the fixture, the
 * tunable `adapter-logging` VALUES in the fixture only.
 *
 * 1. What is tested: (both files) that the `logging.level` entry for the exchange stream addresses the SAME
 *    logger `adapter-logging.logger-name` makes the library write on, at INFO or below; that the library's own
 *    logger stays audible; that root is at INFO; that the library is switched on by an explicit entry.
 *    (fixture) the `adapter-logging` values the runbook relies on: headers logged and masked with an explicit
 *    plaintext allowlist, bodies on failure only, the 16 KB capture cap.
 * 2. How it is deemed successful: the bound `logging.level` map carries the logger name the `adapter-logging`
 *    block declares, with a permitted level, and the `adapter-logging` keys bind to the expected values.
 * 3. Why it matters: the exchange logger's name is a literal in two places of the SAME file - the level key and
 *    the library property - and nothing but this test relates them. A mismatch leaves the level addressing
 *    nothing, silently: the stream then inherits whatever its parent logger says (here `eu.inqudium.veredium: DEBUG`), and
 *    the operational knob "WARN = only failures, timeouts and slow calls" filters nothing. The shipped file
 *    carried exactly that drift for a while (`adapter-http-exchange: INFO` next to
 *    `logger-name: adapter-http-exchange`), unseen because the test bound the fixture alone. And the
 *    `adapter-logging` values are what the runbook's queries assume - the Host header readable, the token a
 *    fingerprint, the error body present - so a drift there is a runbook that stops answering, not a build
 *    that fails.
 */
class LoggingLevelContractTest {
    @Nested
    inner class `The logger identities, in both files` {
        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = [SHIPPED, FIXTURE])
        fun `should address the exchange stream's level at the logger the library writes on`(file: String) {
            // Given: the logger the adapter-logging block makes the library write on
            val loggerName = ConfigurationFiles.bindMap(file, "adapter-logging").getValue("logger-name")
            assertThat(loggerName).`as`("adapter-logging.logger-name in $file").isNotBlank()

            // When: the logging.level block of the same file
            val levels = ConfigurationFiles.bindMap(file, "logging.level")

            // Then: it carries THAT name, at INFO or below. A higher level here FILTERS (WARN keeps failures,
            // timeouts and slow calls) rather than switching the feature off - so this pins the operational
            // choice, not a trap. A key under any other name would address a logger nothing writes on.
            assertThat(levels)
                .`as`("logging.level in $file must address the exchange logger under the name adapter-logging.logger-name declares")
                .containsKey(loggerName)
            assertThat(levels.getValue(loggerName)).isIn("INFO", "DEBUG", "TRACE")
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = [SHIPPED, FIXTURE])
        fun `should keep the library's own logger audible for its breadcrumb and fail-open lines`(file: String) {
            // Given / When / Then: WARN or lower - silencing it would hide the immediate WARN breadcrumb when a
            // call throws, and the lines that say the logging itself failed open.
            val levels = ConfigurationFiles.bindMap(file, "logging.level")
            assertThat(levels).containsKey(LIBRARY_LOGGER)
            assertThat(levels.getValue(LIBRARY_LOGGER)).isIn("WARN", "INFO", "DEBUG", "TRACE")
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = [SHIPPED, FIXTURE])
        fun `should keep the root level free of framework debug noise`(file: String) {
            // Given / When / Then: root at DEBUG turns on full framework logging in the shipped artefact.
            assertThat(ConfigurationFiles.bindMap(file, "logging.level")["root"]).isEqualTo("INFO")
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = [SHIPPED, FIXTURE])
        fun `should keep the library switched on by an explicit entry`(file: String) {
            // Given / When / Then: explicit although it is the default - it is a context-start switch, so its
            // presence in the file is what makes an intentional `false` reviewable.
            assertThat(ConfigurationFiles.bindMap(file, "adapter-logging")).containsEntry("enabled", "true")
        }
    }

    @Nested
    inner class `The adapter-logging values, in the fixture` {
        private val adapterLogging: Map<String, String> = ConfigurationFiles.bindMap(FIXTURE, "adapter-logging")

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
            // Given / When / Then: logging every body of every call would put customer data on every line.
            // `on-failure` keeps the error body the runbook needs and drops the rest; 16384 is the library
            // default, written out so the cap is reviewable here. The SHIPPED file currently says `always` - a
            // deliberate rollout posture, and a value an operator tunes, which is why it is not pinned there
            // (LegatiumExchangeLoggingIntegrationTest reads the shipped mode as it is).
            assertThat(adapterLogging).containsEntry("log-request-body", "on-failure")
            assertThat(adapterLogging).containsEntry("log-response-body", "on-failure")
            assertThat(adapterLogging).containsEntry("max-body-bytes", "16384")
        }

        private fun unmaskedRequestHeaders(): List<String> = adapterLogging.filterKeys { it.startsWith("request-headers.unmasked.") }.values.toList()
    }

    private companion object {
        const val LIBRARY_LOGGER = "eu.inqudium.legatium.restclient.logging"
    }
}
