package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.testsupport.ConfigurationFiles
import eu.inqudium.veredium.testsupport.ConfigurationFiles.FIXTURE
import eu.inqudium.veredium.testsupport.ConfigurationFiles.REFERENCE
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * Contract test for the logging-related blocks of the configuration. Which file a case binds follows the rule
 * in [ConfigurationFiles]: the logger IDENTITIES in both the reference `application.yml` and the fixture, the
 * tunable `adapter-logging` VALUES in the fixture only.
 *
 * In both files the tests pin that the `logging.level` entry for the exchange stream addresses the SAME logger
 * `adapter-logging.logger-name` makes the library write on, at INFO or below; that the library's own logger stays
 * audible; that this module's own classes are addressed under their real package (`eu.inqudium.veredium`) and no
 * key names the origin project's package; that root is at INFO; and that the library is switched on by an explicit
 * entry. In the fixture they pin the `adapter-logging` values the log queries rely on: headers logged and masked
 * with an explicit plaintext allowlist, bodies on failure only, the 16 KB capture cap.
 *
 * The exchange logger's name is a literal in two places of the SAME file - the level key and the library
 * property - and nothing but these tests relates them. A mismatch leaves the level addressing nothing, silently:
 * the stream then inherits the root level, and the operational knob "WARN = only failures, timeouts and slow
 * calls" filters nothing. Such a drift in the reference file (a level key that differs from `logger-name` by one
 * segment) is invisible to a test bound to the fixture alone, and so is a level for the origin project's package
 * (`com.ing`), whose classes do not exist here - hence both files. The `adapter-logging` values are what the log
 * queries assume - the Host header readable, the token a fingerprint, the error body present - so a drift there is
 * a query that stops answering, not a build that fails.
 */
class LoggingLevelContractTest {
    @Nested
    inner class `The logger identities, in both files` {
        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = [REFERENCE, FIXTURE])
        fun `should address the exchange stream's level at the logger the library writes on`(file: String) {
            // What is tested: the pair "level key" / "adapter-logging.logger-name" inside one file.
            // Success criteria: logging.level carries the declared logger name as a key, at INFO, DEBUG or TRACE.
            // Why it matters: the two literals are related by nothing but this test; a mismatch makes the level
            //   address a logger nobody writes on, and the stream silently follows root instead.
            //
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
        @ValueSource(strings = [REFERENCE, FIXTURE])
        fun `should keep the library's own logger audible for its breadcrumb and fail-open lines`(file: String) {
            // What is tested: the level of legatium's own logger, `eu.inqudium.legatium.restclient.logging`.
            // Success criteria: the key is present at WARN or lower.
            // Why it matters: silencing it would hide the immediate WARN breadcrumb when a call throws, and the
            //   lines that say the logging itself failed open.
            //
            // Given / When
            val levels = ConfigurationFiles.bindMap(file, "logging.level")

            // Then
            assertThat(levels).containsKey(LIBRARY_LOGGER)
            assertThat(levels.getValue(LIBRARY_LOGGER)).isIn("WARN", "INFO", "DEBUG", "TRACE")
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = [REFERENCE, FIXTURE])
        fun `should address this module's own classes under their real package`(file: String) {
            // What is tested: the level line for the module's own loggers - `eu.inqudium.veredium`, the package
            //   ResilientRestClient and its factory log under.
            // Success criteria: the key is present, and no key of the origin project's package (`com.ing`) is.
            // Why it matters: the comment above the line promises "this module's own classes stay verbose"; a
            //   key for a package that does not exist here keeps that promise for nobody, and a reader copying
            //   the reference configuration copies a level that addresses nothing.
            //
            // Given / When
            val levels = ConfigurationFiles.bindMap(file, "logging.level")

            // Then
            assertThat(levels).containsKey(MODULE_LOGGER)
            assertThat(levels.keys).noneMatch { it.startsWith("com.ing") }
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = [REFERENCE, FIXTURE])
        fun `should keep the root level free of framework debug noise`(file: String) {
            // What is tested: the root level.
            // Success criteria: root is INFO.
            // Why it matters: root at DEBUG turns on full framework logging for every host that copies the file.
            //
            // Given / When / Then
            assertThat(ConfigurationFiles.bindMap(file, "logging.level")["root"]).isEqualTo("INFO")
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = [REFERENCE, FIXTURE])
        fun `should keep the library switched on by an explicit entry`(file: String) {
            // What is tested: `adapter-logging.enabled`.
            // Success criteria: it is present and `true`.
            // Why it matters: explicit although it is the default - it is a context-start switch, so its presence
            //   in the file is what makes an intentional `false` reviewable.
            //
            // Given / When / Then
            assertThat(ConfigurationFiles.bindMap(file, "adapter-logging")).containsEntry("enabled", "true")
        }
    }

    @Nested
    inner class `The adapter-logging values, in the fixture` {
        private val adapterLogging: Map<String, String> = ConfigurationFiles.bindMap(FIXTURE, "adapter-logging")

        @Test
        fun `should log every header, masked by default, with the Host header in plaintext`() {
            // What is tested: the header selection and masking of the fixture's adapter-logging block.
            // Success criteria: includes "*" on both directions, Host on the request plaintext allowlist, and no
            //   X-Access-Token variant on it.
            // Why it matters: a log query reads the backend off the Host header next to adapter_name, and the
            //   token must stay a fingerprint whatever else is allowed in plaintext.
            //
            // Given / When / Then
            assertThat(adapterLogging).containsEntry("request-headers.includes.0", "*")
            assertThat(adapterLogging).containsEntry("response-headers.includes.0", "*")
            assertThat(unmaskedRequestHeaders()).contains("Host")
            assertThat(unmaskedRequestHeaders()).noneMatch { it.startsWith("X-Access-Token", ignoreCase = true) }
        }

        @Test
        fun `should log bodies on failure only, capped at 16 KB`() {
            // What is tested: the body modes and the capture cap of the fixture.
            // Success criteria: both directions `on-failure`, `max-body-bytes` 16384.
            // Why it matters: logging every body of every call would put customer data on every line;
            //   `on-failure` keeps the error body an operator needs and drops the rest, and 16384 - the library
            //   default - is written out so the cap is reviewable. The body mode of the REFERENCE file is a
            //   rollout posture and a value an operator tunes, which is why it is not pinned there
            //   (LegatiumExchangeLoggingIntegrationTest reads the reference mode as it is).
            //
            // Given / When / Then
            assertThat(adapterLogging).containsEntry("log-request-body", "on-failure")
            assertThat(adapterLogging).containsEntry("log-response-body", "on-failure")
            assertThat(adapterLogging).containsEntry("max-body-bytes", "16384")
        }

        private fun unmaskedRequestHeaders(): List<String> = adapterLogging.filterKeys { it.startsWith("request-headers.unmasked.") }.values.toList()
    }

    private companion object {
        const val LIBRARY_LOGGER = "eu.inqudium.legatium.restclient.logging"
        const val MODULE_LOGGER = "eu.inqudium.veredium"
    }
}
