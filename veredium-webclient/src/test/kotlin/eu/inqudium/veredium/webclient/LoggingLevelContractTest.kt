package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.testsupport.ConfigurationFiles
import eu.inqudium.veredium.testsupport.ConfigurationFiles.FIXTURE
import eu.inqudium.veredium.testsupport.ConfigurationFiles.REFERENCE
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * Contract test for the logger identities of the configuration, pinned in BOTH the module's reference
 * configuration ([REFERENCE], the documented example a host copies from; a library ships no `application.yml`) and
 * the test fixture ([FIXTURE]) - the rule of [ConfigurationFiles] for an identity, which nobody tunes by hand.
 *
 * It pins that the `logging.level` entry for the exchange stream legatium-webclient-logging writes addresses the
 * SAME logger `adapter-logging.logger-name` makes the library write on, at INFO or below; that the library's own
 * logger stays audible; that this module's own classes are addressed under their real package
 * (`eu.inqudium.veredium`), with no key for the origin project's package and none for a module `HttpExchange` logger
 * nothing writes on; that root is at INFO; and that the library is switched on by an explicit entry. The logger
 * names are literals in a YAML file and in the library, and a typo in either leaves the level addressing nothing,
 * silently - in the reference, which is what hosts copy, as much as in the fixture.
 *
 * The `adapter-logging` VALUES - which headers are plaintext, the body modes, the capture cap - are not pinned here:
 * the contexts of `LegatiumExchangeLoggingIntegrationTest` load the reference block and pin what those values DO on a
 * real exchange (the Host header readable, the token a fingerprint, bodies as the reference logs them, the cap), and
 * the fixture carries no values of its own that nothing loads.
 */
class LoggingLevelContractTest {
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = [REFERENCE, FIXTURE])
    fun `should address the exchange stream's level at the logger the library writes on`(file: String) {
        // What is tested: the pair "level key" / "adapter-logging.logger-name" inside one file.
        // Success criteria: the logger name is legatium's default `adapter-http-exchange`, and logging.level carries
        //   it as a key at INFO, DEBUG or TRACE.
        // Why it matters: the two literals are related by nothing but this test; a mismatch makes the level address
        //   a logger nobody writes on, and the stream silently follows root instead. A higher level here FILTERS
        //   (WARN keeps failures, timeouts, cancellations and slow calls) rather than switching the feature off - so
        //   this pins the operational choice of seeing every exchange.
        //
        // Given: the logger the adapter-logging block makes the library write on
        val loggerName = ConfigurationFiles.bindMap(file, "adapter-logging").getValue("logger-name")
        assertThat(loggerName).`as`("adapter-logging.logger-name in $file").isEqualTo(EXCHANGE_LOGGER)

        // When: the logging.level block of the same file
        val levels = ConfigurationFiles.bindMap(file, "logging.level")

        // Then
        assertThat(levels)
            .`as`("logging.level in $file must address the exchange logger under the name adapter-logging.logger-name declares")
            .containsKey(loggerName)
        assertThat(levels.getValue(loggerName)).isIn("INFO", "DEBUG", "TRACE")
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = [REFERENCE, FIXTURE])
    fun `should keep the library's own logger audible for its breadcrumb and fail-open lines`(file: String) {
        // What is tested: the level of legatium's own logger.
        // Success criteria: `eu.inqudium.legatium.webclient.logging` is present at WARN or lower.
        // Why it matters: silencing it would hide the immediate WARN breadcrumb when a call throws, and the
        //   lines that say the logging itself failed open - the one place a broken exchange log reports itself.
        //
        // Given / When
        val levels = ConfigurationFiles.bindMap(file, "logging.level")

        // Then
        assertThat(levels).containsKey(LIBRARY_LOGGER)
        assertThat(levels.getValue(LIBRARY_LOGGER)).isIn("WARN", "INFO", "DEBUG", "TRACE")
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = [REFERENCE, FIXTURE])
    fun `should keep the root level free of framework debug noise`(file: String) {
        // What is tested: the root level.
        // Success criteria: root is INFO.
        // Why it matters: root at DEBUG turns on full framework logging - Netty, Reactor, Spring - for every host
        //   that copies the file, and drowns the lines an operator looks for.
        //
        // Given / When / Then
        assertThat(ConfigurationFiles.bindMap(file, "logging.level")["root"]).isEqualTo("INFO")
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = [REFERENCE, FIXTURE])
    fun `should address this library's own loggers under their real package`(file: String) {
        // What is tested: the "this module's own classes stay verbose" entry of the logging.level block.
        // Success criteria: `eu.inqudium.veredium` is set to DEBUG, and no entry addresses `com.ing`, the
        //   package of the project this code was ported from.
        // Why it matters: a level for a package nothing lives in is silently a no-op - a comment promising
        //   verbose library classes above an entry that addresses another package changes nothing, and a host
        //   copying the reference copies a level that addresses nothing.
        //
        // Given / When
        val levels = ConfigurationFiles.bindMap(file, "logging.level")

        // Then
        assertThat(levels).containsEntry(MODULE_LOGGER, "DEBUG")
        assertThat(levels.keys).noneMatch { it.startsWith("com.ing") }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = [REFERENCE, FIXTURE])
    fun `should not address the module's own HttpExchange logger name`(file: String) {
        // What is tested: that the file carries no level for `eu.inqudium.veredium.webclient.HttpExchange`.
        // Success criteria: the name is absent from logging.level.
        // Why it matters: nothing writes to that name - the per-exchange event is legatium's, on
        //   `adapter-http-exchange`; an entry for it would be a level pointing at nothing, and would suggest
        //   to a reader that the module writes an exchange event of its own.
        //
        // Given / When / Then
        assertThat(ConfigurationFiles.bindMap(file, "logging.level")).doesNotContainKey("eu.inqudium.veredium.webclient.HttpExchange")
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = [REFERENCE, FIXTURE])
    fun `should keep the library switched on by an explicit entry`(file: String) {
        // What is tested: `adapter-logging.enabled`.
        // Success criteria: the key is present and `true`.
        // Why it matters: explicit although it is the default - it is a context-start switch that removes
        //   the filter and its customizer, so its presence in the file is what makes an intentional `false`
        //   reviewable.
        //
        // Given / When / Then
        assertThat(ConfigurationFiles.bindMap(file, "adapter-logging")).containsEntry("enabled", "true")
    }

    private companion object {
        /** legatium's default exchange logger; the module keeps the default so the library's docs apply. */
        const val EXCHANGE_LOGGER = "adapter-http-exchange"
        const val LIBRARY_LOGGER = "eu.inqudium.legatium.webclient.logging"
        const val MODULE_LOGGER = "eu.inqudium.veredium"
    }
}
