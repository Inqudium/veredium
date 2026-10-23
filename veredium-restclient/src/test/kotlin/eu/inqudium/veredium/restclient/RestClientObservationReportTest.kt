package eu.inqudium.veredium.restclient

import ch.qos.logback.classic.Logger
import eu.inqudium.veredium.testsupport.capturing
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.mock.env.MockEnvironment

/**
 * Pins what this module adds to the core's [eu.inqudium.veredium.core.observation.HttpObservationReport] (whose four verdicts and the central
 * disable-filter are tested there): the stack name and the property key on the toggle line, written under
 * this class's logger name - which is what a logging configuration and an operator's search key on.
 */
class RestClientObservationReportTest {
    @Test
    fun `should write the toggle line under this module's name, key and logger`() {
        // What is tested: this module's subclass of the core's HttpObservationReport - its stack name, its
        //   property key and its logger.
        // Success criteria: the report writes its two startup lines on RestClientObservationReport's logger, and
        //   the toggle line starts with `RestClient HTTP observation module toggle` and the module's key.
        // Why it matters: an operator searches the startup log by logger and by key; a line written under the
        //   core's logger or naming the webclient's key would point at the wrong module's configuration.
        //
        // Given: the report's logger capturing
        val logger = LoggerFactory.getLogger(RestClientObservationReport::class.java) as Logger
        capturing(logger) { appender ->
            // When
            RestClientObservationReport(
                RestClientProperties(observability = RestClientProperties.Observability(httpObservationEnabled = true)),
                environment = MockEnvironment(),
            )

            // Then
            val toggleLine = appender.events.map { it.formattedMessage }.single { it.contains("HTTP observation module toggle") }
            assertThat(toggleLine).startsWith("RestClient HTTP observation module toggle ${RestClientObservationReport.TOGGLE_PROPERTY}=true")
            assertThat(appender.events).hasSize(2)
        }
    }
}
