package eu.inqudium.veredium.webclient

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
class WebClientObservationReportTest {
    @Test
    fun `should write the toggle line under this module's name, key and logger`() {
        // What is tested: this module's half of the startup report - the stack name and property key on the
        //   toggle line, under this class's logger.
        // Success criteria: the toggle line starts with "WebClient HTTP observation module toggle" followed by
        //   the module's property key and its value, and the report writes exactly its two lines.
        // Why it matters: an operator who finds no client span searches the startup log for that key; a line
        //   naming another stack's key, or written under the core's logger name, would not be found where the
        //   logging configuration and an operator's log query look for it.
        //
        // Given: the report's logger capturing
        val logger = LoggerFactory.getLogger(WebClientObservationReport::class.java) as Logger
        capturing(logger) { appender ->
            // When
            WebClientObservationReport(
                WebClientProperties(observability = WebClientProperties.Observability(httpObservationEnabled = true)),
                environment = MockEnvironment(),
            )

            // Then
            val toggleLine = appender.events.map { it.formattedMessage }.single { it.contains("HTTP observation module toggle") }
            assertThat(toggleLine).startsWith("WebClient HTTP observation module toggle ${WebClientObservationReport.TOGGLE_PROPERTY}=true")
            assertThat(appender.events).hasSize(2)
        }
    }
}
