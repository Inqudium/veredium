package eu.inqudium.veredium.webclient

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.mock.env.MockEnvironment

/**
 * Pins what this module adds to the core's `HttpObservationReport` (whose four verdicts and the central
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
        //   logging configuration and the runbook look for it.
        //
        // Given: the report's logger capturing
        val logger = LoggerFactory.getLogger(WebClientObservationReport::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        try {
            // When
            WebClientObservationReport(
                WebClientProperties(observability = WebClientProperties.Observability(httpObservationEnabled = true)),
                environment = MockEnvironment(),
            )

            // Then
            val toggleLine = appender.list.map { it.formattedMessage }.single { it.contains("HTTP observation module toggle") }
            assertThat(toggleLine).startsWith("WebClient HTTP observation module toggle ${WebClientObservationReport.TOGGLE_PROPERTY}=true")
            assertThat(appender.list).hasSize(2)
        } finally {
            logger.detachAppender(appender)
        }
    }
}
