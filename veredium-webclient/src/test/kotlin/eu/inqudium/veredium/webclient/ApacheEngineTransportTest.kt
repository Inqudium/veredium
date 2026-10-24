package eu.inqudium.veredium.webclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.testsupport.capturing
import eu.inqudium.veredium.testsupport.eventually
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.channels.ClosedSelectorException
import java.time.Duration

/**
 * The I/O reactor exception callback of the Apache engine ([ApacheEngineTransport.onReactorException]), called directly -
 * the exception it exists for, a worker meeting its selector closed under it, arrives during a shutdown only on a
 * slow machine, so no integration case reaches it reliably.
 *
 * The transport is a real one, with live I/O workers that report to the same logger, so the lines are captured with
 * the thread-safe appender - and only once the workers of a released transport have ended, so a late report of one
 * of them cannot land among the lines a case asserts on.
 */
class ApacheEngineTransportTest {
    private val logger = LoggerFactory.getLogger(ApacheEngineTransportTest::class.java) as Logger

    private val transport = ApacheEngineTransport(WebClientProperties(), MeshProperties.Transport(), Duration.ZERO, logger)

    @AfterEach
    fun release() {
        transport.shutdown(System.nanoTime(), System::nanoTime)
    }

    @Test
    fun `should log a reactor exception at ERROR outside a release, a closed selector included`() {
        // What is tested: the callback before any release, for the closed-selector exception and for another one.
        // Success criteria: both are written at ERROR, each with its exception.
        // Why it matters: outside a release a closed selector is a genuine fault of the reactor; taking it to DEBUG
        //   everywhere would hide the one failure Apache's own callback exists to report.
        //
        // Given / When
        capturing(logger, Level.DEBUG) { lines ->
            transport.onReactorException(ClosedSelectorException())
            transport.onReactorException(IOException("reactor broke"))

            // Then
            assertThat(lines.events.map { it.level }).containsExactly(Level.ERROR, Level.ERROR)
            assertThat(lines.events.map { it.throwableProxy.className })
                .containsExactly(ClosedSelectorException::class.java.name, IOException::class.java.name)
        }
    }

    @Test
    fun `should take a closed selector to DEBUG during the shutdown and keep every other exception at ERROR`() {
        // What is tested: the callback once shutdown() has started - here, after it returned and the reactor's own
        //   workers ended, as a worker still shutting down would report it.
        // Success criteria: the closed selector is one DEBUG line without a stack trace; another exception stays an
        //   ERROR with its exception.
        // Why it matters: the hard close at the end of a release regularly meets a worker that has not ended yet on a
        //   slow machine (the CI runner), and Apache's default callback logged that expected end at ERROR - noise at
        //   the moment someone reads the shutdown log. Anything else must stay loud.
        //
        // Given: a released transport whose I/O workers have ended, so none of them can still report
        transport.shutdown(System.nanoTime(), System::nanoTime)
        assertThat(eventually { Thread.getAllStackTraces().keys.none { it.name.startsWith(ApacheEngineTransport.THREAD_NAME_PREFIX) } })
            .`as`("the Apache engine's I/O threads must end")
            .isTrue()
        capturing(logger, Level.DEBUG) { lines ->
            // When
            transport.onReactorException(ClosedSelectorException())
            transport.onReactorException(IOException("reactor broke"))

            // Then
            assertThat(lines.events.map { it.level }).containsExactly(Level.DEBUG, Level.ERROR)
            assertThat(lines.events.first().throwableProxy).isNull()
            assertThat(
                lines.events
                    .last()
                    .throwableProxy.className,
            ).isEqualTo(IOException::class.java.name)
        }
    }
}
