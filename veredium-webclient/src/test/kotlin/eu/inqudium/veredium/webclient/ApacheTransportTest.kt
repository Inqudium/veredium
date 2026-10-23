package eu.inqudium.veredium.webclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import eu.inqudium.veredium.core.mesh.MeshProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.channels.ClosedSelectorException

/**
 * The I/O reactor exception callback of the Apache engine ([ApacheTransport.onReactorException]), called directly -
 * the exception it exists for, a worker meeting its selector closed under it, arrives during a release only on a
 * slow machine, so no integration case reaches it reliably.
 */
class ApacheTransportTest {
    private val logger = LoggerFactory.getLogger(ApacheTransportTest::class.java) as Logger
    private val lines = ListAppender<ILoggingEvent>().apply { start() }.also { logger.addAppender(it) }.also { logger.level = Level.DEBUG }
    private val transport = ApacheTransport(WebClientProperties(), MeshProperties.Transport(), logger)

    @AfterEach
    fun releaseAndDetach() {
        transport.release(System.nanoTime(), System::nanoTime)
        logger.detachAppender(lines)
        logger.level = null
    }

    @Test
    fun `should log a reactor exception at ERROR outside a release, a closed selector included`() {
        // What is tested: the callback before any release, for the closed-selector exception and for another one.
        // Success criteria: both are written at ERROR, each with its exception.
        // Why it matters: outside a release a closed selector is a genuine fault of the reactor; taking it to DEBUG
        //   everywhere would hide the one failure Apache's own callback exists to report.
        //
        // Given/When
        transport.onReactorException(ClosedSelectorException())
        transport.onReactorException(IOException("reactor broke"))

        // Then
        assertThat(lines.list.map { it.level }).containsExactly(Level.ERROR, Level.ERROR)
        assertThat(lines.list.map { it.throwableProxy.className })
            .containsExactly(ClosedSelectorException::class.java.name, IOException::class.java.name)
    }

    @Test
    fun `should take a closed selector to DEBUG during the release and keep every other exception at ERROR`() {
        // What is tested: the callback once release() has started - here, after it returned, as a worker still
        //   shutting down would report it.
        // Success criteria: the closed selector is one DEBUG line without a stack trace; another exception stays an
        //   ERROR with its exception.
        // Why it matters: the hard close at the end of a release regularly meets a worker that has not ended yet on a
        //   slow machine (the CI runner), and Apache's default callback logged that expected end at ERROR - noise at
        //   the moment someone reads the shutdown log. Anything else must stay loud.
        //
        // Given
        transport.release(System.nanoTime(), System::nanoTime)
        lines.list.clear()

        // When
        transport.onReactorException(ClosedSelectorException())
        transport.onReactorException(IOException("reactor broke"))

        // Then
        assertThat(lines.list.map { it.level }).containsExactly(Level.DEBUG, Level.ERROR)
        assertThat(lines.list.first().throwableProxy).isNull()
        assertThat(
            lines.list
                .last()
                .throwableProxy.className,
        ).isEqualTo(IOException::class.java.name)
    }
}
