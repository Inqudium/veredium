package eu.inqudium.veredium.testsupport

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.github.tomakehurst.wiremock.http.ImmutableRequest
import com.github.tomakehurst.wiremock.http.RequestMethod
import com.github.tomakehurst.wiremock.http.Response
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.LockSupport

/** The helpers the client suites rely on, held to the promises those suites make with them. */
class TestSupportTest {
    @Test
    fun `should report a condition another thread makes true, and give up on one that never holds`() {
        // What is tested: eventually, against a flag set from another thread after 50 ms and a condition that
        //   never holds.
        // Success criteria: the first returns true; the second returns false after its 100 ms bound, not later
        //   than 2 s.
        // Why it matters: the suites wait with it instead of sleeping; a poller that missed a late state would
        //   make them flaky, one that never gave up would hang the build.
        //
        // Given
        val flag = AtomicBoolean(false)
        Thread.ofVirtual().start {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50))
            flag.set(true)
        }

        // When
        val reached = eventually { flag.get() }
        val started = System.nanoTime()
        val never = eventually(Duration.ofMillis(100)) { false }
        val waited = Duration.ofNanos(System.nanoTime() - started)

        // Then
        assertThat(reached).isTrue()
        assertThat(never).isFalse()
        assertThat(waited).isGreaterThanOrEqualTo(Duration.ofMillis(100)).isLessThan(Duration.ofSeconds(2))
    }

    @Test
    fun `should walk a cause chain outermost first and stop at a cause that is its own`() {
        // What is tested: causeChain and causeNames over a two-link chain, and over a throwable whose cause is
        //   itself.
        // Success criteria: the chain lists the outer and the inner throwable in that order, the names are their
        //   simple class names; the self-caused throwable yields a chain of one.
        // Why it matters: the suites assert an engine's exception by its place in the chain; a walk that looped
        //   on a self-cause would never end.
        //
        // Given
        val inner = IOException("inner")
        val outer = IllegalStateException("outer", inner)
        val selfCaused =
            object : RuntimeException("self") {
                override val cause: Throwable get() = this
            }

        // When / Then
        assertThat(causeChain(outer)).containsExactly(outer, inner)
        assertThat(causeNames(outer)).containsExactly("IllegalStateException", "IOException")
        assertThat(causeChain(selfCaused)).hasSize(1)
        assertThat(causeChain(null)).isEmpty()
    }

    @Test
    fun `should capture only the accepted events and wait for the ones it is told to`() {
        // What is tested: CapturingAppender with a filter on the message, fed two events, one of them accepted.
        // Success criteria: events holds only the accepted one; awaitEvents for it returns at once with it.
        // Why it matters: the suites capture one call's lines out of a logger every test writes to; a filter that
        //   let others through, or a wait that did not see the line, would make their assertions lie.
        //
        // Given
        val logger = LoggerFactory.getLogger("capture-probe") as Logger
        logger.level = Level.INFO
        val appender = CapturingAppender { it.formattedMessage.contains("wanted") }.apply { start() }
        logger.addAppender(appender)
        try {
            // When
            logger.info("a wanted line")
            logger.info("another line")

            // Then
            assertThat(appender.events.map { it.formattedMessage }).containsExactly("a wanted line")
            assertThat(appender.awaitEvents(until = { it.formattedMessage.startsWith("a wanted") })).hasSize(1)
        } finally {
            logger.detachAppender(appender)
        }
    }

    @Test
    fun `should open the latch of a URL only for that URL, and forget it once cleared`() {
        // What is tested: WireMockArrivals, through its listener method, for the registered URL, another URL, and
        //   the registered URL after clear.
        // Success criteria: the latch opens for its own URL only; after clear, a new latch for the same URL stays
        //   closed when the action of the earlier test is gone.
        // Why it matters: it replaces the per-test listeners that piled up across tests and fired for a later
        //   test's request to the same URL.
        //
        // Given
        val arrivals = WireMockArrivals()
        val held = arrivals.arrivalOf("/held")

        // When
        arrivals.requestReceived(request("/other"), RESPONSE)
        val beforeOwn = held.count
        arrivals.requestReceived(request("/held"), RESPONSE)
        arrivals.clear()
        val later = arrivals.arrivalOf("/later")
        arrivals.requestReceived(request("/held"), RESPONSE)

        // Then
        assertThat(beforeOwn).isEqualTo(1)
        assertThat(held.await(0, TimeUnit.SECONDS)).isTrue()
        assertThat(later.count).isEqualTo(1)
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private fun request(url: String) =
        ImmutableRequest
            .create()
            .withAbsoluteUrl("http://localhost$url")
            .withMethod(RequestMethod.GET)
            .build()

    private companion object {
        val RESPONSE: Response = Response.notConfigured()
    }
}
