package eu.inqudium.veredium.core.engine

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * The drain both client stacks' engines share, on a clock the test steps itself: each read of the clock is one step
 * of the wait, so the deadline is reached after a known number of polls - real time passes only in the poll interval
 * between them, never as the thing under test.
 */
class EngineDrainTest {
    private val logger = LoggerFactory.getLogger(EngineDrainTest::class.java) as Logger
    private val lines = ListAppender<ILoggingEvent>().apply { start() }.also { logger.addAppender(it) }

    @AfterEach
    fun detach() {
        logger.detachAppender(lines)
        // A case that sets the interrupt flag must not leave it to the next one.
        Thread.interrupted()
    }

    @Nested
    inner class `The wait for the exchanges in flight` {
        @Test
        fun `should not wait at all when nothing is in flight`() {
            // What is tested: await() with no exchange in flight and a deadline far ahead.
            // Success criteria: it returns 0 without reading the clock.
            // Why it matters: an idle engine is the common case at shutdown; it must release at once, not after a poll.
            //
            // Given
            val clock = SteppingClock(start = 0L, step = 1L)

            // When
            val left = EngineDrain.await({ 0 }, deadlineNanos = 1_000L, nanoTime = clock)

            // Then
            assertThat(left).isZero()
            assertThat(clock.reads).isZero()
        }

        @Test
        fun `should wait until the deadline across the wrap of the nano clock, then return what is left`() {
            // What is tested: await() with one exchange in flight on a clock 10 ns below Long.MAX_VALUE that steps 60
            //   ns per read, and a deadline 100 ns ahead - past the wrap, so the deadline's value is negative.
            // Success criteria: it returns 1 after reading the clock three times - at +0 and +60 it waits, at +120 the
            //   deadline has passed.
            // Why it matters: System.nanoTime has an arbitrary origin and may wrap; only the difference
            //   `nanoTime() - deadline` is meaningful. A plain `nanoTime() < deadline` would see the deadline as long
            //   past and skip the drain - every in-flight exchange of that JVM would be cut off at shutdown.
            //
            // Given
            val start = Long.MAX_VALUE - 10
            val clock = SteppingClock(start, step = 60L)
            val deadline = start + 100

            // When
            val left = EngineDrain.await({ 1 }, deadline, clock)

            // Then
            assertThat(deadline).`as`("the deadline lies past the wrap").isNegative()
            assertThat(left).isEqualTo(1)
            assertThat(clock.reads).isEqualTo(3)
        }

        @Test
        fun `should stop waiting as soon as the last exchange finished`() {
            // What is tested: await() with an in-flight count that drops from 2 to 0 on its third reading, the deadline
            //   far ahead.
            // Success criteria: it returns 0 after reading the clock twice.
            // Why it matters: the deadline is an upper bound, not a wait; a drain that ran to it would hold the
            //   context's shutdown for the whole window on every stop.
            //
            // Given
            val clock = SteppingClock(start = 0L, step = 1L)
            val counts = ArrayDeque(listOf(2, 1, 0))

            // When
            val left = EngineDrain.await({ counts.removeFirstOrNull() ?: 0 }, deadlineNanos = 1_000_000L, nanoTime = clock)

            // Then
            assertThat(left).isZero()
            assertThat(clock.reads).isEqualTo(2)
        }

        @Test
        fun `should end the wait at once on an interrupted thread and leave the flag set`() {
            // What is tested: await() on a thread whose interrupt flag is set, one exchange in flight, the deadline far
            //   ahead.
            // Success criteria: it returns 1 after a single read of the clock; the flag is still set afterwards.
            // Why it matters: parkNanos returns at once on an interrupted thread, so without this exit the loop would
            //   spin hot until the deadline - a container stopping the context under its own deadline would get a
            //   busy CPU instead of a shutdown. The flag belongs to the caller.
            //
            // Given
            val clock = SteppingClock(start = 0L, step = 1L)
            Thread.currentThread().interrupt()

            // When
            val left = EngineDrain.await({ 1 }, deadlineNanos = 1_000_000L, nanoTime = clock)
            val stillInterrupted = Thread.currentThread().isInterrupted

            // Then
            assertThat(left).isEqualTo(1)
            assertThat(clock.reads).isEqualTo(1)
            assertThat(stillInterrupted).isTrue()
        }

        @Test
        fun `should report the time left until the deadline, and zero once it has passed`() {
            // What is tested: remaining() before the deadline, at it and past it.
            // Success criteria: 400 ns before it, exactly 400 ns; at it and 400 ns past it, Duration.ZERO.
            // Why it matters: an engine hands the remainder to a timeout of its own; a negative duration would be
            //   rejected by it, or read as "no timeout" - the opposite of a deadline already spent.
            //
            // Given
            val deadline = 1_000L

            // When
            val before = EngineDrain.remaining(deadline) { 600L }
            val at = EngineDrain.remaining(deadline) { 1_000L }
            val past = EngineDrain.remaining(deadline) { 1_400L }

            // Then
            assertThat(before).isEqualTo(Duration.ofNanos(400))
            assertThat(at).isEqualTo(Duration.ZERO)
            assertThat(past).isEqualTo(Duration.ZERO)
        }
    }

    @Nested
    inner class `The report of an engine that did not drain` {
        @Test
        fun `should name the window, or the interrupt, and the count in the line of an engine that did not drain`() {
            // What is tested: reportUndrained() with a count and without one (an engine that does not count), and with
            //   the calling thread's interrupt flag set.
            // Success criteria: three WARN lines - "within PT2S" and "3 exchange(s)"; "within PT2S" and "the
            //   exchanges"; "because the shutdown thread was interrupted" and "1 exchange(s)".
            // Why it matters: the line is the only trace of exchanges cut off by a shutdown. It has to say whether the
            //   window was too short or the container interrupted the wait - the two call for different fixes - and
            //   must not print "null" for an engine that has no count.
            //
            // Given
            val window = Duration.ofSeconds(2)

            // When
            EngineDrain.reportUndrained(logger, "apache", window, 3)
            EngineDrain.reportUndrained(logger, "jdk", window, null)
            Thread.currentThread().interrupt()
            EngineDrain.reportUndrained(logger, "jetty", window, 1)

            // Then
            assertThat(lines.list.map { it.level }).containsOnly(Level.WARN)
            assertThat(lines.list.map { it.formattedMessage }).containsExactly(
                "HTTP engine 'apache' did not drain within PT2S - closing it with 3 exchange(s) still in flight",
                "HTTP engine 'jdk' did not drain within PT2S - closing it with the exchanges still in flight",
                "HTTP engine 'jetty' did not drain because the shutdown thread was interrupted - closing it with 1 exchange(s) still in flight",
            )
        }

        @Test
        fun `should report only an engine that did not drain`() {
            // What is tested: drainOrReport() for an engine that drains and one that keeps an exchange past the
            //   deadline.
            // Success criteria: one WARN line, for the second engine, with its count; none for the first.
            // Why it matters: a line on every shutdown would teach operators to ignore the one that matters.
            //
            // Given
            val window = Duration.ofSeconds(1)

            // When
            EngineDrain.drainOrReport(logger, "drained", { 0 }, deadlineNanos = 100L, nanoTime = SteppingClock(0L, 60L), window = window)
            EngineDrain.drainOrReport(logger, "stuck", { 1 }, deadlineNanos = 100L, nanoTime = SteppingClock(0L, 60L), window = window)

            // Then
            assertThat(lines.list.map { it.formattedMessage }).containsExactly(
                "HTTP engine 'stuck' did not drain within PT1S - closing it with 1 exchange(s) still in flight",
            )
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** A nano clock that returns [start] on its first read and [step] more on every read after it, counting the reads. */
    private class SteppingClock(
        start: Long,
        private val step: Long,
    ) : () -> Long {
        private var next = start
        var reads = 0
            private set

        override fun invoke(): Long {
            reads++
            return next.also { next += step }
        }
    }
}
