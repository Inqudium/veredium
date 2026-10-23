package eu.inqudium.veredium.core.engine

import org.slf4j.Logger
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

/**
 * The draining shutdown both client stacks give their HTTP engines: wait, bounded by a deadline SHARED by every engine
 * of a factory, for the exchanges in flight to finish, then report an engine that did not drain - an outcome, never an
 * exception, since this runs from a destroy callback.
 */
object EngineDrain {
    /** How often a draining shutdown re-checks the exchanges in flight. */
    val POLL_NANOS: Long = TimeUnit.MILLISECONDS.toNanos(50)

    /**
     * Waits until [inFlight] reports zero, [deadlineNanos] (on [nanoTime]'s clock) passes or the calling thread is
     * interrupted, and returns what is still in flight then. The deadline is compared as `nanoTime() - deadline`, the
     * overflow-safe form `System.nanoTime` prescribes.
     *
     * The wait also ends when the calling thread is INTERRUPTED - a container tearing the context down under a deadline
     * of its own: `parkNanos` returns at once on a thread whose interrupt flag is set and leaves the flag set, so without
     * this exit the loop would spin hot until the deadline instead of waiting. The flag is left for the caller.
     */
    fun await(
        inFlight: () -> Int,
        deadlineNanos: Long,
        nanoTime: () -> Long,
    ): Int {
        while (inFlight() > 0 && nanoTime() - deadlineNanos < 0 && !Thread.currentThread().isInterrupted) {
            LockSupport.parkNanos(POLL_NANOS)
        }
        return inFlight()
    }

    /** The time left until [deadlineNanos], never negative. */
    fun remaining(
        deadlineNanos: Long,
        nanoTime: () -> Long,
    ): Duration = Duration.ofNanos(maxOf(0L, deadlineNanos - nanoTime()))

    /**
     * The one WARN line of an engine that did not drain: [engine] by its key, why the wait ended - the interrupt when the
     * calling thread's flag is set, [deadline] otherwise (`within <window>`, `by the factory's shutdown deadline`) - and
     * [left], the exchanges still in flight, or `null` for an engine that does not count them (the JDK client).
     */
    fun reportUndrained(
        logger: Logger,
        engine: String,
        deadline: String,
        left: Int?,
    ) {
        logger.warn(
            "HTTP engine '{}' did not drain {} - closing it with {} still in flight",
            engine,
            if (Thread.currentThread().isInterrupted) "because the shutdown thread was interrupted" else deadline,
            left?.let { "$it exchange(s)" } ?: "the exchanges",
        )
    }
}
