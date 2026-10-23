package eu.inqudium.veredium.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.assertj.core.api.Assertions.entry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.slf4j.MDC

/** [CallerMdc]: the caller's MDC on a thread the caller never ran on, and the thread's own MDC restored afterwards. */
class CallerMdcTest {
    @AfterEach
    fun clearMdc() {
        MDC.clear()
    }

    @Test
    fun `should install the snapshot for the block and restore the thread's own MDC afterwards`() {
        // What is tested: CallerMdc.with with a snapshot, on a thread that has an MDC of its own.
        // Success criteria: inside the block the MDC is exactly the snapshot (the thread's own key hidden);
        //   the block's value is returned; afterwards the thread's own MDC is back, without the snapshot's key.
        // Why it matters: the reactive stack writes its error and cancel lines through this on Netty event
        //   loops, the blocking decorator runs every call through it on a worker; a snapshot left behind would
        //   stamp the next request's lines on that thread with this request's correlation id.
        //
        // Given
        MDC.put("THREAD_OWN", "event-loop")

        // When
        val inside = CallerMdc.with(mapOf("CORRELATION_ID" to "Out:1")) { MDC.getCopyOfContextMap().orEmpty() }

        // Then
        assertThat(inside).containsExactly(entry("CORRELATION_ID", "Out:1"))
        assertThat(MDC.getCopyOfContextMap()).containsExactly(entry("THREAD_OWN", "event-loop"))
    }

    @Test
    fun `should clear the MDC for a null snapshot and restore it even when the block throws`() {
        // What is tested: the null snapshot (the caller had no MDC) and the failure path of the restoration.
        // Success criteria: a null snapshot runs the block on an EMPTY MDC; a block that throws propagates
        //   its exception and still leaves the thread's own MDC in place; a thread without an MDC is left
        //   without one.
        // Why it matters: a line must never borrow another request's keys from the thread it happens to run
        //   on, and an exception inside a log call must not be the way a snapshot leaks.
        //
        // Given
        MDC.put("THREAD_OWN", "event-loop")
        val failure = IllegalStateException("inside")

        // When
        val insideNull = CallerMdc.with(null) { MDC.getCopyOfContextMap().orEmpty() }
        val thrown = catchThrowable { CallerMdc.with(mapOf("CORRELATION_ID" to "Out:2")) { throw failure } }
        val afterThrow = MDC.getCopyOfContextMap()
        MDC.clear()
        CallerMdc.with(mapOf("CORRELATION_ID" to "Out:3")) { }
        val withoutOwn = MDC.getCopyOfContextMap().orEmpty()

        // Then
        assertThat(insideNull).isEmpty()
        assertThat(thrown).isSameAs(failure)
        assertThat(afterThrow).containsExactly(entry("THREAD_OWN", "event-loop"))
        assertThat(withoutOwn).isEmpty()
    }
}
