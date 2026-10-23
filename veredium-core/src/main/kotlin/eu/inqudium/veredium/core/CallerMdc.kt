package eu.inqudium.veredium.core

import org.slf4j.MDC

/**
 * Carries the caller's MDC onto a thread the caller never ran on - the one hand-off both client stacks need: the
 * blocking decorator's virtual worker, and the Netty event loop the reactive client's signal hooks fire on.
 */
object CallerMdc {
    /**
     * Runs [block] with [snapshot] installed as the MDC, and restores the thread's own MDC afterwards.
     *
     * The caller takes [snapshot] on its own thread (`MDC.getCopyOfContextMap()`), where a coroutine's `MDCContext`
     * or a servlet filter put its keys, so the lines written in [block] carry the caller's correlation keys whatever
     * the host configured for the other thread. A `null` snapshot (the caller had no MDC) clears the MDC for the
     * block, so a line never borrows another request's keys from the thread it happens to run on.
     *
     * Restoration is unconditional and in `finally`: a thread that runs this twice must not carry one caller's keys
     * into the next call, whether or not it is ever reused.
     */
    fun <R> with(
        snapshot: Map<String, String>?,
        block: () -> R,
    ): R {
        val previous: Map<String, String>? = MDC.getCopyOfContextMap()
        if (snapshot != null) MDC.setContextMap(snapshot) else MDC.clear()
        try {
            return block()
        } finally {
            if (previous != null) MDC.setContextMap(previous) else MDC.clear()
        }
    }
}
