package eu.inqudium.veredium.core

import java.time.Duration

/**
 * The two effective per-call timeouts an adapter exposes to its callers, so upstream orchestration can
 * compose an end-to-end budget from the individual backend budgets (e.g. a resolver summing the timeouts
 * along its call chain) without reaching into the adapter's clients.
 *
 * A contract for the HOST's adapters, not for this library: veredium implements it nowhere and reads it nowhere.
 * Each client already carries the two values (`responseTimeout`, `timeLimiterTimeout`); an adapter implements
 * this interface to publish them under one type its callers can depend on, whichever stack it runs on: an
 * adapter over `veredium-restclient`'s `ResilientRestClient` and one over `veredium-webclient`'s
 * `ResilientWebClient` publish the same two numbers, derived from the same mesh ladder, so a host that runs
 * both - or switches - composes its budgets from one type.
 *
 * If an adapter fronts more than one mesh endpoint (one client per operation), then an implementation can
 * aggregate the per-client values - typically the `maxOf` across its clients, the pessimistic ceiling for a
 * single logical call.
 *
 * The two values are layered, not alternatives, and the ladder fixes their order
 * ([eu.inqudium.veredium.core.mesh.ResolvedTimeouts]: `timeLimiter = responseTimeout + transport margin`). The
 * TimeLimiter is the outer, hard deadline after which the CALLER is released - a blocking caller with a
 * `TimeoutException`, a reactive chain by cancellation. The transport's response timeout is the tighter bound
 * underneath it on the response phase alone, so under normal conditions the transport is what trips, and the
 * TimeLimiter covers the phases before the response (connect, pending-acquire) that the response timeout does
 * not. See the ordering in [eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator] and in the
 * reactive stack's `ReactiveResilienceDecorator`.
 */
interface BaseAdapter {
    /**
     * The Resilience4j TimeLimiter budget for the call - the outer, hard deadline enforced by the TimeLimiter,
     * after which the caller is released.
     */
    val timeLimiterTimeout: Duration

    /**
     * The transport's response timeout - the bound on waiting for the HTTP response, sitting just below
     * [timeLimiterTimeout]. The ladder's `responseTimeout` rung: Apache's `RequestConfig.responseTimeout` on
     * the blocking stack, Reactor Netty's `responseTimeout` on the reactive one, the same number on both.
     */
    val responseTimeout: Duration
}
