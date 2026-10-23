package eu.inqudium.veredium.core

import java.time.Duration

/**
 * The two effective per-call timeouts an adapter exposes to its callers, so upstream orchestration can
 * compose an end-to-end budget from the individual backend budgets (e.g. a resolver summing the timeouts
 * along its call chain) without reaching into the adapter's clients. One contract for both stacks: an adapter
 * over `veredium-restclient`'s `ResilientRestClient` and one over `veredium-webclient`'s `ResilientWebClient`
 * publish the same two numbers, derived from the same mesh ladder, so a host that runs both - or switches -
 * composes its budgets from one type.
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
     *
     * ## What an adapter call can fail with, beyond the documented non-2xx
     *
     * A 200 whose body does not fit the declared DTO fails the call rather than yielding a partially populated
     * object. A property declared required and non-nullable that is missing is rejected by Jackson; one that is
     * explicitly `null` is rejected by the null check the Kotlin compiler puts in the constructor. Both surface
     * as a `tools.jackson.databind.DatabindException` naming the property. This matters to a caller because
     * Kotlin's types promise the field cannot be null - nothing downstream guards against it, so a silent null
     * would only fail much later.
     *
     * Such a failure is CLIENT-side: the backend answered, and with a 200. Like a parse error it is therefore
     * kept out of the Resilience4j decoration on both stacks and does not count towards the circuit breaker.
     * It is logged with full severity; the exchange line legatium writes for the call shows the 200 the backend
     * answered with, because the exchange itself succeeded.
     */
    val timeLimiterTimeout: Duration

    /**
     * The transport's response timeout - the bound on waiting for the HTTP response, sitting just below
     * [timeLimiterTimeout]. The ladder's `responseTimeout` rung: Apache's `RequestConfig.responseTimeout` on
     * the blocking stack, Reactor Netty's `responseTimeout` on the reactive one, the same number on both.
     */
    val responseTimeout: Duration
}
