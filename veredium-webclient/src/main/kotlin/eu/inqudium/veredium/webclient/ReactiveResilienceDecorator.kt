package eu.inqudium.veredium.webclient

import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator
import io.github.resilience4j.reactor.timelimiter.TimeLimiterOperator
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * Applies the three Resilience4j primitives as `CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )`, using
 * the registries auto-configured from `application.yml`. This order keeps the bulkhead measuring genuine
 * in-flight calls, lets the time limiter cancel slow calls, and lets the breaker record the resulting
 * `TimeoutException` as a failure.
 *
 * ## Why THIS order - it is Resilience4j's own
 *
 * The nesting is not a local preference. Assumption: it is exactly the composition Resilience4j's Spring
 * integration applies to its annotations, `Retry( CircuitBreaker( RateLimiter( TimeLimiter( Bulkhead( call ) ) ) ) )`,
 * minus the two primitives this pipeline does not use. Both decorators of this library follow the same default
 * aspect orders; [eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator] tabulates them with the
 * Resilience4j version they were read off, and a Resilience4j upgrade rechecks that table for both.
 *
 * Matching it matters because `resilience4j-spring-boot*` is on the classpath: anything annotated with
 * `@CircuitBreaker` / `@Bulkhead` elsewhere in the application composes in the order above. Were this
 * hand-rolled reactive chain to nest differently, the same three primitives would behave differently
 * depending on which path a call took - a difference nothing in the code would make visible.
 *
 * ## The consequence that is NOT self-enforcing
 *
 * The breaker sits OUTSIDE the bulkhead, so a `BulkheadFullException` - this pipeline's own load shedding,
 * not a backend fault - reaches the breaker's error hook like any other error and would be counted as a
 * backend failure. Assumption: nothing in Resilience4j's defaults prevents that (2.4.0: the breaker records every
 * exception, with an empty ignore list); it takes an explicit ignore predicate, which
 * [eu.inqudium.veredium.core.resilience.circuitBreakerCustomizer], fed with this stack's [WebClientVerdicts],
 * installs while Resilience4j builds the instance from `resilience4j.circuitbreaker.instances.<alias>` - for
 * every MESH ALIAS, and only those. Without that customizer the invariant does not hold, and the core's
 * [eu.inqudium.veredium.core.resilience.MeshResilienceStartupGuard] fails the start when it did not land.
 *
 * ## Three outcomes, not two
 *
 * That customizer sorts every error into the three outcomes Resilience4j has, by the two verdicts of
 * [WebClientVerdicts]: a call that never reached the backend ([WebClientVerdicts.clientSideFailure] - a
 * `BulkheadFullException`, a codec limit) is **ignored**, one that got a benign 4xx
 * ([WebClientVerdicts.httpExchangeError]) is a **success**, anything else - the TimeLimiter abort included - a
 * **failure**. The table, and why a benign 4xx is a success rather than ignored, are in the customizer's KDoc.
 *
 * The scope limit is load-bearing, not incidental: one customizer exists per `meshProperties.resolveAll()`
 * alias, so a breaker constructed OUTSIDE this module never receives the invariant and has to carry its own
 * `ignoreExceptions` (`veredium-restclient/docs/decisions.md` D1). A component breaker that wraps a whole
 * operation and sees a `BulkheadFullException` raised anywhere beneath it is exactly such a case: extending
 * the list here does NOT extend it there; both move together.
 *
 * Rationale: the nesting is not inverted (bulkhead outermost), although that would make the rejection
 * structurally invisible to the breaker and is the obvious cheaper alternative - it was weighed and rejected: it
 * departs from the order above, it makes the bulkhead count calls the breaker short-circuits (so its limit bounds
 * a different population and its metric no longer means "in-flight at the backend"), and it would surface a
 * `BulkheadFullException` to callers where a `CallNotPermittedException` is the truthful answer. It would also
 * not remove the need for the record/ignore mechanism, since a client-side failure raised by the SOURCE - a codec
 * limit, a parse error - sits inside every operator regardless of their order.
 *
 * ## Pinned, and one thing the order does NOT do
 *
 * `ReactiveResilienceDecoratorTest` pins the nesting by its consequences (a timeout counted by the breaker, an
 * open breaker that neither subscribes nor takes a permit, a full bulkhead's rejection reaching the breaker);
 * each of the five other orders turns one of them red. It also pins a property no order changes: a bulkhead
 * configured to WAIT for a permit waits inside `subscribe()`, on the subscribing thread, BEFORE the
 * TimeLimiter's timer starts. Assumption: Resilience4j's reactive bulkhead (2.4.0) acquires before it passes its
 * subscription on, and Reactor's `timeout` (reactor-core 3.8.7) starts counting only once it receives one. So a
 * waiting caller does not spend its budget, it parks a thread no timeout of the ladder bounds - which is why the
 * reference configuration keeps `max-wait-duration: 0`.
 */
class ReactiveResilienceDecorator(
    private val circuitBreakerRegistry: CircuitBreakerRegistry,
    private val timeLimiterRegistry: TimeLimiterRegistry,
    private val bulkheadRegistry: BulkheadRegistry,
) {
    /**
     * [source] guarded by the three instances registered under [backendName], nested as the class KDoc
     * orders them: bulkhead innermost, then TimeLimiter, then CircuitBreaker. Cold like [source]: every
     * subscription takes a permit, starts the budget and asks the breaker anew. The rejections arrive as error
     * signals - `TimeoutException` when the budget expired, `CallNotPermittedException` from an open breaker,
     * `BulkheadFullException` from a full bulkhead.
     */
    fun <T : Any> decorate(
        backendName: String,
        source: Mono<T>,
    ): Mono<T> {
        val circuitBreaker = circuitBreakerRegistry.circuitBreaker(backendName)
        val timeLimiter = timeLimiterRegistry.timeLimiter(backendName)
        // Rationale: the semaphore bulkhead, not a `ThreadPoolBulkhead` - only the semaphore one is reactive-compatible
        // (the `BulkheadRegistry` type already rules the other out).
        val bulkhead = bulkheadRegistry.bulkhead(backendName)

        return source
            .transformDeferred(BulkheadOperator.of(bulkhead))
            .transformDeferred(TimeLimiterOperator.of(timeLimiter))
            .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
    }

    /**
     * The configured TimeLimiter budget for [backendName] - the end-to-end request budget, published by adapters
     * and named on the call lines. Nothing derives a timeout from it: the transport's response timeout is its
     * own rung of the ladder, resolved below the budget by [eu.inqudium.veredium.core.mesh.ResolvedTimeouts].
     */
    fun requestBudget(backendName: String): Duration = timeLimiterRegistry.timeLimiter(backendName).timeLimiterConfig.timeoutDuration
}
