package eu.inqudium.veredium.webclient

import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator
import io.github.resilience4j.reactor.timelimiter.TimeLimiterOperator
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.springframework.stereotype.Component
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
 * The nesting is not a local preference: it is exactly the composition Resilience4j's Spring integration
 * applies to its annotations, minus the two primitives this pipeline does not use. The aspects declare a
 * default order (Spring `Ordered`, so a LOWER value binds FURTHER OUT), which yields
 * `Retry( CircuitBreaker( RateLimiter( TimeLimiter( Bulkhead( call ) ) ) ) )`:
 *
 * | Primitive      | Default aspect order      | Position | Used here |
 * |----------------|---------------------------|----------|-----------|
 * | Retry          | `LOWEST_PRECEDENCE - 5`   | outermost| no        |
 * | CircuitBreaker | `LOWEST_PRECEDENCE - 4`   |          | yes       |
 * | RateLimiter    | `LOWEST_PRECEDENCE - 3`   |          | no        |
 * | TimeLimiter    | `LOWEST_PRECEDENCE - 2`   |          | yes       |
 * | Bulkhead       | `LOWEST_PRECEDENCE - 1`   | innermost| yes       |
 *
 * (Read off the `*ConfigurationProperties.get*AspectOrder()` defaults in `resilience4j-spring6`, the version
 * this module resolves. Should a future upgrade change them, this table - not the reasoning above - is what
 * needs rechecking.)
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
 * backend failure. Nothing in Resilience4j's defaults prevents that; it takes an explicit ignore predicate,
 * which the per-alias `CircuitBreakerConfigCustomizer` of veredium-core (`MeshResilienceCustomizers.kt`, fed
 * with this stack's [WebClientVerdicts]) installs while Resilience4j builds the instance from
 * `resilience4j.circuitbreaker.instances.<alias>` - for every MESH ALIAS, and only those. Without that
 * customizer the invariant does not hold, and the core's `MeshResilienceStartupGuard` fails the start when it
 * did not land.
 *
 * ## Three verdicts, not two
 *
 * That customizer sorts every error into the three answers Resilience4j actually has, the same split the
 * resolver breaker of this stack's original consumer uses (`answered-means-healthy`,
 * `veredium-restclient/docs/decisions.md` D2):
 *
 * | The call | Verdict | Source |
 * |---|---|---|
 * | never reached the backend (`BulkheadFullException`, a codec limit) | **ignored** - evidence in neither direction | [WebClientVerdicts.clientSideFailure] |
 * | reached it and got a benign 4xx | **success** - the backend answered, promptly and correctly | [WebClientVerdicts.httpExchangeError] |
 * | anything else - transport faults, 5xx, 408/429, the TimeLimiter abort | **failure** | the config defaults |
 *
 * The middle row is the one that is easy to get wrong in the safe-looking direction. Ignoring a benign 4xx
 * also keeps it out of the sliding window, so on a 4xx-dominated backend the window fills with whatever was
 * left over and a few genuine faults become its entire content - the failure mode that opened the resolver
 * breaker at a nominal 100 % on 2026-07-31. The customizer's KDoc in veredium-core carries the numbers.
 *
 * The scope limit is load-bearing, not incidental: one customizer exists per `meshProperties.resolveAll()`
 * alias, so a breaker constructed OUTSIDE this module never receives the invariant and has to carry its own
 * `ignoreExceptions` (`veredium-restclient/docs/decisions.md` D1). A component breaker that wraps a whole
 * operation and sees a `BulkheadFullException` raised anywhere beneath it is exactly such a case: extending
 * the list here does NOT extend it there; both move together.
 *
 * Inverting the nesting (bulkhead outermost) would make the rejection structurally invisible to the breaker
 * and is the obvious cheaper alternative - it was weighed and rejected: it departs from the order above, it
 * makes the bulkhead count calls the breaker short-circuits (so its limit bounds a different population and
 * its metric no longer means "in-flight at the backend"), and it would surface a `BulkheadFullException` to
 * callers where a `CallNotPermittedException` is the truthful answer. It would also not remove the need for
 * the record/ignore mechanism, since a client-side failure raised by the SOURCE - a codec limit, a parse
 * error - sits inside every operator regardless of their order.
 */
@Component
class ReactiveResilienceDecorator(
    private val circuitBreakerRegistry: CircuitBreakerRegistry,
    private val timeLimiterRegistry: TimeLimiterRegistry,
    private val bulkheadRegistry: BulkheadRegistry,
) {
    fun <T : Any> decorate(
        backendName: String,
        source: Mono<T>,
    ): Mono<T> {
        val circuitBreaker = circuitBreakerRegistry.circuitBreaker(backendName)
        val timeLimiter = timeLimiterRegistry.timeLimiter(backendName)
        // Only the semaphore Bulkhead is reactive-compatible; ThreadPoolBulkhead must not be used here.
        val bulkhead = bulkheadRegistry.bulkhead(backendName)

        return source
            .transformDeferred(BulkheadOperator.of(bulkhead))
            .transformDeferred(TimeLimiterOperator.of(timeLimiter))
            .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
    }

    /**
     * The configured TimeLimiter timeout for [backendName] - the end-to-end request budget. Exposed so the
     * transport can derive a *tighter* response-phase guard from it, rather than a second independent timeout
     * that could drift past the budget.
     */
    fun requestBudget(backendName: String): Duration = timeLimiterRegistry.timeLimiter(backendName).timeLimiterConfig.timeoutDuration
}
