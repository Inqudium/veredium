package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.webclient.ClientSideFailurePolicy.isClientSideFailure
import eu.inqudium.veredium.webclient.ClientSideFailurePolicy.logPolicy
import eu.inqudium.veredium.webclient.mesh.MeshProperties
import eu.inqudium.veredium.webclient.mesh.ResolvedTimeouts
import eu.inqudium.veredium.webclient.resilience.AmendmentPlan
import eu.inqudium.veredium.webclient.resilience.ResilienceAmendmentApplier
import eu.inqudium.veredium.webclient.resilience.ResilienceAmendmentContributor
import eu.inqudium.veredium.webclient.resilience.ignoreAlso
import eu.inqudium.veredium.webclient.resilience.recordUnless
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator
import io.github.resilience4j.reactor.timelimiter.TimeLimiterOperator
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
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
 * backend failure. Nothing in Resilience4j's defaults prevents that; it takes an explicit `ignoreExceptions`,
 * which [ClientSideFailureIgnoreRegistrar] applies at startup to every MESH ALIAS - and only those. Without
 * that registrar the invariant does not hold.
 *
 * ## Three verdicts, not two
 *
 * The two registrars in this file sort every error into the three answers Resilience4j actually has, the same
 * split the sync-bridge resolver breaker uses (ADR-032, `answered-means-healthy`):
 *
 * | The call | Verdict | Applied by |
 * |---|---|---|
 * | never reached the backend (`BulkheadFullException`, a codec limit, a parse error) | **ignored** - evidence in neither direction | [ClientSideFailureIgnoreRegistrar] |
 * | reached it and got a benign 4xx | **success** - the backend answered, promptly and correctly | [ExpectedClientErrorSuccessRegistrar] |
 * | anything else - transport faults, 5xx, 408/429, the TimeLimiter abort | **failure** | the config defaults |
 *
 * The middle row is the one that is easy to get wrong in the safe-looking direction. Ignoring a benign 4xx
 * also keeps it out of the sliding window, so on a 4xx-dominated backend the window fills with whatever was
 * left over and a few genuine faults become its entire content - the failure mode that opened the resolver
 * breaker at a nominal 100 % on 2026-07-31. See [ExpectedClientErrorSuccessRegistrar].
 *
 * The scope limit is load-bearing, not incidental: the registrar iterates `meshProperties.resolveAll()`, so a
 * breaker constructed OUTSIDE this module never receives the invariant and has to carry its own
 * `ignoreExceptions`. The sync-bridge's `identity-resolver` component breaker
 * (`SyncBridgeResolverConfig.resolverCircuitBreaker`) is exactly such a case: it wraps a whole resolution, sees
 * a `BulkheadFullException` raised anywhere beneath it, and maintains its own list. Extending the list here
 * does NOT extend it there; both move together. See ADR-006 (D4).
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

/**
 * Enforces ONE rule on every mesh backend's CircuitBreaker: a failure this pipeline raises ITSELF must not be
 * charged to the backend.
 *
 * The breaker exists to answer "is the backend unhealthy?". It answers that by counting the errors it sees -
 * and `CircuitBreakerSubscriber.hookOnError` records EVERY error unconditionally, leaving the record/ignore
 * decision entirely to the config, whose default ignore list is empty. So out of the box the breaker charges
 * the backend for faults it never caused, and enough of them open the circuit while it is perfectly healthy:
 * a self-inflicted outage, and one that looks like a backend problem to whoever investigates it.
 *
 * [CLIENT_SIDE_FAILURES] is the list of those faults, and the single place to extend when another one turns
 * up. Membership has one criterion: **the backend is not at fault** - either it was never called, or it
 * answered and this process failed to handle the answer.
 *
 * This is registered in CODE rather than left to `resilience4j.circuitbreaker.*` in YAML because it is not a
 * tunable policy but a structural invariant of the pipeline above - the code that raises the exception is the
 * code that must discount it. A YAML entry would also be silently lost whenever an instance referenced a
 * different named config.
 *
 * Resilience4j's own `CircuitBreakerConfigCustomizer` is the mechanically cleaner tool, and now that
 * `application.yml` declares an instance per backend it would also fire (customizers reach only names that
 * exist as properties). It was weighed and rejected: nothing in this module boots the autoconfiguration, so a
 * customizer's effect cannot be observed here, whereas the tests for this class need no Spring context.
 * Revisit if that ever changes.
 *
 * Deliberately NOT gated on [ResolvedTimeouts.circuitBreakerEnabled]: that toggle governs whether mesh DERIVES
 * the slow-call knobs (see MeshTimeoutRegistrar) and defaults to off, whereas the breaker itself is in the call
 * path unconditionally. Gating this on the toggle would leave the invariant unmet in exactly the default
 * configuration.
 *
 * Order-independent with respect to MeshTimeoutRegistrar: both merely CONTRIBUTE to a plan that
 * [ResilienceAmendmentApplier] folds and applies once per instance, each amendment seeing the previous one's
 * result. Whichever is contributed first, both changes survive - pinned by `RegistryAmendmentsTest` and
 * `ResilienceAmendmentApplierTest` rather than asserted here.
 */
@Component
class ClientSideFailureIgnoreRegistrar(
    private val meshProperties: MeshProperties,
) : ResilienceAmendmentContributor {
    private val logger: Logger = LoggerFactory.getLogger(ClientSideFailureIgnoreRegistrar::class.java)

    override fun contribute(plan: AmendmentPlan) {
        val aliases =
            meshProperties
                .resolveAll()
                .values
                .map { it.alias }
                .sorted()
        aliases.forEach { alias -> plan.circuitBreaker(alias, ignoreAlso(::isClientSideFailure)) }
        logPolicy(logger.atInfo(), aliases.joinToString(", "))
    }
}

/**
 * Records a BENIGN 4xx response as a SUCCESS on a mesh backend's CircuitBreaker.
 *
 * A 4xx means the backend WAS called and answered CORRECTLY by rejecting the request - a malformed body, a
 * missing resource, a failed authorisation. That says nothing bad about the backend's health: a healthy
 * backend returns 4xx for bad requests all day. Counting it toward the failure rate would let a burst of
 * business cases (a caller sending bad input, a 404-heavy lookup) open the circuit against a backend that is
 * perfectly up - the same self-inflicted outage [ClientSideFailureIgnoreRegistrar] guards against, reached by
 * a different route.
 *
 * **Success, not ignore - and the difference is not cosmetic.** This registrar used to ignore the benign 4xx,
 * which keeps it out of the failure rate but also out of the sliding window entirely. The window then holds
 * only what the predicate did not filter, so on a 4xx-dominated backend a handful of genuine faults can BE
 * the whole sample. The sync-bridge resolver breaker has that measured rather than argued: on 2026-07-31,
 * 41,903 filtered-out benign 4xx left five 504 timeouts as the entire window and the breaker opened at a
 * nominal 100 % failure rate. It was rebuilt on `answered-means-healthy` (ADR-032,
 * `SyncBridgeResolverConfig.resolverCircuitBreaker`), and this contributor now follows the same rule: an
 * answer - even a rejection the backend is entitled to give - is positive evidence that it is alive and
 * serving, and belongs in the window as such. What stays IGNORED is the call that never reached the
 * backend, which is [ClientSideFailureIgnoreRegistrar]'s membership criterion and unchanged.
 *
 * The hazard this direction does NOT cover, stated because the ignore reading feared it: a backend answering
 * 4xx to EVERYTHING is a data or query defect, and it is invisible to a breaker built this way by design.
 * Catching that belongs to the consumer of the answers - for the migration path, the backfill's reject-rate
 * brake (backfill ADR-008 §3) - not to a health signal.
 *
 * The distinction is NOT reinvented here: it reuses [HttpExchangeErrorPolicy.isExpectedClientError], the single
 * source of truth that also drives log severity. A status the breaker treats as a success is therefore exactly
 * a status the logs treat as a business case, and the two cannot drift. In particular 408 and 429 are NOT
 * benign (a request timeout and a rate-limit rejection are operational signals): the shared predicate already
 * excludes them, so the breaker keeps recording them as failures.
 *
 * This is a SEPARATE contributor rather than an entry in [ClientSideFailureIgnoreRegistrar] on purpose, and
 * now for a stronger reason than before: the two no longer even reach the same verdict. That registrar's
 * criterion is "the call carries no evidence about the backend"; this one's is "the backend answered". They
 * are different questions with different answers, which is exactly what the [AmendmentPlan] framework
 * composes - see `recordUnless` and `ignoreAlso` in `RegistryAmendments` for the fold semantics that keep
 * them order-independent on the same instance.
 *
 * Scope, like [ClientSideFailureIgnoreRegistrar]: MESH ALIASES only. A breaker constructed outside this module
 * - e.g. the sync-bridge `resolverCircuitBreaker` - never receives this and must carry its own predicate if a
 * 4xx can surface through it. That one already does, which is where this rule came from.
 */
@Component
class ExpectedClientErrorSuccessRegistrar(
    private val meshProperties: MeshProperties,
) : ResilienceAmendmentContributor {
    private val logger: Logger = LoggerFactory.getLogger(ExpectedClientErrorSuccessRegistrar::class.java)

    override fun contribute(plan: AmendmentPlan) {
        val aliases =
            meshProperties
                .resolveAll()
                .values
                .map { it.alias }
                .sorted()
        aliases.forEach { alias -> plan.circuitBreaker(alias, recordUnless(HttpExchangeErrorPolicy::isExpectedClientError)) }
        HttpExchangeErrorPolicy.logPolicy(logger.atInfo(), aliases.joinToString(", "))
    }
}
