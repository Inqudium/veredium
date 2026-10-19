package eu.inqudium.veredium.webclient.resilience

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import org.springframework.beans.factory.SmartInitializingSingleton
import org.springframework.stereotype.Component

/**
 * The changes one startup component wants made to this module's Resilience4j instances, collected before any
 * of them is applied.
 *
 * A contributor states WHAT should change and for which instance; it does not touch a registry, does not
 * resolve a base config, and does not decide when its change happens relative to anyone else's. That
 * separation is the point: the two contributors here exist for unrelated reasons - one derives timeouts from
 * the mesh ladder, the other enforces that a failure this client causes itself is not charged to the backend -
 * and neither should have to know about the other to be correct.
 *
 * Several amendments may target the same instance. They are applied in contribution order, each seeing the
 * previous one's result (see [CircuitBreakerRegistry.amendAll]).
 */
class AmendmentPlan {
    private val circuitBreakers = LinkedHashMap<String, MutableList<CircuitBreakerAmendment>>()
    private val timeLimiters = LinkedHashMap<String, MutableList<TimeLimiterAmendment>>()

    /** Record [amendment] for the circuit breaker registered under [alias]. */
    fun circuitBreaker(
        alias: String,
        amendment: CircuitBreakerAmendment,
    ) {
        circuitBreakers.getOrPut(alias) { mutableListOf() } += amendment
    }

    /** Record [amendment] for the time limiter registered under [alias]. */
    fun timeLimiter(
        alias: String,
        amendment: TimeLimiterAmendment,
    ) {
        timeLimiters.getOrPut(alias) { mutableListOf() } += amendment
    }

    internal fun circuitBreakerAmendments(): Map<String, List<CircuitBreakerAmendment>> = circuitBreakers

    internal fun timeLimiterAmendments(): Map<String, List<TimeLimiterAmendment>> = timeLimiters
}

/** A component that wants Resilience4j instances amended at startup. Contributes to a plan; applies nothing. */
fun interface ResilienceAmendmentContributor {
    fun contribute(plan: AmendmentPlan)
}

/**
 * Applies every contributed amendment to this module's Resilience4j registries, ONCE per instance, after the
 * context is fully constructed.
 *
 * ## Why a single applier rather than one registrar per policy
 *
 * Each registration of an amended config is a remove + recreate - a Resilience4j config is immutable, so there
 * is no other way to override one - and each remove + recreate makes the metric and health binders re-bind to
 * a new instance. With one applying component per policy, an instance that two policies care about goes
 * through that twice, in an order Spring does not promise. Collecting first and applying once removes both:
 * one recreate per instance, and no ordering question between beans. What order remains - between two
 * amendments of the SAME instance - is contribution order, which is explicit and testable.
 *
 * ## Why it runs in afterSingletonsInstantiated
 *
 * Not in a `@PostConstruct`: the registries materialize their instances from `resilience4j.*.instances.<name>`
 * during singleton creation, and the metric/health binders attach then too. Amending before that point would
 * base the change on a config the operator's YAML has not produced yet, and the recreate would leave the
 * binders pointing at an instance nobody uses. [SmartInitializingSingleton] runs after every non-lazy
 * singleton exists, which is exactly late enough.
 *
 * Consumers that read a derived value must therefore READ IT LAZILY - an adapter capturing its time-limiter
 * budget in a `val` at construction would pin the pre-amendment value for the lifetime of the context
 * (`MeshTimeoutOrderingTest` pins that hand-off from both sides).
 */
@Component
class ResilienceAmendmentApplier(
    private val contributors: List<ResilienceAmendmentContributor>,
    private val circuitBreakerRegistry: CircuitBreakerRegistry,
    private val timeLimiterRegistry: TimeLimiterRegistry,
) : SmartInitializingSingleton {
    private val log = LoggerFactory.getLogger(ResilienceAmendmentApplier::class.java)

    override fun afterSingletonsInstantiated() = applyAmendments()

    /** Plain side-effecting method, so the wiring can be exercised against `Registry.of(...)` without Spring. */
    fun applyAmendments() {
        val plan = AmendmentPlan()
        contributors.forEach { it.contribute(plan) }

        val circuitBreakers = plan.circuitBreakerAmendments()
        val timeLimiters = plan.timeLimiterAmendments()
        circuitBreakers.forEach { (alias, amendments) ->
            // Read BEFORE amending: `circuitBreaker(name, config)` creates the instance when the name is free,
            // so afterwards an alias that never existed is indistinguishable from one that did.
            val existedBefore = circuitBreakerRegistry.find(alias).isPresent
            val expected = circuitBreakerRegistry.amendAll(alias, amendments)
            verifyApplied(
                alias,
                LAYER_CIRCUIT_BREAKER,
                existedBefore,
                expected,
                circuitBreakerRegistry.find(alias).orElse(null)?.circuitBreakerConfig,
            )
        }
        timeLimiters.forEach { (alias, amendments) ->
            val existedBefore = timeLimiterRegistry.find(alias).isPresent
            val expected = timeLimiterRegistry.amendAll(alias, amendments)
            verifyApplied(
                alias,
                LAYER_TIME_LIMITER,
                existedBefore,
                expected,
                timeLimiterRegistry.find(alias).orElse(null)?.timeLimiterConfig,
            )
        }

        log.info(
            "Resilience4j amendments applied by {} contributor(s): {} circuit breaker instance(s) {}, " +
                "{} time limiter instance(s) {}",
            contributors.size,
            circuitBreakers.size,
            circuitBreakers.keys.sorted(),
            timeLimiters.size,
            timeLimiters.keys.sorted(),
        )
    }

    /**
     * That the amended config actually reached the instance the registry now holds - by REFERENCE, not by
     * field comparison.
     *
     * [CircuitBreakerRegistry.amendAll] builds the config object and hands it to `circuitBreaker(name, config)`,
     * so the resulting instance must hold that very object. Anything else means the registration returned the
     * EXISTING instance instead of creating a new one, which is "Trap 2" in `RegistryAmendments` - a config is
     * immutable and registering does not override, so overriding is only ever a remove + recreate. An identity
     * comparison catches exactly that, and costs nothing; a field comparison would need
     * `CircuitBreakerConfig.equals`, which is not usable (the config carries predicates) and would also be
     * answering a different question.
     *
     * The second finding needs [existedBefore] because it cannot be seen afterwards: `circuitBreaker(name,
     * config)` CREATES the instance when the name is free, so amending an alias this deployment does not have
     * succeeds - onto a phantom instance nobody ever calls. The policy is then absent exactly where it was
     * meant to apply, and every after-the-fact check says the amendment worked.
     *
     * **What this does NOT check:** whether the amendment's SEMANTICS are right - that the timeout really is
     * three seconds. That belongs to the contributors' own tests. Here the only question is whether the change
     * landed on the instance the registry serves, because failing to do so is silent: nothing throws, the alias
     * keeps working, and it simply keeps its old behaviour.
     *
     * Nothing is repaired here. The phantom instance is left in place deliberately - removing an entry this
     * component did not create would be a second guess about a registry it shares with the rest of the host.
     */
    private fun verifyApplied(
        alias: String,
        layer: String,
        existedBefore: Boolean,
        expected: Any,
        actual: Any?,
    ) {
        if (!existedBefore) {
            log
                .resilienceEvent(Level.ERROR, layer, alias, EVENT_AMENDMENT_UNKNOWN_ALIAS)
                .setMessage(
                    "Resilience4j amendment for '{}' targeted a name this deployment does not have. Registering " +
                        "a config under a free name CREATES the instance, so the amendment succeeded onto a " +
                        "phantom nobody calls - and the policy it carries is absent wherever it was meant to " +
                        "apply. Either the alias is misspelled, or the endpoint it belongs to is not configured " +
                        "here.",
                ).addArgument(alias)
                .log()
            return
        }
        if (actual === expected) return
        log
            .resilienceEvent(Level.ERROR, layer, alias, EVENT_AMENDMENT_NOT_APPLIED)
            .setMessage(
                "Resilience4j amendment for '{}' did NOT reach the registry: the instance there holds a " +
                    "DIFFERENT config object than the one just built, so the remove + recreate did not replace " +
                    "it. Registering a config over an existing name does not override it, so this alias keeps " +
                    "its previous behaviour - silently, since nothing fails.",
            ).addArgument(alias)
            .log()
    }

    companion object {
        /**
         * Applies [contributors] to the two registries exactly as the context does at startup. For tests, which
         * would otherwise have to reproduce the collect-then-fold step and could drift from it.
         */
        fun applyOnce(
            circuitBreakerRegistry: CircuitBreakerRegistry,
            timeLimiterRegistry: TimeLimiterRegistry,
            vararg contributors: ResilienceAmendmentContributor,
        ) = ResilienceAmendmentApplier(contributors.toList(), circuitBreakerRegistry, timeLimiterRegistry)
            .applyAmendments()
    }
}
