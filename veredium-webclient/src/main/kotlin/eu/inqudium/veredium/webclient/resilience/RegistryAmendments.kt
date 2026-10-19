package eu.inqudium.veredium.webclient.resilience

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry

/*
 * Changing the configuration of an ALREADY REGISTERED Resilience4j instance, in place.
 *
 * Two components in this module do that at startup, for different reasons and on different knobs - the mesh
 * ladder derives timeouts, and the client-side-failure rule amends the circuit breaker's ignore predicate. What
 * they share is not the policy but the MECHANICS, and those mechanics have three traps in them. This file is
 * the one place where each is handled, so a third caller inherits the answers instead of rediscovering them.
 *
 * **Trap 1: which config is the base.** `registry.getConfiguration(name)` sees only NAMED `configs:` entries;
 * an instance materialized from `instances.<name>` is not among them, and asking the registry for its default
 * silently discards whatever the operator configured per backend. The lookup below therefore asks in order of
 * specificity - a materialized instance first, then a named configuration, then the registry default - so an
 * amendment always builds on the most specific policy that exists at that moment.
 *
 * **Trap 2: registering does not override.** A Resilience4j config is immutable, and `circuitBreaker(name,
 * config)` RETURNS the existing instance rather than replacing it when the name is taken. Overriding means
 * remove + recreate, which is also why callers must run late enough (after every non-lazy singleton) for the
 * metric and health binders to re-bind to the new instance.
 *
 * **Trap 3: order between amendments.** Two amendments to the same instance must converge regardless of which
 * runs first. They do, because `from(<existing config>)` copies what the previous one built - including the
 * assembled `ignoreExceptionPredicate`. That is a property of the library, not of the callers, and the reason
 * it belongs here: `AmendmentOrderIndependenceTest` pins it once instead of two KDocs claiming it.
 */

/** A change to one Resilience4j config, expressed against the config it is amending. */
typealias CircuitBreakerAmendment = CircuitBreakerConfig.Builder.(base: CircuitBreakerConfig) -> Unit

typealias TimeLimiterAmendment = TimeLimiterConfig.Builder.(base: TimeLimiterConfig) -> Unit

/**
 * A [CircuitBreakerAmendment] that ADDS [predicate] to the instance's ignore predicate without dropping what it
 * already ignores. `ignoreException(...)` REPLACES the predicate that `from(base)` copied, so the base predicate
 * is OR-ed back in explicitly - the additive contract of Trap 3 above. Centralising that OR here is the point of
 * this file: a registrar that wants the breaker to ignore one more class of error supplies only its [predicate],
 * not the wiring, and every such amendment still stacks with the others on the same instance regardless of
 * contribution order.
 */
fun ignoreAlso(predicate: (Throwable) -> Boolean): CircuitBreakerAmendment = { base -> ignoreException { error -> predicate(error) || base.ignoreExceptionPredicate.test(error) } }

/**
 * A [CircuitBreakerAmendment] that WITHDRAWS [predicate]'s matches from what the breaker records as a
 * failure, without disturbing how the base config judges everything else. In Resilience4j's three verdicts -
 * recorded, ignored, success - not-recorded-and-not-ignored IS success, so this is the amendment that makes a
 * class of error count as positive evidence about the backend.
 *
 * **Success and ignore are not two spellings of "do not count it against the backend".** An ignored call
 * leaves the sliding window untouched; a successful one enters it and dilutes the failure rate. The
 * difference only shows when the ignored class is the bulk of the traffic: the window then fills with
 * whatever the predicate did NOT filter, and a handful of genuine faults can be its entire content. The
 * sync-bridge resolver breaker records that as measured rather than argued - on 2026-07-31, 41,903 benign
 * 4xx were filtered out and five 504s became the whole sample, opening the breaker at a nominal 100 %
 * failure rate (`SyncBridgeResolverConfig.resolverCircuitBreaker`, ADR-032 `answered-means-healthy`).
 *
 * So the rule is about what the failure means, not about whether it is our fault: a backend that ANSWERED -
 * promptly, with a rejection it is entitled to give - is positive evidence that it is alive and serving, and
 * belongs in the window as such. Only a call that never reached the backend is evidence in neither
 * direction, and that is what [ignoreAlso] is for.
 *
 * Order-independent for the same reason [ignoreAlso] is: `recordException(...)` REPLACES the predicate
 * `from(base)` copied, so the base is AND-ed back in - a later amendment can still withdraw something this
 * one left recordable, and neither can resurrect what the other declared a success.
 *
 * **Why not `recordAlso`, symmetric with [ignoreAlso].** Because the two fold in OPPOSITE directions and a
 * symmetric name would hide that. `ignoreAlso` GROWS a set and therefore ORs; this one SHRINKS a set and
 * therefore ANDs the negation. Feeding a negated predicate to an OR-fold does not invert it - `!benign(e) ||
 * base.record(e)` still records the benign error, because the default base predicate records everything and
 * the OR keeps it. `recordAlso` is the correct name for the amendment that widens what counts as a failure;
 * it is left unwritten and unclaimed for the day one is needed.
 */
fun recordUnless(predicate: (Throwable) -> Boolean): CircuitBreakerAmendment = { base -> recordException { error -> !predicate(error) && base.recordExceptionPredicate.test(error) } }

/**
 * Rebuilds the [CircuitBreakerConfig] registered under [name] by applying [amendment] on top of the most
 * specific existing config, and re-registers the instance with the result. Returns the config that is now in
 * force.
 *
 * The amendment receives the base config as its argument, which matters for any builder call that REPLACES
 * rather than adds: `ignoreException(Predicate)` overwrites the predicate `from(base)` copied, so an
 * amendment that means "also ignore X" has to OR the base predicate back in explicitly. Handing the base in
 * is what makes that possible without a second lookup at the call site.
 */
fun CircuitBreakerRegistry.amend(
    name: String,
    amendment: CircuitBreakerAmendment,
): CircuitBreakerConfig = amendAll(name, listOf(amendment))

/**
 * Applies several [amendments] to the instance under [name] and registers the result ONCE.
 *
 * Each amendment sees the config as the previous one left it, not the untouched original - so composing them
 * here is equivalent to applying them one after another, and an amendment that ADDS to a predicate still finds
 * its predecessor's work in the base it is handed. Folding first and registering once is the difference that
 * matters at startup: every registration is a remove + recreate, and every remove + recreate makes the metric
 * and health binders re-bind to a new instance.
 */
fun CircuitBreakerRegistry.amendAll(
    name: String,
    amendments: List<CircuitBreakerAmendment>,
): CircuitBreakerConfig {
    var config =
        find(name)
            .map { it.circuitBreakerConfig }
            .or { getConfiguration(name) }
            .orElseGet { defaultConfig }
    amendments.forEach { amendment ->
        val base = config
        config = CircuitBreakerConfig.from(base).apply { amendment(base) }.build()
    }
    remove(name)
    circuitBreaker(name, config)
    return config
}

/**
 * The [TimeLimiterRegistry] counterpart of [CircuitBreakerRegistry.amend], with the same lookup order and the
 * same remove-and-recreate. The base is handed to the amendment for symmetry; no `TimeLimiterConfig` knob
 * currently needs it.
 */
fun TimeLimiterRegistry.amend(
    name: String,
    amendment: TimeLimiterAmendment,
): TimeLimiterConfig = amendAll(name, listOf(amendment))

/** The [TimeLimiterRegistry] counterpart of [CircuitBreakerRegistry.amendAll], with the same fold semantics. */
fun TimeLimiterRegistry.amendAll(
    name: String,
    amendments: List<TimeLimiterAmendment>,
): TimeLimiterConfig {
    var config =
        find(name)
            .map { it.timeLimiterConfig }
            .or { getConfiguration(name) }
            .orElseGet { defaultConfig }
    amendments.forEach { amendment ->
        val base = config
        config = TimeLimiterConfig.from(base).apply { amendment(base) }.build()
    }
    remove(name)
    timeLimiter(name, config)
    return config
}
