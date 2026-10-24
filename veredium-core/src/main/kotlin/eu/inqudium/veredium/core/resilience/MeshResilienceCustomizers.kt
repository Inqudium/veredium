package eu.inqudium.veredium.core.resilience

import eu.inqudium.veredium.core.mesh.MeshConnectionPools
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.common.circuitbreaker.configuration.CircuitBreakerConfigCustomizer
import io.github.resilience4j.common.timelimiter.configuration.TimeLimiterConfigCustomizer
import io.github.resilience4j.spring6.bulkhead.configure.BulkheadConfigurationProperties
import io.github.resilience4j.spring6.circuitbreaker.configure.CircuitBreakerConfigurationProperties
import io.github.resilience4j.spring6.timelimiter.configure.TimeLimiterConfigurationProperties
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.springframework.beans.factory.BeanRegistrar
import org.springframework.beans.factory.BeanRegistry
import org.springframework.core.env.Environment

/** The slow-call rate an enabled circuit breaker is pinned to when the derived slow-call sits below `individual`. */
internal const val SLOW_CALL_RATE_PIN_PERCENT: Float = 100f

/**
 * The customizer of [endpoint]'s circuit breaker: the three verdicts of [verdicts], plus the mesh-derived
 * slow-call knobs when the endpoint has the circuit-breaker derivation enabled.
 *
 * ## Three verdicts, not two
 *
 * Resilience4j has three answers for an error - recorded, ignored, success - and the breaker's error hook
 * records EVERY exception unless the config says otherwise, with an empty ignore list by default. Out of the
 * box the breaker therefore charges the backend for faults it never caused and opens the circuit while the
 * backend is perfectly healthy. The two predicates sort every error the same way (`answered-means-healthy`,
 * `veredium-restclient/docs/decisions.md` D2):
 *
 * | The call | Verdict |
 * |---|---|
 * | never reached the backend, or the caller stopped waiting ([ClientSideFailurePolicy.clientSideFailures], as a cause too) | **ignored** - evidence in neither direction |
 * | reached it and got a benign 4xx ([HttpExchangeErrorPolicy.isExpectedClientError]) | **success** - the backend answered, promptly and correctly |
 * | anything else - transport faults, 5xx, 408/429, the TimeLimiter abort | **failure** |
 *
 * The middle row is the one that is easy to get wrong in the safe-looking direction. Ignoring a benign 4xx
 * also keeps it out of the sliding window, so on a 4xx-dominated backend the window fills with whatever was
 * left over and a few genuine faults become its entire content, until the breaker opens at a nominal 100 %
 * failure rate on a handful of timeouts (the measured case is recorded under D2, "Answered means healthy", in
 * `veredium-restclient/docs/decisions.md`). A 4xx is positive evidence that the backend is alive and serving, and enters the window as such. What
 * stays IGNORED is the call that carries no evidence about the backend at all.
 *
 * ## Additive, not replacing
 *
 * `ignoreException(predicate)` and `recordException(predicate)` REPLACE the predicate the builder holds, and the
 * builder holds what the operator wrote into YAML (`ignore-exceptions`, `record-exceptions`, the predicate
 * classes). The customizer therefore reads the config the builder would produce as it stands and folds it back
 * in: the ignore predicate is OR-ed (the breaker ignores MORE), the record predicate is AND-ed (it records
 * LESS). An operator's own list survives; a benign 4xx cannot be recorded by it, since a 4xx that the adapter
 * contract documents as a normal outcome is never a backend fault.
 *
 * That only holds because the customizer also EMPTIES the builder's two class lists once it has read them.
 * The lists are not part of the predicate a setter replaces: `build()` OR-s `record-exceptions` /
 * `ignore-exceptions` back in on top of whatever predicate is set, so an operator who listed a superclass of
 * the stack's 4xx exception (`record-exceptions: [...WebClientResponseException]`, to count 5xx explicitly)
 * would have had the benign 404 recorded again - `isInstance(list) OR (!benign AND configured)`. Their effect
 * is already inside the captured config's predicates, so after the customizer they reach the instance through
 * those and through nothing else.
 *
 * ## The derived slow-call knobs
 *
 * Only when [ResolvedTimeouts.circuitBreakerEnabled]: the `slowCallDurationThreshold` becomes the ladder's
 * [ResolvedTimeouts.slowCallDuration], and when that sits below `individual` (the "inversion") the slow-call
 * rate is pinned to 100 % in the SAME customizer - the derived duration is only safe against a healthy-but-slow
 * endpoint when the rate demands total degradation before opening, so the pair can never be split apart. With
 * the toggle off, the knobs stay whatever YAML or the library default made them; the two verdicts apply
 * regardless, because the breaker is in the call path regardless.
 */
fun circuitBreakerCustomizer(
    endpoint: ResolvedTimeouts,
    verdicts: BreakerVerdicts,
): CircuitBreakerConfigCustomizer =
    CircuitBreakerConfigCustomizer.of(endpoint.alias) { builder ->
        // What YAML produced for this instance, read before anything is replaced. `build()` is side-effect
        // free: it assembles a config from the builder's fields and leaves the builder as it was.
        val configured = builder.build()
        // Assumption: CircuitBreakerConfig.Builder.build() (Resilience4j 2.4.0) composes each final predicate as
        // `classes(list) OR predicate` (PredicateCreator.createExceptionsPredicate). The lists' effect is inside
        // `configured` now; left on the builder, `record-exceptions` would be OR-ed back past the benign-4xx
        // verdict below. Empty lists drop out of that composition entirely (function KDoc, "Additive").
        builder.recordExceptions()
        builder.ignoreExceptions()
        builder.ignoreException { error ->
            verdicts.clientSideFailure.isClientSideFailure(error) || configured.ignoreExceptionPredicate.test(error)
        }
        builder.recordException { error ->
            !verdicts.httpExchangeError.isExpectedClientError(error) && configured.recordExceptionPredicate.test(error)
        }
        if (endpoint.circuitBreakerEnabled) {
            builder.slowCallDurationThreshold(endpoint.slowCallDuration)
            if (endpoint.slowCallInverted) builder.slowCallRateThreshold(SLOW_CALL_RATE_PIN_PERCENT)
        }
    }

/**
 * The customizer of [endpoint]'s time limiter: the ladder's [ResolvedTimeouts.timeLimiterTimeout] as the budget.
 * Registered only for an endpoint whose time-limiter derivation is enabled (see
 * [MeshResilienceRegistrar]); a disabled endpoint keeps the budget its YAML or the library default
 * gave it, untouched. Everything else on the instance - `cancelRunningFuture` above all, which the blocking
 * decorator relies on - is inherited from YAML, which is why this customizer sets exactly one knob.
 */
fun timeLimiterCustomizer(endpoint: ResolvedTimeouts): TimeLimiterConfigCustomizer = TimeLimiterConfigCustomizer.of(endpoint.alias) { builder -> builder.timeoutDuration(endpoint.timeLimiterTimeout) }

/**
 * The shared mesh wiring a client stack registers with its own [verdicts]: one circuit-breaker customizer per
 * mesh alias, one time-limiter customizer per alias whose time-limiter derivation is enabled, and the
 * [MeshResilienceStartupGuard] - as beans, which is what Resilience4j's Spring configuration collects.
 *
 * Abstract, because the verdicts are a fact about the client STACK - which exception carries a response status,
 * which failures the client raises itself - and the stack is known at compile time. Each stack subclasses it with
 * its verdicts object (`RestClientMeshRegistrar`, `WebClientMeshRegistrar`) and imports the subclass next to
 * [eu.inqudium.veredium.core.mesh.MeshResilienceConfiguration]. The verdicts therefore never pass through the context: a host can neither replace
 * them with a bean of its own nor override them by accident, and the guard probes with the verdicts the customizers
 * actually installed.
 *
 * A [BeanRegistrar] rather than `@Bean` methods because the number of beans is only known once `app.mesh` is
 * read: the aliases come from the endpoint list. The registrar binds `app.mesh` from the [Environment] for
 * exactly that enumeration - which beans to declare under which names - and nothing else; the customizer each
 * bean supplies is built from the [MeshProperties] BEAN, so every consumer (the client factory, the ladder
 * logger, the guard, these customizers) reasons about the one resolved set of [ResolvedTimeouts] the context
 * holds. The bean name carries the stack and the alias, so two customizers can never collide on a Spring name.
 *
 * ## The stack's own endpoints
 *
 * The registrar declares beans for the endpoints its [stack] owns ([MeshProperties.endpointsOwnedBy]) and for no
 * other: with both client stacks in one context, each customizes its own aliases with its own verdicts. A registrar
 * cannot see what another registrar declares, so each also claims the endpoints that name no stack; with two stacks
 * such an endpoint is claimed twice, and [MeshOwnershipCheck] fails the start, naming the `stack` key to set.
 *
 * ## How the customizers reach the instances
 *
 * Resilience4j's Spring integration builds one config per `resilience4j.<layer>.instances.<name>` entry when it
 * creates the registry bean, and consults a `*ConfigCustomizer` bean named after the instance as the LAST step
 * of that build - after the named base config and the instance's own properties have been applied, and before
 * the instance exists. A customizer therefore sees the operator's YAML on its builder and shapes the instance
 * that the metric and health binders then bind to: no second lifecycle, no remove + recreate, no re-binding.
 *
 * Two things follow, and both are load-bearing:
 *
 *  - **One customizer per instance name.** Resilience4j rejects a second customizer for the same name at
 *    startup, so everything the wiring wants on an instance - the two breaker verdicts and, when enabled, the
 *    mesh-derived rungs - is ONE customizer per alias, built by [circuitBreakerCustomizer] and
 *    [timeLimiterCustomizer].
 *  - **An undeclared instance is created on the default.** For a customizer name it finds no `instances.<name>`
 *    entry for, the Boot configuration creates the instance anyway - on the registry DEFAULT config, not on the
 *    named base config the operator chose (`mesh-backend`). The verdicts and the derived budget arrive, every
 *    value of the shipped policy is silently lost. [MeshResilienceStartupGuard] turns that silence into a
 *    failed start.
 *
 * The verdicts themselves live in the stack's [BreakerVerdicts]; the registrar only installs them. They are
 * installed in CODE rather than written as `ignore-exceptions` / `record-exceptions` into YAML because they
 * are structural invariants of the stack's decorator pipeline - the code that raises `BulkheadFullException`
 * is the code that must discount it - and a YAML entry would be silently lost whenever an instance referenced
 * a different named config.
 */
abstract class MeshResilienceRegistrar(
    private val stack: String,
    private val verdicts: BreakerVerdicts,
) : BeanRegistrar {
    override fun register(
        registry: BeanRegistry,
        env: Environment,
    ) {
        val mesh = MeshProperties.bind(env)
        mesh.endpointsOwnedBy(stack).values.sortedBy { it.alias }.forEach { endpoint ->
            registry.registerBean(circuitBreakerBeanName(stack, endpoint.alias), CircuitBreakerConfigCustomizer::class.java) { spec ->
                spec
                    .description("mesh circuit-breaker customizer for ${endpoint.triplet}")
                    .supplier { context -> circuitBreakerCustomizer(resolved(context, endpoint.triplet), verdicts) }
            }
            if (endpoint.timeLimiterEnabled) {
                registry.registerBean(timeLimiterBeanName(stack, endpoint.alias), TimeLimiterConfigCustomizer::class.java) { spec ->
                    spec
                        .description("mesh time-limiter customizer for ${endpoint.triplet}")
                        .supplier { context -> timeLimiterCustomizer(resolved(context, endpoint.triplet)) }
                }
            }
        }
        registry.registerBean(startupGuardBeanName(stack), MeshResilienceStartupGuard::class.java) { spec ->
            spec
                .description("mesh resilience startup guard of the $stack stack")
                .supplier { context ->
                    val connectionPools = context.beanProvider(MeshConnectionPools::class.java)
                    MeshResilienceStartupGuard(
                        context.bean(MeshProperties::class.java),
                        stack,
                        verdicts,
                        context.bean(CircuitBreakerConfigurationProperties::class.java),
                        context.bean(TimeLimiterConfigurationProperties::class.java),
                        context.bean(BulkheadConfigurationProperties::class.java),
                        context.bean(CircuitBreakerRegistry::class.java),
                        context.bean(TimeLimiterRegistry::class.java),
                        context.bean(BulkheadRegistry::class.java),
                    ) { connectionPools.orderedStream().toList().flatMap { it.connectionPools() } }
                }
        }
    }

    private fun resolved(
        context: BeanRegistry.SupplierContext,
        triplet: String,
    ): ResolvedTimeouts =
        context
            .bean(MeshProperties::class.java)
            .resolveAll()
            .getValue(triplet)

    companion object {
        /**
         * The prefix of a circuit-breaker customizer's bean name, `meshCircuitBreakerCustomizer.<stack>.<alias>`. The
         * stack in the name keeps two stacks' customizers apart, and it is how [MeshOwnershipCheck] reads which stack
         * claims which alias.
         */
        const val CIRCUIT_BREAKER_BEAN_PREFIX: String = "meshCircuitBreakerCustomizer."

        /** The prefix of a time-limiter customizer's bean name, `meshTimeLimiterCustomizer.<stack>.<alias>`. */
        const val TIME_LIMITER_BEAN_PREFIX: String = "meshTimeLimiterCustomizer."

        /**
         * The prefix of a stack's [MeshResilienceStartupGuard] bean name, `meshResilienceStartupGuard.<stack>` - how
         * [MeshOwnershipCheck] reads which stacks are in the context.
         */
        const val STARTUP_GUARD_BEAN_PREFIX: String = "meshResilienceStartupGuard."

        /** The bean name of [stack]'s circuit-breaker customizer for [alias]. */
        fun circuitBreakerBeanName(
            stack: String,
            alias: String,
        ): String = "$CIRCUIT_BREAKER_BEAN_PREFIX$stack.$alias"

        /** The bean name of [stack]'s time-limiter customizer for [alias]. */
        fun timeLimiterBeanName(
            stack: String,
            alias: String,
        ): String = "$TIME_LIMITER_BEAN_PREFIX$stack.$alias"

        /** The bean name of [stack]'s startup guard. */
        fun startupGuardBeanName(stack: String): String = "$STARTUP_GUARD_BEAN_PREFIX$stack"
    }
}
