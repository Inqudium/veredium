package eu.inqudium.veredium.core.resilience

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadFullException
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.spring6.bulkhead.configure.BulkheadConfigurationProperties
import io.github.resilience4j.spring6.circuitbreaker.configure.CircuitBreakerConfigurationProperties
import io.github.resilience4j.spring6.timelimiter.configure.TimeLimiterConfigurationProperties
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.SmartInitializingSingleton

/**
 * Fails the start when a mesh alias is not wired the way the stack's decorator assumes - and says which YAML key
 * to add.
 *
 * ## Why a guard at all
 *
 * Nothing in the wiring fails on its own when `resilience4j.<layer>.instances.<alias>` is missing for an alias
 * the decorator uses. Resilience4j's Boot configuration creates an instance for every customizer name it finds
 * no declaration for - on the registry DEFAULT config, not on the named base config the operator chose
 * (`lap-backend`) - and the bulkhead, which has no customizer, is created on the first call from the default
 * likewise. The call runs; the module's verdicts and the derived budget are even present on the breaker and the
 * time limiter; what is silently lost is every value the operator wrote into the shipped policy, and the only
 * trace is a meter whose values look like nobody chose them. A registry bean a host or a test built by hand is
 * the second silent case: every alias present, no customizer ever consulted. This guard makes both loud, and
 * it runs where the registries are complete and nothing has been called yet: after every singleton exists,
 * before the context is reported up.
 *
 * ## What it checks
 *
 * For every endpoint of [MeshProperties.resolveAll]:
 *
 *  1. **Declared** - `resilience4j.<layer>.instances.<alias>` exists in the bound properties for the circuit
 *     breaker, the time limiter AND the bulkhead. Read off the properties, not the registry: the registry holds
 *     the instance either way (see above), so its presence proves nothing.
 *  2. **Reached** - the registry holds an instance under the alias, the breaker's config discounts a
 *     client-side failure and does not record a benign 4xx and, where the derivation is enabled, carries the
 *     derived slow-call threshold; the time limiter carries the derived budget. Asserted by BEHAVIOUR (the
 *     predicates are applied to probe exceptions), not by identity: the question is whether the instance the
 *     registry serves behaves as configured.
 *
 * Every problem is collected before anything is thrown, so one failed start names them all. It does NOT check
 * that the derived numbers are right - that is the ladder's own tests - only that they arrived.
 *
 * ## What it logs
 *
 * On success, the startup evidence an operator reads next to the meters: the two policy lines naming the
 * aliases the verdicts were installed on, and one line per endpoint stating what mesh decided for its time
 * limiter and circuit breaker. The derived numbers themselves are logged by `MeshLadderLogger` on
 * `ApplicationReadyEvent`; repeating them here would only let the two drift.
 *
 * The benign-4xx probe comes from the stack ([BreakerVerdicts.benignClientErrorProbe]): the record predicate is
 * applied to a 404 in the exact shape that stack throws, which is the only shape its policy recognises.
 */
class MeshResilienceStartupGuard(
    private val meshProperties: MeshProperties,
    private val verdicts: BreakerVerdicts,
    // The three property beans Resilience4j's Boot configuration binds `resilience4j.*` into - the record of
    // what the operator DECLARED, as opposed to what the registries ended up holding.
    private val circuitBreakerProperties: CircuitBreakerConfigurationProperties,
    private val timeLimiterProperties: TimeLimiterConfigurationProperties,
    private val bulkheadProperties: BulkheadConfigurationProperties,
    private val circuitBreakerRegistry: CircuitBreakerRegistry,
    private val timeLimiterRegistry: TimeLimiterRegistry,
    private val bulkheadRegistry: BulkheadRegistry,
) : SmartInitializingSingleton {
    private val log: Logger = LoggerFactory.getLogger(MeshResilienceStartupGuard::class.java)

    override fun afterSingletonsInstantiated() = verify()

    /**
     * The check itself, a plain method so it can be driven against `Registry.of(...)` without a context.
     *
     * @throws IllegalStateException naming every mesh alias that is undeclared or unreached, with the key to add.
     */
    fun verify() {
        val endpoints = meshProperties.resolveAll().values.sortedBy { it.alias }
        val problems = endpoints.flatMap(::problemsOf)
        check(problems.isEmpty()) {
            "Resilience4j is not wired for every mesh endpoint - the call path would silently run on registry " +
                "defaults. ${problems.size} problem(s):\n  - " + problems.joinToString("\n  - ")
        }
        val aliases = endpoints.joinToString(", ") { it.alias }
        verdicts.clientSideFailure.logPolicy(log.atInfo(), aliases)
        verdicts.httpExchangeError.logPolicy(log.atInfo(), aliases)
        endpoints.forEach { endpoint -> log.info("mesh backends [{}] {}: {}", endpoint.alias, endpoint.triplet, decisionOf(endpoint)) }
    }

    private fun problemsOf(endpoint: ResolvedTimeouts): List<String> {
        val alias = endpoint.alias
        val circuitBreaker = circuitBreakerRegistry.find(alias).orElse(null)
        val timeLimiter = timeLimiterRegistry.find(alias).orElse(null)
        val bulkhead = bulkheadRegistry.find(alias).orElse(null)
        return buildList {
            if (alias !in circuitBreakerProperties.instances) add(undeclared(LAYER_CIRCUIT_BREAKER, alias))
            if (alias !in timeLimiterProperties.instances) add(undeclared(LAYER_TIME_LIMITER, alias))
            if (alias !in bulkheadProperties.instances) add(undeclared(LAYER_BULKHEAD, alias))
            if (circuitBreaker == null) add(unreached(LAYER_CIRCUIT_BREAKER, alias, "has no instance in the registry"))
            if (timeLimiter == null) add(unreached(LAYER_TIME_LIMITER, alias, "has no instance in the registry"))
            if (bulkhead == null && alias in bulkheadProperties.instances) {
                add(unreached(LAYER_BULKHEAD, alias, "has no instance in the registry"))
            }
            circuitBreaker?.circuitBreakerConfig?.let { config ->
                if (!config.ignoreExceptionPredicate.test(PROBE_CLIENT_SIDE_FAILURE)) {
                    add(unreached(LAYER_CIRCUIT_BREAKER, alias, "does not ignore a client-side failure (BulkheadFullException)"))
                }
                if (config.recordExceptionPredicate.test(verdicts.benignClientErrorProbe)) {
                    add(unreached(LAYER_CIRCUIT_BREAKER, alias, "records a benign 4xx (404) as a backend failure"))
                }
                if (endpoint.circuitBreakerEnabled && config.slowCallDurationThreshold != endpoint.slowCallDuration) {
                    add(
                        unreached(
                            LAYER_CIRCUIT_BREAKER,
                            alias,
                            "has slowCallDurationThreshold=${config.slowCallDurationThreshold.toMillis()}ms instead of the " +
                                "derived ${endpoint.slowCallDuration.toMillis()}ms",
                        ),
                    )
                }
            }
            timeLimiter?.timeLimiterConfig?.let { config ->
                if (endpoint.timeLimiterEnabled && config.timeoutDuration != endpoint.timeLimiterTimeout) {
                    add(
                        unreached(
                            LAYER_TIME_LIMITER,
                            alias,
                            "has timeoutDuration=${config.timeoutDuration.toMillis()}ms instead of the derived " +
                                "${endpoint.timeLimiterTimeout.toMillis()}ms",
                        ),
                    )
                }
            }
        }
    }

    private fun undeclared(
        layer: String,
        alias: String,
    ): String =
        "resilience4j.$layer.instances.$alias is not declared: the $layer of mesh endpoint '$alias' runs on the " +
            "registry default config instead of the policy the operator chose (Resilience4j creates the instance for " +
            "the mesh customizer on the default; a bulkhead has no customizer and is created on the first call). " +
            "Declare the instance (base-config: lap-backend), or remove the endpoint from app.mesh.endpoints."

    private fun unreached(
        layer: String,
        alias: String,
        symptom: String,
    ): String =
        "the $layer '$alias' $symptom: the mesh customizer did not reach it. The registry was not built by " +
            "Resilience4j's Boot auto-configuration from the resilience4j.$layer.* properties - a customizer bean is " +
            "consulted only while that configuration builds its instances."

    private fun decisionOf(endpoint: ResolvedTimeouts): String {
        val timeLimiter =
            if (endpoint.timeLimiterEnabled) "on (${endpoint.timeLimiterTimeout.toMillis()}ms)" else "off (untouched)"
        val circuitBreaker =
            when {
                !endpoint.circuitBreakerEnabled -> "off (untouched)"
                endpoint.slowCallInverted -> "on (slowCall=${endpoint.slowCallDuration.toMillis()}ms, rate pinned to 100%)"
                else -> "on (slowCall=${endpoint.slowCallDuration.toMillis()}ms, rate inherited)"
            }
        return "timeLimiter=$timeLimiter, circuitBreaker=$circuitBreaker"
    }

    private companion object {
        // The layer spellings match Resilience4j's own property prefixes, so a message names the key verbatim.
        const val LAYER_CIRCUIT_BREAKER = "circuitbreaker"
        const val LAYER_TIME_LIMITER = "timelimiter"
        const val LAYER_BULKHEAD = "bulkhead"

        /**
         * A real rejection instance - the type is only constructible through a bulkhead's factory. The one
         * client-side failure both stacks share, which is why the guard can own this probe itself.
         */
        val PROBE_CLIENT_SIDE_FAILURE: BulkheadFullException =
            BulkheadFullException.createBulkheadFullException(Bulkhead.of("mesh-guard-probe", BulkheadConfig.ofDefaults()))
    }
}
