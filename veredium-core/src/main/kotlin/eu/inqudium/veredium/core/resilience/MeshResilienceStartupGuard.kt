package eu.inqudium.veredium.core.resilience

import eu.inqudium.veredium.core.mesh.MeshConnectionPool
import eu.inqudium.veredium.core.mesh.MeshConnectionPools
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
 * (`mesh-backend`) - and the bulkhead, which has no customizer, is created on the first call from the default
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
 * A breaker that records the benign 4xx is reported in one of two ways, because the two have different fixes.
 * When it does not ignore the client-side probe either, no customizer ever ran on it (the hand-built registry
 * above). When it DOES ignore the probe, the client-side verdict is in place - the customizer most likely ran -
 * and something composed after it put the 4xx back; the message then points at the instance's own
 * record-exceptions configuration instead of at the registry. (A hand-built registry that carries an ignore
 * list of its own but no benign-4xx rule looks the same from here, and the message says so.)
 *
 * ## What it warns about
 *
 * Once every alias is wired, the bulkheads are checked against the connection pool they share:
 * `sum(max-concurrent-calls)` over the aliases on a pool must not exceed its `max-connections`
 * (`app.mesh.defaults.transport.max-connections`).
 * The bulkheads are sized as a PARTITION of that pool; if together they admit more calls than it has
 * connections, the excess does not shed at the bulkhead with a named `BulkheadFullException` but queues for a
 * connection, and a pending-acquire timeout is a FAILURE on the breaker of whichever backend happened to be
 * waiting - not necessarily the one holding the connections. That is a sizing decision, not a wiring fault, and
 * a deliberately over-committed pool can be right for a host whose backends are never busy at once, so it is a
 * WARN naming both numbers and every alias, not a failed start. Which aliases share a pool is the client
 * modules' knowledge - each engine of a client factory has a pool of its own - so the pools come from the
 * [MeshConnectionPools] beans the factories are; a pool without a bound (the JDK client) is not
 * checked. Without such a bean the sum runs over ALL mesh aliases against the one pool the mesh transport
 * describes. Per engine the sum is exact for Apache's blocking pool (one total cap for every route) and
 * conservative where the cap is per remote address and an endpoint has a `base-url` of its own.
 * The values are read off the registry's instances, i.e. after base config and instance properties were
 * merged; the wiring check guarantees there is one per alias by then.
 *
 * ## What it logs
 *
 * On success, the startup evidence an operator reads next to the meters: the two policy lines naming the
 * aliases the verdicts were installed on, and one line per endpoint stating what mesh decided for its time
 * limiter and circuit breaker. The derived numbers themselves are logged by
 * [eu.inqudium.veredium.core.mesh.MeshLadderLogger] on `ApplicationReadyEvent`; repeating them here would only
 * let the two drift.
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
    /**
     * The pools the client factories built, read when [verify] runs - after every singleton exists, so the
     * factories are there to ask. Empty: the single shared pool of the mesh transport.
     */
    private val connectionPools: () -> List<MeshConnectionPool> = { emptyList() },
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
        warnIfBulkheadsOvercommitThePool(endpoints)
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
                val ignoresClientSideFailure = config.ignoreExceptionPredicate.test(PROBE_CLIENT_SIDE_FAILURE)
                if (!ignoresClientSideFailure) {
                    add(unreached(LAYER_CIRCUIT_BREAKER, alias, "does not ignore a client-side failure (BulkheadFullException)"))
                }
                if (config.recordExceptionPredicate.test(verdicts.benignClientErrorProbe)) {
                    add(
                        if (ignoresClientSideFailure) {
                            overridden(alias)
                        } else {
                            unreached(LAYER_CIRCUIT_BREAKER, alias, "records a benign 4xx (404) as a backend failure")
                        },
                    )
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
            "Declare the instance (base-config: mesh-backend), or remove the endpoint from app.mesh.endpoints."

    private fun unreached(
        layer: String,
        alias: String,
        symptom: String,
    ): String =
        "the $layer '$alias' $symptom: the mesh customizer did not reach it. The registry was not built by " +
            "Resilience4j's Boot auto-configuration from the resilience4j.$layer.* properties - a customizer bean is " +
            "consulted only while that configuration builds its instances."

    /**
     * The breaker records the benign 4xx although its client-side verdict is in place (class KDoc): not a
     * customizer that never ran, but a record rule composed past it.
     */
    private fun overridden(alias: String): String =
        "the $LAYER_CIRCUIT_BREAKER '$alias' records a benign 4xx (404) as a backend failure although it ignores a " +
            "client-side failure: the mesh customizer's client-side verdict is in place, so its benign-4xx verdict " +
            "was overridden by a record rule applied after it. Check resilience4j.$LAYER_CIRCUIT_BREAKER.instances." +
            "$alias and its base config for a record-exceptions or record-exception-predicate entry that matches the " +
            "stack's 4xx exception. (A registry built by hand with an ignore list of its own but without the " +
            "benign-4xx verdict shows the same symptom.)"

    /**
     * The pool check of the class KDoc ("What it warns about"). Runs only after the wiring check passed, so
     * every alias has a bulkhead instance in the registry - the merged values, not the declared fragments.
     */
    private fun warnIfBulkheadsOvercommitThePool(endpoints: List<ResolvedTimeouts>) {
        val aliases = endpoints.map { it.alias }.toSet()
        val pools =
            connectionPools().ifEmpty {
                listOf(MeshConnectionPool(SHARED_POOL, meshProperties.defaults.transport.maxConnections, aliases))
            }
        pools.forEach { pool ->
            val maxConnections = pool.maxConnections ?: return@forEach
            val permits =
                pool.aliases
                    .filter { it in aliases }
                    .sorted()
                    .associateWith { bulkheadRegistry.bulkhead(it).bulkheadConfig.maxConcurrentCalls }
            val total = permits.values.sumOf { it.toLong() }
            if (total <= maxConnections) return@forEach
            log.warn(
                "The mesh bulkheads admit more concurrent calls than the connection pool '{}' holds: " +
                    "sum(resilience4j.bulkhead.instances.<alias>.max-concurrent-calls)={} > " +
                    "app.mesh.defaults.transport.max-connections={} ({}). The excess queues for a connection instead " +
                    "of being shed by its bulkhead, and a pending-acquire timeout is recorded as a failure on the " +
                    "breaker of whichever backend is waiting. Lower the bulkheads or raise max-connections.",
                pool.name,
                total,
                maxConnections,
                permits.entries.joinToString(", ") { (alias, calls) -> "$alias=$calls" },
            )
        }
    }

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
        /** The pool's name when no client factory names its pools: the one the mesh transport describes. */
        const val SHARED_POOL: String = "shared"

        // The layer spellings match Resilience4j's own property prefixes, so a message names the key verbatim.
        const val LAYER_CIRCUIT_BREAKER: String = "circuitbreaker"
        const val LAYER_TIME_LIMITER: String = "timelimiter"
        const val LAYER_BULKHEAD: String = "bulkhead"

        /**
         * A real rejection instance - the type is only constructible through a bulkhead's factory. The one
         * client-side failure both stacks share, which is why the guard can own this probe itself.
         */
        val PROBE_CLIENT_SIDE_FAILURE: BulkheadFullException =
            BulkheadFullException.createBulkheadFullException(Bulkhead.of("mesh-guard-probe", BulkheadConfig.ofDefaults()))
    }
}
