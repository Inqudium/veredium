package eu.inqudium.veredium.testsupport

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import eu.inqudium.veredium.core.resilience.BreakerVerdicts
import eu.inqudium.veredium.core.resilience.circuitBreakerCustomizer
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import java.time.Duration

/**
 * Applies the mesh customizers, fed with a stack's [BreakerVerdicts], to plain `Registry.of(...)` registries the way
 * Resilience4j's Boot configuration applies them to the registry it builds - so a test can exercise the customized
 * instance through the real decorator without a Spring context.
 *
 * The Boot configuration seeds a builder from the instance's YAML (named base config plus instance properties), hands
 * it to the customizer registered under the instance name, and registers the result BEFORE the instance exists. Here
 * the seed is the config already registered under the alias - which is how a test states "the operator's YAML said
 * this" - or the registry default; the customizer runs; the result replaces whatever was there. The replacement is the
 * one thing the real path never does (it never has a prior instance), and it is what lets a test register a marker
 * config first and assert what survives the customizer.
 *
 * The same object exists in veredium-core's tests, which cannot depend on this module (see its pom). There it also
 * customizes single instances and the time limiters, which only the core's own tests exercise; the verdicts differ as
 * well: here a caller passes its stack's, there they default to the core's test verdicts.
 */
object CustomizedRegistries {
    /** Every circuit breaker of [mesh], customized with [verdicts]. */
    fun circuitBreakers(
        registry: CircuitBreakerRegistry,
        mesh: MeshProperties,
        verdicts: BreakerVerdicts,
    ) = mesh.resolveAll().values.forEach { circuitBreaker(registry, it, verdicts) }

    /** The circuit breaker of [endpoint], customized with [verdicts] over the config registered under its alias. */
    private fun circuitBreaker(
        registry: CircuitBreakerRegistry,
        endpoint: ResolvedTimeouts,
        verdicts: BreakerVerdicts,
    ): CircuitBreaker {
        val seed = registry.find(endpoint.alias).map { it.circuitBreakerConfig }.orElse(registry.defaultConfig)
        val builder = CircuitBreakerConfig.from(seed)
        circuitBreakerCustomizer(endpoint, verdicts).customize(builder)
        registry.remove(endpoint.alias)
        return registry.circuitBreaker(endpoint.alias, builder.build())
    }
}

/** Whether the circuit breaker registered under [alias] classifies [error] as ignorable (not a backend failure). */
fun ignores(
    registry: CircuitBreakerRegistry,
    error: Throwable,
    alias: String,
): Boolean =
    registry
        .circuitBreaker(alias)
        .circuitBreakerConfig.ignoreExceptionPredicate
        .test(error)

/** A registry whose bulkheads hold exactly one permit and wait [maxWait] for it - by default not at all. */
fun singlePermitBulkheads(maxWait: Duration = Duration.ZERO): BulkheadRegistry =
    BulkheadRegistry.of(
        BulkheadConfig
            .custom()
            .maxConcurrentCalls(1)
            .maxWaitDuration(maxWait)
            .build(),
    )
