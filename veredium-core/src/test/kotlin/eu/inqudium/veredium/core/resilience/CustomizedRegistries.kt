package eu.inqudium.veredium.core.resilience

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiter
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry

/**
 * Applies the mesh customizers to plain `Registry.of(...)` registries the way Resilience4j's Boot configuration
 * applies them to the registry it builds - so a test can exercise the customized instance without a Spring
 * context. The verdicts are [TestVerdicts]' unless a test hands in its own.
 *
 * The Boot configuration seeds a builder from the instance's YAML (named base config plus instance properties),
 * hands it to the customizer registered under the instance name, and registers the result BEFORE the instance
 * exists. Here the seed is the config already registered under the alias - which is how a test states "the
 * operator's YAML said this" - or the registry default; the customizer runs; the result replaces whatever was
 * there. The replacement is the one thing the real path never does (it never has a prior instance), and it is
 * what lets a test register a marker config first and assert what survives the customizer.
 */
object CustomizedRegistries {
    /** The circuit breaker of [endpoint], customized over [base] (default: the config registered under its alias). */
    fun circuitBreaker(
        registry: CircuitBreakerRegistry,
        endpoint: ResolvedTimeouts,
        base: CircuitBreakerConfig? = null,
        verdicts: BreakerVerdicts = TestVerdicts.breakerVerdicts,
    ): CircuitBreaker {
        val seed = base ?: registry.find(endpoint.alias).map { it.circuitBreakerConfig }.orElse(registry.defaultConfig)
        val builder = CircuitBreakerConfig.from(seed)
        circuitBreakerCustomizer(endpoint, verdicts).customize(builder)
        registry.remove(endpoint.alias)
        return registry.circuitBreaker(endpoint.alias, builder.build())
    }

    /**
     * The time limiter of [endpoint], customized over [base] when its derivation is enabled; an endpoint with the
     * derivation off has no customizer bean, so its instance is left exactly as registered (or created from the
     * default), as in the context.
     */
    fun timeLimiter(
        registry: TimeLimiterRegistry,
        endpoint: ResolvedTimeouts,
        base: TimeLimiterConfig? = null,
    ): TimeLimiter {
        if (!endpoint.timeLimiterEnabled) return registry.timeLimiter(endpoint.alias)
        val seed = base ?: registry.find(endpoint.alias).map { it.timeLimiterConfig }.orElse(registry.defaultConfig)
        val builder = TimeLimiterConfig.from(seed)
        timeLimiterCustomizer(endpoint).customize(builder)
        registry.remove(endpoint.alias)
        return registry.timeLimiter(endpoint.alias, builder.build())
    }

    /** Every circuit breaker of [mesh], customized. */
    fun circuitBreakers(
        registry: CircuitBreakerRegistry,
        mesh: MeshProperties,
        verdicts: BreakerVerdicts = TestVerdicts.breakerVerdicts,
    ) = mesh.resolveAll().values.forEach { circuitBreaker(registry, it, verdicts = verdicts) }

    /** Every time limiter of [mesh], customized where enabled. */
    fun timeLimiters(
        registry: TimeLimiterRegistry,
        mesh: MeshProperties,
    ) = mesh.resolveAll().values.forEach { timeLimiter(registry, it) }
}
