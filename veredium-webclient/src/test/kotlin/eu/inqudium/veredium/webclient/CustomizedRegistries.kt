package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.resilience.circuitBreakerCustomizer
import eu.inqudium.veredium.core.resilience.timeLimiterCustomizer
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry

/**
 * Applies the mesh customizers, fed with THIS stack's [WebClientVerdicts], to plain `Registry.of(...)`
 * registries the way Resilience4j's Boot configuration applies them to the registry it builds - so a test can
 * exercise the customized instance through the real [ReactiveResilienceDecorator] without a Spring context.
 *
 * The seed is the config already registered under the alias - which is how a test states "the operator's YAML
 * said this" - or the registry default; the customizer runs; the result replaces whatever was there. (The core's
 * own test helper of the same name does the same with a stand-in stack; this one exists so the verdicts under
 * test are the ones this module ships.)
 */
object CustomizedRegistries {
    /** Every circuit breaker of [mesh], customized with the reactive stack's verdicts. */
    fun circuitBreakers(
        registry: CircuitBreakerRegistry,
        mesh: MeshProperties,
    ) = mesh.resolveAll().values.forEach { endpoint ->
        val seed = registry.find(endpoint.alias).map { it.circuitBreakerConfig }.orElse(registry.defaultConfig)
        val builder = CircuitBreakerConfig.from(seed)
        circuitBreakerCustomizer(endpoint, WebClientVerdicts.breakerVerdicts).customize(builder)
        registry.remove(endpoint.alias)
        registry.circuitBreaker(endpoint.alias, builder.build())
    }

    /** Every time limiter of [mesh] whose derivation is on, customized; the others left as registered. */
    fun timeLimiters(
        registry: TimeLimiterRegistry,
        mesh: MeshProperties,
    ) = mesh.resolveAll().values.filter { it.timeLimiterEnabled }.forEach { endpoint ->
        val seed = registry.find(endpoint.alias).map { it.timeLimiterConfig }.orElse(registry.defaultConfig)
        val builder = TimeLimiterConfig.from(seed)
        timeLimiterCustomizer(endpoint).customize(builder)
        registry.remove(endpoint.alias)
        registry.timeLimiter(endpoint.alias, builder.build())
    }
}
