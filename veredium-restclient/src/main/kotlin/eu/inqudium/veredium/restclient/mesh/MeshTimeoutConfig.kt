package eu.inqudium.veredium.restclient.mesh

import eu.inqudium.veredium.restclient.resilience.MeshResilienceCustomizerRegistrar
import eu.inqudium.veredium.restclient.resilience.MeshResilienceStartupGuard
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.spring6.bulkhead.configure.BulkheadConfigurationProperties
import io.github.resilience4j.spring6.circuitbreaker.configure.CircuitBreakerConfigurationProperties
import io.github.resilience4j.spring6.timelimiter.configure.TimeLimiterConfigurationProperties
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import

/**
 * Integrates the mesh timeout derivation into the Resilience4j registries that resilience4j-spring-boot4
 * auto-configures - no registry bean of this module's own, no change to an instance after it exists.
 *
 * Three things are wired: the `app.mesh` binding; the per-alias `*ConfigCustomizer` beans that hand the derived
 * rungs and this module's breaker verdicts to Resilience4j while it builds each `instances.<alias>` entry
 * ([MeshResilienceCustomizerRegistrar]); and the start-time check that every mesh alias is declared and
 * actually received them ([MeshResilienceStartupGuard]) - an undeclared alias silently runs on the registry
 * default instead of the operator's policy, and the guard is what turns that into a failed start.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(MeshProperties::class)
@Import(MeshResilienceCustomizerRegistrar::class)
class MeshTimeoutConfig {
    @Bean
    fun meshResilienceStartupGuard(
        meshProperties: MeshProperties,
        circuitBreakerProperties: CircuitBreakerConfigurationProperties,
        timeLimiterProperties: TimeLimiterConfigurationProperties,
        bulkheadProperties: BulkheadConfigurationProperties,
        circuitBreakerRegistry: CircuitBreakerRegistry,
        timeLimiterRegistry: TimeLimiterRegistry,
        bulkheadRegistry: BulkheadRegistry,
    ): MeshResilienceStartupGuard =
        MeshResilienceStartupGuard(
            meshProperties,
            circuitBreakerProperties,
            timeLimiterProperties,
            bulkheadProperties,
            circuitBreakerRegistry,
            timeLimiterRegistry,
            bulkheadRegistry,
        )
}
