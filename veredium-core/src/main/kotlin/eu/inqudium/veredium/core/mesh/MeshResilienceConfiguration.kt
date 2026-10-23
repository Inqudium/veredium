package eu.inqudium.veredium.core.mesh

import eu.inqudium.veredium.core.resilience.BreakerVerdicts
import eu.inqudium.veredium.core.resilience.MeshResilienceCustomizerRegistrar
import eu.inqudium.veredium.core.resilience.MeshResilienceStartupGuard
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.spring6.bulkhead.configure.BulkheadConfigurationProperties
import io.github.resilience4j.spring6.circuitbreaker.configure.CircuitBreakerConfigurationProperties
import io.github.resilience4j.spring6.timelimiter.configure.TimeLimiterConfigurationProperties
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import

/**
 * Integrates the mesh timeout derivation into the Resilience4j registries that resilience4j-spring-boot4
 * auto-configures - no registry bean of this module's own, no change to an instance after it exists.
 *
 * Four things are wired: the `app.mesh` binding; the per-alias `*ConfigCustomizer` beans that hand the derived
 * rungs and the client stack's breaker verdicts to Resilience4j while it builds each `instances.<alias>` entry
 * ([MeshResilienceCustomizerRegistrar]); the start-time check that every mesh alias is declared and actually
 * received them ([MeshResilienceStartupGuard]) - an undeclared alias silently runs on the registry default
 * instead of the operator's policy, and the guard is what turns that into a failed start; and the startup line
 * that prints the derived ladder ([MeshLadderLogger]).
 *
 * The one thing this configuration does NOT provide is the [BreakerVerdicts] bean it consumes: which exception
 * carries a response status and which exceptions this client raises itself are facts about the client STACK,
 * so each stack's configuration declares that bean and `@Import`s this class. A host therefore never imports
 * this class directly; it comes with the stack it depends on.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(MeshProperties::class)
@Import(MeshResilienceCustomizerRegistrar::class)
class MeshResilienceConfiguration {
    @Bean
    fun meshLadderLogger(meshProperties: MeshProperties): MeshLadderLogger = MeshLadderLogger(meshProperties)

    @Bean
    fun meshResilienceStartupGuard(
        meshProperties: MeshProperties,
        verdicts: BreakerVerdicts,
        circuitBreakerProperties: CircuitBreakerConfigurationProperties,
        timeLimiterProperties: TimeLimiterConfigurationProperties,
        bulkheadProperties: BulkheadConfigurationProperties,
        circuitBreakerRegistry: CircuitBreakerRegistry,
        timeLimiterRegistry: TimeLimiterRegistry,
        bulkheadRegistry: BulkheadRegistry,
        connectionPools: ObjectProvider<MeshConnectionPools>,
    ): MeshResilienceStartupGuard =
        MeshResilienceStartupGuard(
            meshProperties,
            verdicts,
            circuitBreakerProperties,
            timeLimiterProperties,
            bulkheadProperties,
            circuitBreakerRegistry,
            timeLimiterRegistry,
            bulkheadRegistry,
        ) { connectionPools.orderedStream().toList().flatMap { it.connectionPools() } }
}
