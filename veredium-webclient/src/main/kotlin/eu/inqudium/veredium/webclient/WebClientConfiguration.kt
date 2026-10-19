package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.mesh.MeshTimeoutConfig
import eu.inqudium.veredium.core.resilience.BreakerVerdicts
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import

/**
 * How this module wires itself into a host - the ONE configuration class of the reactive client, and the one a
 * host imports when it does not scan the package:
 *
 * - the `app.webclient` binding ([WebClientProperties]);
 * - the shared mesh wiring of `veredium-core` ([MeshTimeoutConfig]: the `app.mesh` binding, the per-alias
 *   Resilience4j customizers, the startup guard, the ladder logger), fed with this stack's verdicts
 *   ([WebClientVerdicts]) through the [BreakerVerdicts] bean - so a host that scans this module's package gets
 *   the core wiring too, without knowing the core package exists.
 *
 * The scanned `@Component`s of this module - [ResilientWebClientFactory], [ReactiveResilienceDecorator],
 * [WebClientObservationReport] - and the `@Configuration` [WebClientReactorContextPropagationConfig] are
 * deliberately not declared here as well: they are components because they have dependencies the context
 * resolves (or, for the hook, a process-wide side effect a host may want to leave out), and listing them twice
 * would create them twice.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(WebClientProperties::class)
@Import(MeshTimeoutConfig::class)
class WebClientConfiguration {
    @Bean
    fun breakerVerdicts(): BreakerVerdicts = WebClientVerdicts.breakerVerdicts
}
