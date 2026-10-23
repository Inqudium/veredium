package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.mesh.MeshResilienceConfiguration
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import eu.inqudium.veredium.core.resilience.MeshResilienceRegistrar
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.observation.ObservationRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import

/**
 * How this module wires itself into a host - the ONE configuration class of the blocking client, and the one a
 * host imports when it does not scan the package:
 *
 * - the `app.restclient` binding ([RestClientProperties]);
 * - the shared mesh wiring of `veredium-core`: [MeshResilienceConfiguration] (the `app.mesh` binding, the ladder
 *   logger) and [RestClientMeshRegistrar] (the per-alias Resilience4j customizers and the startup guard, with
 *   this stack's verdicts, [RestClientVerdicts], handed in directly) - so a host that scans this module's package
 *   gets the core wiring too, without knowing the core package exists;
 * - this module's instance of the core's [BlockingResilienceDecorator], the pipeline every call of a
 *   [ResilientRestClient] runs through, over the three registries `resilience4j-spring-boot4` auto-configures.
 *   The decorator carries no Spring annotation of its own - it is a plain component of `veredium-core`, usable
 *   without any client - so the bean is declared here, and here is where this module names its workers:
 *   `restclient-call-<n>` is the thread name on every log line written during an exchange, which is how a
 *   reader tells this module's calls from a host's own decorated calls in the same JVM. The observation
 *   registry is optional (the decorator's KDoc, "The thread hand-off the TimeLimiter needs"): absent, the
 *   exchange's observation starts as its own root.
 *
 * The scanned `@Component`s of this module - [ResilientRestClientFactory], [RestClientObservationReport] - are
 * deliberately not declared here as well: they are components because they have dependencies the context
 * resolves, and listing them twice would create them twice.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RestClientProperties::class)
@Import(MeshResilienceConfiguration::class, RestClientMeshRegistrar::class)
class RestClientConfiguration {
    /**
     * This module's [BlockingResilienceDecorator]: over the host's three Resilience4j registries, with the host's
     * [ObservationRegistry] when there is one, and with workers named [WORKER_NAME_PREFIX]`<n>`.
     */
    @Bean
    fun blockingResilienceDecorator(
        circuitBreakerRegistry: CircuitBreakerRegistry,
        timeLimiterRegistry: TimeLimiterRegistry,
        bulkheadRegistry: BulkheadRegistry,
        observationRegistry: ObjectProvider<ObservationRegistry>,
    ): BlockingResilienceDecorator =
        BlockingResilienceDecorator(
            circuitBreakerRegistry,
            timeLimiterRegistry,
            bulkheadRegistry,
            observationRegistry.ifAvailable,
            WORKER_NAME_PREFIX,
        )

    companion object {
        /** The name prefix of this module's worker threads, as thread dumps and the exchange event's `thread` show it. */
        const val WORKER_NAME_PREFIX: String = "restclient-call-"
    }
}

/** The shared mesh wiring of `veredium-core`, registered with this stack's verdicts. */
class RestClientMeshRegistrar : MeshResilienceRegistrar(RestClientVerdicts.breakerVerdicts)
