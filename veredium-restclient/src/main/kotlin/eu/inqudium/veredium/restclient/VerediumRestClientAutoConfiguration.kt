package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.MeshResilienceConfiguration
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import eu.inqudium.veredium.core.resilience.MeshResilienceRegistrar
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.observation.ObservationRegistry
import jakarta.validation.ValidatorFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.core.env.Environment
import org.springframework.web.client.RestClient
import tools.jackson.databind.ObjectMapper

/**
 * How this module wires itself into a host: a Spring Boot auto-configuration, registered in
 * `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`, so the dependency is all a
 * host adds - no package to scan, no class to import.
 *
 * - the `app.restclient` binding ([RestClientProperties]);
 * - the shared mesh wiring of `veredium-core`: [MeshResilienceConfiguration] (the `app.mesh` binding, the ladder
 *   logger) and [RestClientMeshRegistrar] (the per-alias Resilience4j customizers and the startup guard, with
 *   this stack's verdicts, [RestClientVerdicts], handed in directly) - so a host gets the core wiring without
 *   knowing the core package exists;
 * - this module's instance of the core's [BlockingResilienceDecorator], the [ResilientRestClientFactory] and the
 *   [RestClientObservationReport].
 *
 * The decorator and the factory back off for a host's own bean of the same type. A host with the reactive client on
 * its classpath as well excludes one of the two auto-configurations: the two stacks cannot share a context yet
 * (ADR-0003, "each mesh endpoint is owned by one client stack").
 */
@AutoConfiguration
@EnableConfigurationProperties(RestClientProperties::class)
@Import(MeshResilienceConfiguration::class, RestClientMeshRegistrar::class)
class VerediumRestClientAutoConfiguration {
    /**
     * This module's [BlockingResilienceDecorator]: over the host's three Resilience4j registries, with the host's
     * [ObservationRegistry] when there is one (absent, the exchange's observation starts as its own root), and with
     * workers named [WORKER_NAME_PREFIX]`<n>` - the thread name on every log line written during an exchange, which
     * is how a reader tells this module's calls from a host's own decorated calls in the same JVM. The decorator
     * carries no Spring annotation of its own - it is a plain component of `veredium-core`, usable without any
     * client - so the bean is declared here.
     */
    @Bean
    @ConditionalOnMissingBean
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

    /**
     * The [ResilientRestClientFactory] over Boot's `RestClient.Builder` - required, so a host without one fails the
     * start naming it (see the factory's constructor) - and the optional Bean Validation provider and meter registry.
     */
    @Bean
    @ConditionalOnMissingBean
    fun resilientRestClientFactory(
        properties: RestClientProperties,
        meshProperties: MeshProperties,
        resilience: BlockingResilienceDecorator,
        objectMapper: ObjectMapper,
        validatorFactory: ObjectProvider<ValidatorFactory>,
        meterRegistry: ObjectProvider<MeterRegistry>,
        restClientBuilder: RestClient.Builder,
    ): ResilientRestClientFactory =
        ResilientRestClientFactory(
            properties,
            meshProperties,
            resilience,
            objectMapper,
            validatorFactory = validatorFactory.ifAvailable,
            meterRegistry = meterRegistry.ifAvailable,
            restClientBuilder = restClientBuilder,
        )

    /** The startup lines on what `app.restclient.observability.http-observation-enabled` achieves in this host. */
    @Bean
    fun restClientObservationReport(
        properties: RestClientProperties,
        environment: Environment,
        observationRegistry: ObjectProvider<ObservationRegistry>,
    ): RestClientObservationReport = RestClientObservationReport(properties, environment, observationRegistry.ifAvailable)

    companion object {
        /** The name prefix of this module's worker threads, as thread dumps and the exchange event's `thread` show it. */
        const val WORKER_NAME_PREFIX: String = "restclient-call-"
    }
}

/** The shared mesh wiring of `veredium-core`, registered with this stack's verdicts. */
class RestClientMeshRegistrar : MeshResilienceRegistrar(RestClientVerdicts.breakerVerdicts)
