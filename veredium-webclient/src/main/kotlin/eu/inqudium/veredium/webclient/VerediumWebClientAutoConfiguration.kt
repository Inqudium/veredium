package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.MeshResilienceConfiguration
import eu.inqudium.veredium.core.resilience.MeshResilienceRegistrar
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.observation.ObservationRegistry
import jakarta.validation.ValidatorFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.core.env.Environment
import org.springframework.core.io.ResourceLoader
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Hooks
import tools.jackson.databind.ObjectMapper

/**
 * How this module wires itself into a host: a Spring Boot auto-configuration, registered in
 * `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`, so the dependency is all a
 * host adds - no package to scan, no class to import.
 *
 * - the `app.webclient` binding ([WebClientProperties]);
 * - the shared mesh wiring of `veredium-core`: [MeshResilienceConfiguration] (the `app.mesh` binding, the ladder
 *   logger) and [WebClientMeshRegistrar] (the per-alias Resilience4j customizers and the startup guard, with
 *   this stack's verdicts, [WebClientVerdicts], handed in directly) - so a host gets the core wiring without
 *   knowing the core package exists;
 * - [WebClientReactorContextPropagationConfiguration], the one process-wide switch, gated on the module toggle;
 * - the [ReactiveResilienceDecorator], the [ResilientWebClientFactory] and the [WebClientObservationReport].
 *
 * The decorator and the factory back off for a host's own bean of the same type. A host with the blocking client on
 * its classpath as well runs both stacks in one context; each mesh endpoint then names its stack in
 * `app.mesh.endpoints[].stack`, and the core's ownership check fails the start for one that does not (ADR-0003, "each
 * mesh endpoint is owned by one client stack").
 */
@AutoConfiguration
@EnableConfigurationProperties(WebClientProperties::class)
@Import(MeshResilienceConfiguration::class, WebClientMeshRegistrar::class, WebClientReactorContextPropagationConfiguration::class)
class VerediumWebClientAutoConfiguration {
    /** The [ReactiveResilienceDecorator] over the host's three Resilience4j registries. */
    @Bean
    @ConditionalOnMissingBean
    fun reactiveResilienceDecorator(
        circuitBreakerRegistry: CircuitBreakerRegistry,
        timeLimiterRegistry: TimeLimiterRegistry,
        bulkheadRegistry: BulkheadRegistry,
    ): ReactiveResilienceDecorator = ReactiveResilienceDecorator(circuitBreakerRegistry, timeLimiterRegistry, bulkheadRegistry)

    /**
     * The [ResilientWebClientFactory] over Boot's `WebClient.Builder` - required, so a host without one fails the
     * start naming it (see the factory's constructor) - the optional Bean Validation provider and meter registry, and
     * the context's class loader for the engine-library checks.
     */
    @Bean
    @ConditionalOnMissingBean
    fun resilientWebClientFactory(
        properties: WebClientProperties,
        meshProperties: MeshProperties,
        resilience: ReactiveResilienceDecorator,
        objectMapper: ObjectMapper,
        validatorFactory: ObjectProvider<ValidatorFactory>,
        meterRegistry: ObjectProvider<MeterRegistry>,
        webClientBuilder: WebClient.Builder,
        resourceLoader: ResourceLoader,
    ): ResilientWebClientFactory =
        ResilientWebClientFactory(
            properties,
            meshProperties,
            resilience,
            objectMapper,
            validatorFactory = validatorFactory.ifAvailable,
            meterRegistry = meterRegistry.ifAvailable,
            webClientBuilder = webClientBuilder,
            resourceLoader = resourceLoader,
        )

    /** The startup lines on what `app.webclient.observability.http-observation-enabled` achieves in this host. */
    @Bean
    fun webClientObservationReport(
        properties: WebClientProperties,
        environment: Environment,
        observationRegistry: ObjectProvider<ObservationRegistry>,
    ): WebClientObservationReport = WebClientObservationReport(properties, environment, observationRegistry.ifAvailable)
}

/** The shared mesh wiring of `veredium-core`, registered under this stack's name with this stack's verdicts. */
class WebClientMeshRegistrar : MeshResilienceRegistrar(STACK, WebClientVerdicts.breakerVerdicts) {
    companion object {
        /**
         * This stack's name: the value of `app.mesh.endpoints[].stack` that assigns an endpoint to it, and the stack
         * segment of its mesh bean names. Needed in the configuration only when a host depends on both client modules.
         */
        const val STACK: String = "webclient"
    }
}

/**
 * Owns the ONE process-wide switch this module makes: Reactor's automatic context propagation.
 *
 * The observation's "current observation" is a thread-local, and WebFlux hops Reactor threads constantly, so at
 * the client-call injection point the thread-local would otherwise be empty. Enabling the hook makes Reactor
 * capture and restore the registered `ThreadLocalAccessor`s (the `ObservationThreadLocalAccessor`, auto-loaded
 * via `ServiceLoader` once `io.micrometer:context-propagation` is present) across operator boundaries - so the
 * `http.client.requests` client span nests on the active trace instead of starting a fresh root.
 *
 * It lives in its own `@Configuration`, imported by [VerediumWebClientAutoConfiguration], rather than in
 * [ResilientWebClientFactory]'s constructor because its effect is JVM-WIDE and cannot be undone: it must be a
 * deliberate, declared piece of wiring, not a side effect of instantiating a factory (which a test may do
 * repeatedly, and which then silently changes the behaviour of every later context in the same JVM). Gated on the module toggle, so a host that did not opt in gets no
 * global change at all.
 */
@Configuration(proxyBeanMethods = false)
class WebClientReactorContextPropagationConfiguration(
    properties: WebClientProperties,
) {
    private val logger: Logger = LoggerFactory.getLogger(WebClientReactorContextPropagationConfiguration::class.java)

    init {
        if (properties.observability.httpObservationEnabled) {
            // Idempotent: repeated calls are a no-op, so several contexts in one JVM are harmless.
            Hooks.enableAutomaticContextPropagation()
            logger.info("Reactor automatic context propagation ENABLED (JVM-wide) - HTTP observation is on")
        } else {
            logger.debug("Reactor automatic context propagation NOT enabled - HTTP observation is off")
        }
    }
}
