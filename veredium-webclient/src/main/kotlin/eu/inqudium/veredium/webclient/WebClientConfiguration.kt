package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.mesh.MeshResilienceConfiguration
import eu.inqudium.veredium.core.resilience.MeshResilienceRegistrar
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import reactor.core.publisher.Hooks

/**
 * How this module wires itself into a host - the ONE configuration class of the reactive client, and the one a
 * host imports when it does not scan the package:
 *
 * - the `app.webclient` binding ([WebClientProperties]);
 * - the shared mesh wiring of `veredium-core`: [MeshResilienceConfiguration] (the `app.mesh` binding, the ladder
 *   logger) and [WebClientMeshRegistrar] (the per-alias Resilience4j customizers and the startup guard, with
 *   this stack's verdicts, [WebClientVerdicts], handed in directly) - so a host that scans this module's package
 *   gets the core wiring too, without knowing the core package exists.
 *
 * The scanned `@Component`s of this module - [ResilientWebClientFactory], [ReactiveResilienceDecorator],
 * [WebClientObservationReport] - and the `@Configuration` [WebClientReactorContextPropagationConfiguration] are
 * deliberately not declared here as well: they are components because they have dependencies the context
 * resolves (or, for the hook, a process-wide side effect a host may want to leave out), and listing them twice
 * would create them twice.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(WebClientProperties::class)
@Import(MeshResilienceConfiguration::class, WebClientMeshRegistrar::class)
class WebClientConfiguration

/** The shared mesh wiring of `veredium-core`, registered with this stack's verdicts. */
class WebClientMeshRegistrar : MeshResilienceRegistrar(WebClientVerdicts.breakerVerdicts)

/**
 * Owns the ONE process-wide switch this module makes: Reactor's automatic context propagation.
 *
 * The observation's "current observation" is a thread-local, and WebFlux hops Reactor threads constantly, so at
 * the client-call injection point the thread-local would otherwise be empty. Enabling the hook makes Reactor
 * capture and restore the registered `ThreadLocalAccessor`s (the `ObservationThreadLocalAccessor`, auto-loaded
 * via `ServiceLoader` once `io.micrometer:context-propagation` is present) across operator boundaries - so the
 * `http.client.requests` client span nests on the active trace instead of starting a fresh root.
 *
 * It lives in its own `@Configuration` rather than in [ResilientWebClientFactory]'s constructor because its
 * effect is JVM-WIDE and cannot be undone: it must be a deliberate, declared piece of wiring, not a side effect
 * of instantiating a factory (which a test may do repeatedly, and which then silently changes the behaviour of
 * every later context in the same JVM). Gated on the module toggle, so a host that did not opt in gets no
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
