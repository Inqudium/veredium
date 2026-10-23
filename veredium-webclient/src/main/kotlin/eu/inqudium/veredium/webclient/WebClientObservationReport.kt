package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.observation.HttpObservationReport
import io.micrometer.observation.ObservationRegistry
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component

/**
 * The reactive stack's instance of the core's [HttpObservationReport]: the two startup lines that say what
 * [WebClientProperties.Observability.httpObservationEnabled] ACHIEVES in the host at hand - Boot's
 * `ObservationWebClientCustomizer` puts the host's registry on the builder, this only reads whether there was
 * one. Lines are written under this class's logger name.
 *
 * The JVM-global Reactor context-propagation hook this observation setup depends on is NOT flipped here or in
 * the factory - it lives in [WebClientReactorContextPropagationConfiguration], so a process-wide switch is not a side
 * effect of constructing either.
 */
@Component
class WebClientObservationReport(
    properties: WebClientProperties,
    // Nullable ON PURPOSE: an optional dependency, so tests can construct the report directly; the Spring
    // context always injects it. Read ONLY for the two central disable-filter values.
    environment: Environment? = null,
    // Nullable like environment: read ONLY to make the toggle line truthful.
    observationRegistry: ObservationRegistry? = null,
) : HttpObservationReport(
        stackName = "WebClient",
        toggleProperty = TOGGLE_PROPERTY,
        observationEnabled = properties.observability.httpObservationEnabled,
        environment = environment,
        observationRegistry = observationRegistry,
    ) {
    companion object {
        /** The toggle's key, named verbatim on the startup line. */
        const val TOGGLE_PROPERTY: String = "app.webclient.observability.http-observation-enabled"
    }
}
