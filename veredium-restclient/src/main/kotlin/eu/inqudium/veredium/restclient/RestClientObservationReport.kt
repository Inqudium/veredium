package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.observation.HttpObservationReport
import io.micrometer.observation.ObservationRegistry
import org.springframework.core.env.Environment

/**
 * The blocking stack's instance of the core's [HttpObservationReport]: the two startup lines that say what
 * [RestClientProperties.Observability.httpObservationEnabled] ACHIEVES in the host at hand - Boot's
 * `ObservationRestClientCustomizer` puts the host's registry on the builder, this only reads whether there was
 * one. Lines are written under this class's logger name.
 */
class RestClientObservationReport(
    properties: RestClientProperties,
    // Nullable ON PURPOSE: an optional dependency, so tests can construct the report directly; the Spring
    // context always injects it. Read ONLY for the two central disable-filter values.
    environment: Environment? = null,
    // Nullable like environment: read ONLY to make the toggle line truthful.
    observationRegistry: ObservationRegistry? = null,
) : HttpObservationReport(
        stackName = "RestClient",
        toggleProperty = TOGGLE_PROPERTY,
        observationEnabled = properties.observability.httpObservationEnabled,
        environment = environment,
        observationRegistry = observationRegistry,
    ) {
    companion object {
        /** The toggle's key, named verbatim on the startup line. */
        const val TOGGLE_PROPERTY: String = "app.restclient.observability.http-observation-enabled"
    }
}
