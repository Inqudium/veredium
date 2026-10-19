package eu.inqudium.veredium.restclient

import io.micrometer.observation.ObservationRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component

/**
 * Writes, once at startup, what the module's HTTP observation toggle ACHIEVES - as opposed to what it asks for.
 *
 * Three switches can silence the `http.client.requests` observation, all silently and all in the same
 * direction (no span, no timer, no `traceparent`, nothing in the log to say why): the module toggle
 * [RestClientProperties.Observability.httpObservationEnabled]; Spring's central disable-filter
 * `management.observations.enable.<name>` (a name-prefixed DISABLE with default `true`, not an enable switch);
 * and a host without a *recording* `ObservationRegistry` bean. Reporting a toggle that is on as a plain "on"
 * when one of the other two makes it moot is what sends someone hunting through Micrometer configuration for a
 * span the process was never able to create. This component exists so the two startup lines say which case it
 * is; it wires nothing - [ResilientRestClientFactory] does the wiring, this only reads what it will find.
 */
@Component
class RestClientObservationReport(
    properties: RestClientProperties,
    // Nullable ON PURPOSE: an optional dependency, so tests can construct the report directly; the Spring
    // context always injects it. Read ONLY for the two central disable-filter values.
    environment: Environment? = null,
    // Nullable like environment: read ONLY to make the toggle line truthful. Boot's ObservationRestClientCustomizer
    // puts the host's registry on the builder when a bean exists; this parameter tells whether there WAS one,
    // and whether it records anything.
    observationRegistry: ObservationRegistry? = null,
) {
    private val logger: Logger = LoggerFactory.getLogger(RestClientObservationReport::class.java)

    private val observationEnabled: Boolean = properties.observability.httpObservationEnabled

    /**
     * Whether the host has an ObservationRegistry that will actually RECORD something.
     *
     * Deliberately `isNoop` rather than a null check alone, because Micrometer defines it as more than identity:
     * `SimpleObservationRegistry.isNoop()` is `this == NOOP || observationConfig().getObservationHandlers()
     * .isEmpty()`. A registry bean with no `ObservationHandler` is therefore reported as noop - correctly, since
     * it records nothing - and a bean-presence check would have called that case "wired" and been wrong.
     */
    private val hostObservationRegistryActive: Boolean = observationRegistry?.isNoop == false

    /**
     * The text of the toggle line. The `on` case is split deliberately: a toggle that is on without a recording
     * registry behind it is indistinguishable at runtime from one that is off.
     */
    private val observationVerdict: String =
        when {
            !observationEnabled -> {
                "off (registry replaced by NOOP)"
            }

            hostObservationRegistryActive -> {
                "on (Boot's registry stays on the builder)"
            }

            observationRegistry == null -> {
                "on, but the host has NO ObservationRegistry bean - nothing is observed " +
                    "(no span, no timer, no traceparent)"
            }

            else -> {
                "on, but the host's ObservationRegistry records nothing - it is the NOOP or has no " +
                    "ObservationHandler (no span, no timer, no traceparent)"
            }
        }

    /**
     * The central Micrometer observation DEACTIVATION filter: `management.observations.enable.<name>` (default
     * true) is the ONLY central Spring switch and a name-prefixed DISABLE - `all` scopes everything,
     * `http.client.requests` just the client observation. When present, Spring applies it through the registry's
     * filter predicate, so a `false` here suppresses the observation even though the module toggle is on.
     */
    private val centralEnableAll: Boolean =
        environment?.getProperty(CENTRAL_ENABLE_ALL, Boolean::class.java, true) ?: true
    private val centralEnableHttpClient: Boolean =
        environment?.getProperty(CENTRAL_ENABLE_HTTP_CLIENT, Boolean::class.java, true) ?: true

    init {
        logger.info(
            "RestClient HTTP observation module toggle app.restclient.observability.http-observation-enabled={} -> {}",
            observationEnabled,
            observationVerdict,
        )
        val suppressedCentrally = observationEnabled && !(centralEnableAll && centralEnableHttpClient)
        logger.info(
            "Central Micrometer observation disable-filter (default true, false = OFF): {}={}, {}={}{}",
            CENTRAL_ENABLE_ALL,
            centralEnableAll,
            CENTRAL_ENABLE_HTTP_CLIENT,
            centralEnableHttpClient,
            if (suppressedCentrally) {
                " -> '$HTTP_CLIENT_OBSERVATION' is SUPPRESSED centrally despite the module toggle being on"
            } else {
                ""
            },
        )
    }

    private companion object {
        /** Spring's RestClient client observation name - the target of the central disable-filter. */
        const val HTTP_CLIENT_OBSERVATION = "http.client.requests"

        /** Central Micrometer disable-filter keys (default true; false = OFF). */
        const val CENTRAL_ENABLE_ALL = "management.observations.enable.all"
        const val CENTRAL_ENABLE_HTTP_CLIENT = "management.observations.enable.$HTTP_CLIENT_OBSERVATION"
    }
}
