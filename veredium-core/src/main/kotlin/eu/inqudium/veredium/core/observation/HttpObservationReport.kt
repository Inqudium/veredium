package eu.inqudium.veredium.core.observation

import io.micrometer.observation.ObservationRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.core.env.Environment

/**
 * Writes, once at startup, what a client stack's HTTP observation toggle ACHIEVES - as opposed to what it asks
 * for.
 *
 * Three switches can silence the `http.client.requests` observation, all silently and all in the same
 * direction (no span, no timer, no `traceparent`, nothing in the log to say why): the stack's own toggle
 * ([toggleProperty], `app.<stack>.observability.http-observation-enabled`); Spring's central disable-filter
 * `management.observations.enable.<name>` (a name-prefixed DISABLE with default `true`, not an enable switch);
 * and a host without a *recording* `ObservationRegistry` bean. Reporting a toggle that is on as a plain "on"
 * when one of the other two makes it moot is what sends someone hunting through Micrometer configuration for a
 * span the process was never able to create. This class exists so the two startup lines say which case it is;
 * it wires nothing - each stack's factory does the wiring, this only reads what it will find.
 *
 * One implementation for both stacks: the three switches and the four verdicts are the same whether Boot's
 * `ObservationRestClientCustomizer` or its `ObservationWebClientCustomizer` put the registry on the builder.
 * Each stack subclasses it with its name and its property key, which is also what gives the lines their logger
 * name.
 *
 * @param stackName The name the toggle line opens with (`RestClient`, `WebClient`).
 * @param toggleProperty The stack's toggle key, named verbatim on the line so an operator can find it.
 * @param observationEnabled The toggle's bound value.
 * @param environment Read ONLY for the two central disable-filter values; `null` reads as their default `true`.
 * @param observationRegistry Read ONLY to make the toggle line truthful. Assumption: Boot puts the host's registry
 *   on the builder when a bean exists; this parameter tells whether there WAS one, and whether it records anything.
 */
open class HttpObservationReport(
    stackName: String,
    toggleProperty: String,
    observationEnabled: Boolean,
    environment: Environment? = null,
    observationRegistry: ObservationRegistry? = null,
) {
    // The subclass's logger, so each stack's lines carry its own logger name - what a logging configuration and
    // a test capturing the lines address.
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    /**
     * Whether the host has an ObservationRegistry that will actually RECORD something.
     *
     * Deliberately `isNoop` rather than a null check alone. Assumption: Micrometer (1.17.1) defines it as more
     * than identity - `SimpleObservationRegistry.isNoop()` is `this == NOOP || observationConfig().getObservationHandlers()
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
     * `http.client.requests` just the client observation. Assumption: when present, Spring Boot applies it through
     * the registry's filter predicate, so a `false` here suppresses the observation even though the stack's toggle
     * is on.
     */
    private val centralEnableAll: Boolean =
        environment?.getProperty(CENTRAL_ENABLE_ALL, Boolean::class.java, true) ?: true
    private val centralEnableHttpClient: Boolean =
        environment?.getProperty(CENTRAL_ENABLE_HTTP_CLIENT, Boolean::class.java, true) ?: true

    init {
        logger.info(
            "{} HTTP observation module toggle {}={} -> {}",
            stackName,
            toggleProperty,
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

    companion object {
        /** Spring's client observation name, for both stacks - the target of the central disable-filter. */
        const val HTTP_CLIENT_OBSERVATION: String = "http.client.requests"

        /** Central Micrometer disable-filter key for every observation (default true; false = OFF). */
        const val CENTRAL_ENABLE_ALL: String = "management.observations.enable.all"

        /** Central Micrometer disable-filter key for [HTTP_CLIENT_OBSERVATION] (default true; false = OFF). */
        const val CENTRAL_ENABLE_HTTP_CLIENT: String = "management.observations.enable.$HTTP_CLIENT_OBSERVATION"
    }
}
