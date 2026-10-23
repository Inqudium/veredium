package eu.inqudium.veredium.core.client

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import eu.inqudium.veredium.core.validation.ResponseValidation
import eu.inqudium.veredium.core.validation.ResponseValidationProperties
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import jakarta.validation.ValidatorFactory
import org.slf4j.Logger
import java.time.Duration

/**
 * What both client stacks' factories derive from `app.mesh` and their own `validation` block, the same way on both:
 * the endpoints by alias, the drain window of the shutdown, the default headers of a backend's client, its
 * response-size meter and the response validation - each stack's factory adds only its builder, its engines and its
 * client.
 *
 * @param stack The stack's name (`restclient`, `webclient`): the plan covers the endpoints that stack owns
 *   ([MeshProperties.endpointsOwnedBy]) and nothing else.
 * @param propertiesPrefix The stack's properties prefix (`app.restclient`, `app.webclient`), named in the meter's
 *   description.
 * @param logger The factory's logger, which the validation startup line goes to.
 */
class MeshClientPlan(
    meshProperties: MeshProperties,
    private val stack: String,
    private val propertiesPrefix: String,
    validationProperties: ResponseValidationProperties,
    validatorFactory: ValidatorFactory?,
    logger: Logger,
) {
    /** Which backends validate, with which validator, and whether a violation fails the call. */
    val validation: ResponseValidation = ResponseValidation(validationProperties, validatorFactory)

    /**
     * The mesh-derived ladder of every backend this stack owns, keyed by the endpoint alias (which must equal the
     * backend name).
     */
    val endpoints: Map<String, ResolvedTimeouts> = meshProperties.endpointsOwnedBy(stack)

    /** The endpoints another stack owns, as alias -> owning stack - named where this stack refuses them. */
    val foreignEndpoints: Map<String, String> = meshProperties.endpointsOwnedByOthers(stack)

    /** Global mesh transport caps (connect / pending-acquire / max-connections) shared by the engines and clients. */
    val transport: MeshProperties.Transport = meshProperties.defaults.transport

    /**
     * How long a factory's shutdown waits for its engines to drain: the LONGEST response timeout any endpoint permits,
     * plus the pre-response phases (connect + pending-acquire) and a second of slack.
     *
     * Derived rather than chosen, because the quantity it must exceed is knowable: no in-flight call can outlive its
     * own `responseTimeout`, so waiting beyond the largest of them plus the phases that precede it cannot let anything
     * finish that was not going to finish anyway. A literal would be a second, silent timeout next to the mesh ladder -
     * free to drift below the ladder as endpoints are retuned, and nothing would say so. One window for all engines,
     * not one each: the drains run one after another against the same deadline.
     */
    val drainWindow: Duration =
        (endpoints.values.maxOfOrNull { it.responseTimeout } ?: Duration.ZERO)
            .plusMillis(transport.connectTimeoutMs + transport.pendingAcquireTimeoutMs)
            .plusSeconds(1)

    init {
        // Which backends actually validate, resolved the same way the clients resolve it. Worth a startup line because
        // both ways this silently does nothing are invisible at runtime: a backend simply not listed, or no Bean
        // Validation provider on the classpath at all, in which case no configuration can switch it on for anyone.
        logger.info(
            "Response Bean Validation: default={}, validated endpoints={} (provider {})",
            validation.enabledByDefault,
            validation.validatedAliases(endpoints.keys),
            validation.providerVerdict,
        )
    }

    /**
     * The mesh endpoint of [backendName].
     *
     * @throws IllegalArgumentException if the endpoint belongs to another stack - its client is that stack's to build.
     * @throws IllegalStateException if no mesh endpoint with that alias is configured - failing fast at startup beats a
     *   confusing 404 or connection error at the first call.
     */
    fun endpoint(backendName: String): ResolvedTimeouts {
        foreignEndpoints[backendName]?.let { owner ->
            throw IllegalArgumentException(
                "Backend '$backendName' is owned by the $owner stack (app.mesh.endpoints[].stack=$owner), not by the $stack " +
                    "stack: build its client with the $owner stack's factory.",
            )
        }
        return endpoints[backendName]
            ?: error("No mesh endpoint configured for backend '$backendName'. Add an app.mesh.endpoints[] entry whose alias equals '$backendName'.")
    }

    /**
     * The default headers of [endpoint]'s client, in the order they are set: the `Host` the sidecar routes on (from the
     * triplet), the `User-Agent` [userAgent], the endpoint's own static `headers` (which can replace either), and
     * `Accept: application/json` last.
     */
    fun defaultHeaders(
        endpoint: ResolvedTimeouts,
        userAgent: String,
    ): List<Pair<String, String>> =
        buildList {
            add(HOST to endpoint.host)
            add(USER_AGENT to userAgent)
            endpoint.headers.forEach { (name, value) -> add(name to value) }
            add(ACCEPT to APPLICATION_JSON)
        }

    /**
     * The response-size summary [meterName] of [backendName] in [registry] ([ResponseBodyMetrics] for its unit and
     * tag), or `null` without a registry - the client then records nothing.
     *
     * **What it is for.** The stack's `max-in-memory-size` is a ceiling that has to be chosen, and without this meter
     * there is nothing to choose it from. legatium's body capture is bounded by its own `max-body-bytes` and says
     * nothing about this limit; the limit's exception says only that the limit was passed.
     *
     * **What it cannot tell you.** Only responses that FIT are sampled - the ones that motivated the question are
     * absent by construction. Read it as the headroom of the population that works, and pair it with the limit named
     * on the failure line for the ones that do not.
     *
     * Resolved ONCE per backend, when its client is built, rather than looked up per call, the same way the validator
     * is: the hot path should hold no registry lookup and no toggle it could disagree with.
     */
    fun responseBodySize(
        registry: MeterRegistry?,
        meterName: String,
        backendName: String,
    ): DistributionSummary? =
        registry?.let {
            DistributionSummary
                .builder(meterName)
                .description("Decoded response body size in UTF-8 bytes, for sizing $propertiesPrefix.max-in-memory-size")
                .baseUnit(ResponseBodyMetrics.BASE_UNIT)
                .tag(ResponseBodyMetrics.ADAPTER_TAG, backendName)
                .register(it)
        }

    private companion object {
        const val HOST = "Host"
        const val USER_AGENT = "User-Agent"
        const val ACCEPT = "Accept"
        const val APPLICATION_JSON = "application/json"
    }
}
