package eu.inqudium.veredium.webclient

import eu.inqudium.legatium.webclient.logging.ClientRequestLoggingFilter
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.observation.ObservationRegistry
import io.netty.channel.ChannelOption
import jakarta.annotation.PreDestroy
import jakarta.validation.MessageInterpolator
import jakarta.validation.Validator
import jakarta.validation.ValidatorFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeoutException

/**
 * Produces fully assembled [ResilientWebClient]s - one per mesh endpoint - bundling the Reactor-Netty
 * transport (one shared pool, per-target timeouts, base URL, JSON headers, bounded codec) with the
 * [ReactiveResilienceDecorator] pipeline. The backend name is reused as the Resilience4j instance name, so
 * `product-agreements` needs matching `resilience4j.*.instances.product-agreements` config.
 *
 * Exchange logging is not assembled here: legatium-webclient-logging's auto-configuration puts its filter on
 * Boot's `WebClient.Builder` bean as a `WebClientCustomizer`, and every client below is a clone of that bean.
 * The factory's only contribution is the request attribute the library reads the client's name from.
 */
@Component
class ResilientWebClientFactory(
    private val properties: WebClientProperties,
    private val meshProperties: MeshProperties,
    private val resilience: ReactiveResilienceDecorator,
    private val objectMapper: ObjectMapper,
    // Nullable ON PURPOSE: an optional dependency, so tests can construct the factory directly; the Spring
    // context always injects it. Read ONLY to log the central Micrometer disable-filter values at startup (see
    // the init block).
    environment: Environment? = null,
    // Nullable like environment: Spring injects the auto-configured factory when a Bean Validation
    // provider is on the classpath. Absent -> validation cannot run for ANY backend, whatever the properties
    // say, which the init block reports rather than leaving it to be discovered.
    //
    // The FACTORY rather than a ready-made Validator, because a Validator's message interpolator is fixed at
    // construction while `usingContext()` is the standard way to derive one that differs - here, one pinned to
    // English (see [validator]). Deriving keeps that choice inside this module: the host's own Validator bean,
    // which its controllers use, is left exactly as it configured it.
    validatorFactory: ValidatorFactory? = null,
    // Nullable like the two above: an optional dependency, so a direct (test) construction and a minimal
    // context both work without one. Absent -> the response-size summary is simply not registered and the
    // per-call path records nothing; nothing else about the client changes.
    private val meterRegistry: MeterRegistry? = null,
    // Nullable like the three above, and like `environment` it is read ONLY to make the startup line below
    // truthful - the factory does NOT wire it. Boot's ObservationWebClientCustomizer has already put the
    // host's registry on the builder; this parameter exists so the factory can tell whether there WAS one.
    //
    // Without it, the toggle line could only report what the operator ASKED for, never what they will GET:
    // `http-observation-enabled=true` in a host with no ObservationRegistry bean produces no span, no timer
    // and no traceparent, and the line still read "on". Absent or NOOP is the normal, correct state for a
    // host that wants no observation - it is only worth saying out loud when the toggle claims otherwise.
    observationRegistry: ObservationRegistry? = null,
    // Boot's auto-configured builder (prototype scope: this factory owns its instance and clones it per
    // backend). Every WebClientCustomizer the host declares has already been applied to it - Boot's own
    // (observation registry + convention, codecs, connector) and the host's.
    //
    // REQUIRED on purpose, unlike the three optional dependencies above: those are optional because the factory
    // works sensibly without them. Without this one it would work DIFFERENTLY - a bare builder from the static
    // factory carries no host customizer and no observation registry, so the http.client.requests span would vanish
    // without a log line. Spring treats a Kotlin default as an optional dependency and would take exactly that
    // silent path when the bean is absent; a required parameter makes a missing bean a context-start failure
    // that names WebClient.Builder. A host that excludes WebClientAutoConfiguration declares its own
    // WebClient.Builder bean. Tests that construct the factory directly pass a bare builder and say so at the
    // call site.
    webClientBuilder: WebClient.Builder,
) {
    private val logger: Logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java)

    /**
     * The validator the clients use: the host's, but with its messages pinned to [Locale.ENGLISH].
     *
     * Constraint messages are interpolated against `Locale.getDefault()`, so the text of a violation otherwise
     * depends on the locale of the machine the process happens to run on - the same defect would read
     * "size must be between 1 and ..." on one host and "Groesse muss zwischen 1 und ... sein" on the next.
     * That belongs nowhere near a log line: this codebase is English throughout, and log consumers, alert
     * rules and anyone searching for a message have to be able to rely on its wording.
     */
    private val validator: Validator? =
        validatorFactory?.let { factory ->
            factory
                .usingContext()
                .messageInterpolator(EnglishMessageInterpolator(factory.messageInterpolator))
                .validator
        }

    /** Standalone HTTP observation toggle (see [WebClientProperties.Observability]) - no sync-bridge coupling. */
    private val observationEnabled: Boolean = properties.observability.httpObservationEnabled

    /**
     * Whether the host has an ObservationRegistry that will actually RECORD something.
     *
     * Deliberately `isNoop` rather than a null check alone, because Micrometer defines it as more than identity:
     * `SimpleObservationRegistry.isNoop()` is `this == NOOP || observationConfig().getObservationHandlers()
     * .isEmpty()`. A registry bean with no `ObservationHandler` is therefore reported as noop - correctly, since
     * it records nothing - and a bean-presence check would have called that case "wired" and been wrong.
     *
     * This is the difference between what the operator CONFIGURED and what the process will DO. Boot only places
     * a registry on the builder when an `ObservationRegistry` bean exists (its `ObservationWebClientCustomizer`
     * is `@ConditionalOnBean` on one); with no bean, or with one that records nothing, every outbound call opens
     * nothing - no `http.client.requests` timer, no CLIENT span, no `traceparent` - whatever the toggle says.
     */
    private val hostObservationRegistryActive: Boolean = observationRegistry?.isNoop == false

    /**
     * What the module toggle ACHIEVES, as opposed to what it asks for - the text of the startup line below.
     *
     * The `on` case is split deliberately. A toggle that is on without a recording registry behind it is
     * indistinguishable at runtime from one that is off: no span, no timer, nothing in the log. Reporting both
     * as plain "on" is what sends someone hunting through Micrometer configuration for a span the host was never
     * able to create.
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
     * The central Micrometer observation DEACTIVATION filter, read for logging. `management.observations.enable.
     * <name>` (default true) is the ONLY central Spring switch and is a name-prefixed DISABLE, not an enable
     * switch: `all` scopes everything, `http.client.requests` scopes just the client observation. Absent -> the
     * default `true` (enabled). When present, Spring applies it through the registry's filter predicate, so a
     * `false` here suppresses the observation even though the module toggle is on.
     */
    private val centralEnableAll: Boolean =
        environment?.getProperty(CENTRAL_ENABLE_ALL, Boolean::class.java, true) ?: true
    private val centralEnableHttpClient: Boolean =
        environment?.getProperty(CENTRAL_ENABLE_HTTP_CLIENT, Boolean::class.java, true) ?: true

    /**
     * The shared base builder: Boot's, so the host's WebClientCustomizers (observation, codecs, connector, own
     * beans) are already on it and every cloned per-endpoint client inherits them. The module toggle no longer
     * WIRES the observation registry - Boot does, through its ObservationWebClientCustomizer, so each outbound
     * request opens an `http.client.requests` observation (a CLIENT span + `traceparent` injection with a host
     * tracing bridge) - it only REMOVES it when the operator switched this module's HTTP observation off.
     */
    private val baseBuilder: WebClient.Builder =
        webClientBuilder.let { if (observationEnabled) it else it.observationRegistry(ObservationRegistry.NOOP) }

    init {
        // Which backends actually validate, resolved the same way the clients resolve it. Worth a startup line
        // because both ways this silently does nothing are invisible at runtime: a backend simply not listed,
        // or - the trap the previous state was - no Bean Validation provider on the classpath at all, in which
        // case no configuration can switch it on for anyone.
        val validated =
            meshProperties
                .resolveAll()
                .values
                .map { it.alias }
                .filter { properties.validation.isEnabledFor(it) }
        logger.info(
            "Response Bean Validation: default={}, validated endpoints={} (provider {})",
            properties.validation.enabled,
            validated.sorted().ifEmpty { listOf("none") },
            if (validator != null) "present, messages pinned to English" else "MISSING - validation cannot run for any backend",
        )

        // NOTE: the JVM-global Reactor context-propagation hook this observation setup depends on is NOT flipped
        // here - it lives in [WebClientReactorContextPropagationConfig], so a process-wide switch is not a side effect of
        // constructing this factory. What remains here is diagnostics for the factory's own wiring.
        logger.info(
            "WebClient HTTP observation module toggle app.webclient.observability.http-observation-enabled={} -> {}",
            observationEnabled,
            observationVerdict,
        )
        // The ONLY central Spring switch is a DEACTIVATION filter, management.observations.enable.<name>
        // (default true) - a name-prefixed DISABLE, NOT an enable switch. Log its effective values for the
        // client observation name so an operator can spot a central override that suppresses a wired
        // observation (e.g. `...enable.all=false` or `...enable.http.client.requests=false`).
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

    /** The mesh-derived ladder per backend, keyed by the endpoint alias (which must equal the backend name). */
    private val meshByBackend: Map<String, ResolvedTimeouts> = meshProperties.resolveAll().values.associateBy { it.alias }

    /** Global mesh transport caps (connect / pending-acquire / max-connections) shared by the pool and clients. */
    private val meshTransport: MeshProperties.Transport = meshProperties.defaults.transport

    /**
     * How long [shutdown] waits for the pool to drain: the LONGEST response timeout any endpoint permits, plus
     * the pre-response phases (connect + pending-acquire) and a second of slack.
     *
     * Derived rather than chosen, because the quantity it must exceed is knowable: no in-flight call can
     * outlive its own `responseTimeout`, so waiting beyond the largest of them plus the phases that precede it
     * cannot let anything finish that was not going to finish anyway. A literal here would be a second, silent
     * timeout next to the mesh ladder - free to drift below the ladder as endpoints are retuned, and nothing
     * would say so.
     */
    private val poolDisposeTimeout: Duration =
        (meshByBackend.values.maxOfOrNull { it.responseTimeout } ?: Duration.ZERO)
            .plusMillis(meshTransport.connectTimeoutMs + meshTransport.pendingAcquireTimeoutMs)
            .plusSeconds(1)

    /**
     * `maxInMemorySize` in bytes: the cap on how much of a response body the codec will buffer. Set on every
     * client's codecs, and handed to the client so its buffer-limit line can name the number.
     */
    private val maxInMemoryBytes: Int = properties.maxInMemorySize.toBytes().toInt()

    /**
     * The single connection pool shared by every target's [HttpClient], built once and disposed in
     * [shutdown]. Reactor Netty keeps a separate sub-pool per remote address, so distinct sidecar ports
     * stay isolated. `maxConnections` / `pendingAcquireTimeout` come from the mesh transport config.
     */
    private val connectionProvider: ConnectionProvider =
        ConnectionProvider
            .builder(POOL_NAME)
            .maxConnections(meshTransport.maxConnections)
            .pendingAcquireMaxCount(properties.pool.pendingAcquireMaxCount)
            .pendingAcquireTimeout(Duration.ofMillis(meshTransport.pendingAcquireTimeoutMs))
            .maxIdleTime(properties.pool.maxIdleTime)
            .maxLifeTime(properties.pool.maxLifeTime)
            .evictInBackground(properties.pool.evictInBackground)
            .metrics(true)
            .build()

    /** Caches one client per logical name so [create] is idempotent - repeated calls return the same instance. */
    private val cache = ConcurrentHashMap<String, ResilientWebClient>()

    /**
     * Returns the resilient client for [backendName], building it on first request and caching it after.
     * Everything per-endpoint (base URL, Host header, timeouts) comes from the `app.mesh.endpoints[]` entry
     * whose alias equals [backendName].
     *
     * @throws IllegalStateException if no mesh endpoint with that alias is configured - failing fast at
     *         startup beats a confusing 404/connection error at the first call.
     */
    fun create(backendName: String): ResilientWebClient = cache.computeIfAbsent(backendName, ::build)

    private fun build(backendName: String): ResilientWebClient {
        val endpoint =
            meshByBackend[backendName]
                ?: error(
                    "No mesh endpoint configured for backend '$backendName'. " +
                        "Add an app.mesh.endpoints[] entry whose alias equals '$backendName'.",
                )

        val httpClient = buildHttpClient(endpoint)

        val webClient =
            baseBuilder
                .clone()
                .baseUrl(endpoint.baseUrl)
                .clientConnector(ReactorClientHttpConnector(httpClient))
                // Host routes the sidecar to the upstream (from the triplet); set() (not add) so re-applying
                // never accumulates values. Extra static headers are applied on top.
                .defaultHeaders { headers ->
                    headers.set(HttpHeaders.HOST, endpoint.host)
                    endpoint.headers.forEach { (name, value) -> headers.set(name, value) }
                }.defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                // The backend name as a request attribute, set ONCE per client so every request carries it.
                // An attribute, not a header: it names the client for legatium's exchange event (adapter_name
                // and the `name` tag of its body meters) and never goes on the wire. The key is the library's
                // public constant; the library's filter is already on this builder, put there by Boot's bean.
                .defaultRequest { it.attribute(ClientRequestLoggingFilter.ADAPTER_NAME_ATTRIBUTE, backendName) }
                .codecs { it.defaultCodecs().maxInMemorySize(maxInMemoryBytes) }
                .build()

        // The validator is resolved to null when validation is off for THIS backend, so the per-call path
        // carries no toggle. The decision is the global default plus a per-alias override
        // (see WebClientProperties.Validation).
        return ResilientWebClient(
            webClient,
            backendName,
            endpoint,
            resilience,
            objectMapper,
            maxInMemoryBytes,
            validator.takeIf { properties.validation.isEnabledFor(backendName) },
            properties.validation.failOnViolation,
            responseBodySize(backendName),
        )
    }

    /**
     * The response-size summary for one backend, or `null` when the host wired no [MeterRegistry].
     *
     * **What it is for.** [WebClientProperties.maxInMemorySize] is a ceiling that has to be chosen, and until
     * this meter existed there was nothing to choose it from. legatium's body capture is bounded by its own
     * `max-body-bytes` and says nothing about the codec's limit; the `DataBufferLimitException` says only that
     * the limit was passed, not by how much.
     *
     * **What it cannot tell you.** Only responses that FIT are sampled - the ones that motivated the question
     * are absent by construction. Read it as the headroom of the population that works, and pair it with the
     * limit named on the failure line for the ones that do not.
     *
     * Resolved ONCE per backend here rather than looked up per call, the same way [validator] is: the hot path
     * should hold no registry lookup and no toggle it could disagree with.
     */
    private fun responseBodySize(backendName: String): DistributionSummary? =
        meterRegistry?.let { registry ->
            DistributionSummary
                .builder(WebClientMetrics.RESPONSE_BODY_SIZE)
                .description("Decoded response body size in UTF-8 bytes, for sizing app.webclient.max-in-memory-size")
                .baseUnit(WebClientMetrics.RESPONSE_BODY_BASE_UNIT)
                .tag(WebClientMetrics.ADAPTER_TAG, backendName)
                .register(registry)
        }

    private fun buildHttpClient(endpoint: ResolvedTimeouts): HttpClient {
        // Scoped to THIS endpoint's template - see WebClientMetrics.uriTagger for why it is not the full set.
        val uriTag = WebClientMetrics.uriTagger(endpoint.pathTemplate)
        // Response and connect timeouts are the mesh-derived values. The per-endpoint responseTimeout is
        // tighter than that endpoint's TimeLimiter budget by construction, so it never drifts past it; its
        // expiry surfaces as a WebClientRequestException the circuit breaker records.
        return HttpClient
            .create(connectionProvider)
            .option(
                ChannelOption.CONNECT_TIMEOUT_MILLIS,
                meshTransport.connectTimeoutMs.toInt(),
            ).responseTimeout(endpoint.responseTimeout)
            .compress(true)
            .keepAlive(true)
            // Pool metrics come from the shared ConnectionProvider; these add per-request transport
            // metrics. The URI tag is templated centrally so a path variable cannot leak cardinality.
            .metrics(true) { uri -> uriTag(uri) }
    }

    /**
     * Drains the shared pool on context shutdown, waiting up to [poolDisposeTimeout] for in-flight connections
     * to close.
     *
     * Expiry is an OUTCOME, not a failure. `block(Duration)` would report it as an `IllegalStateException` -
     * the same shape as a genuine disposal error, and one that Spring can only catch and log as a destroy
     * method that threw. The last line before the process exits would then be a stack trace, at the moment
     * someone is looking for one, saying nothing about what actually happened. `timeout` + `onErrorResume`
     * turns it into the statement it is: the pool did not drain in time, and it is being dropped anyway.
     *
     * The bare `block()` afterwards cannot hang - [poolDisposeTimeout] guarantees a terminal signal.
     */
    @PreDestroy
    fun shutdown() {
        connectionProvider
            .disposeLater()
            .timeout(poolDisposeTimeout)
            .onErrorResume(TimeoutException::class.java) {
                logger.warn(
                    "Connection pool '{}' did not drain within {} - dropping it with connections still open",
                    POOL_NAME,
                    poolDisposeTimeout,
                )
                Mono.empty()
            }.block()
    }

    private companion object {
        /** Name of the single shared Reactor-Netty pool; also the `name` tag on its pool metrics. */
        const val POOL_NAME = "lap-webclient"

        /** Spring's WebClient client observation name - the target of the central disable-filter below. */
        const val HTTP_CLIENT_OBSERVATION = "http.client.requests"

        /** Central Micrometer disable-filter keys (default true; false = OFF). See [centralEnableAll]. */
        const val CENTRAL_ENABLE_ALL = "management.observations.enable.all"
        const val CENTRAL_ENABLE_HTTP_CLIENT = "management.observations.enable.$HTTP_CLIENT_OBSERVATION"
    }
}

/**
 * A [MessageInterpolator] that renders every constraint message in English, whatever locale is asked for or
 * ambient.
 *
 * Bean Validation interpolates against `Locale.getDefault()` unless told otherwise, which makes the text of a
 * violation a property of the host the process runs on rather than of the defect. These messages end up in log
 * lines and exception texts that people search and alert on, so they have to read the same everywhere.
 *
 * The locale-taking overload deliberately ignores its argument too. It is not called on this path - the engine
 * uses the other one - so honouring it would achieve nothing except leaving a way for the guarantee to be
 * bypassed later.
 */
private class EnglishMessageInterpolator(
    private val delegate: MessageInterpolator,
) : MessageInterpolator {
    override fun interpolate(
        messageTemplate: String,
        context: MessageInterpolator.Context,
    ): String = delegate.interpolate(messageTemplate, context, Locale.ENGLISH)

    override fun interpolate(
        messageTemplate: String,
        context: MessageInterpolator.Context,
        locale: Locale,
    ): String = delegate.interpolate(messageTemplate, context, Locale.ENGLISH)
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
 * It lives in its own `@Configuration` rather than in [ResilientWebClientFactory]'s constructor because its
 * effect is JVM-WIDE and cannot be undone: it must be a deliberate, declared piece of wiring, not a side effect
 * of instantiating a factory (which a test may do repeatedly, and which then silently changes the behaviour of
 * every later context in the same JVM). Gated on the module toggle, so a host that did not opt in gets no
 * global change at all.
 */
@Configuration(proxyBeanMethods = false)
class WebClientReactorContextPropagationConfig(
    properties: WebClientProperties,
) {
    private val logger: Logger = LoggerFactory.getLogger(WebClientReactorContextPropagationConfig::class.java)

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

/**
 * Central URI tag mapper for Reactor-Netty HTTP-client metrics. Left raw, a path variable (a UUID per call)
 * would mint a new time series per request - an unbounded cardinality leak. This maps each known path to its
 * low-cardinality TEMPLATE and collapses the rest to a single [UNKNOWN] bucket.
 *
 * The templates are the mesh endpoints' path templates ([tagsOf]), so the path lives only in the `app.mesh`
 * triplet. Matched by prefix (first match wins, longest prefix first so a nested path resolves to the more
 * specific one); a trailing query string sits past the prefix and is ignored.
 */
object WebClientMetrics {
    /** Catch-all for any path not explicitly templated - bounds cardinality on the unknown. */
    const val UNKNOWN: String = "other"

    /** Decoded response body size per call - the input for sizing `app.webclient.max-in-memory-size`. */
    const val RESPONSE_BODY_SIZE: String = "webclient.response.body.size"

    /**
     * BYTES - the same unit as `app.webclient.max-in-memory-size`, which is the whole point of the meter.
     *
     * Sampled with [utf8Size], not `String.length`. The two agree on ASCII and diverge in exactly one
     * direction: a `length` of N encodes to between N and 3N UTF-8 bytes, so counting UTF-16 units would
     * UNDER-state the buffered size - the direction that quietly sizes a ceiling too small. Measured over the
     * 114 real response bodies in the 2026-08-08 log windows the difference was 0.000 %, but that is a
     * property of today's data (account names are customer-set free text), not of the payload shape.
     */
    const val RESPONSE_BODY_BASE_UNIT: String = "bytes"

    /**
     * The UTF-8 encoded size of [text], counted in one pass and without allocating.
     *
     * Equivalent to `text.toByteArray(Charsets.UTF_8).size` for every input, including unpaired surrogates -
     * `WebClientMetricsTest` pins that against the JDK encoder rather than against hand-written expectations.
     * The point of not simply calling `toByteArray` is the copy: on the hot path that is a second full buffer
     * per response, up to `max-in-memory-size` each, to produce a single number.
     *
     * The surrogate handling is the part worth reading. A valid high/low pair is ONE code point in four bytes
     * and consumes two UTF-16 units; an UNPAIRED surrogate is not encodable, and the JDK encoder substitutes
     * `?` - one byte - rather than failing. Both cases are counted the way the encoder counts them, so the
     * meter can never disagree with the codec about the same body.
     */
    fun utf8Size(text: String): Int {
        var bytes = 0
        var i = 0
        while (i < text.length) {
            val c = text[i]
            when {
                c.code < 0x80 -> {
                    bytes += 1
                }

                c.code < 0x800 -> {
                    bytes += 2
                }

                c.isHighSurrogate() && i + 1 < text.length && text[i + 1].isLowSurrogate() -> {
                    bytes += 4
                    i++
                }

                c.isSurrogate() -> {
                    bytes += 1
                }

                else -> {
                    bytes += 3
                }
            }
            i++
        }
        return bytes
    }

    /**
     * Identifies which downstream this sample came from. Named after the `adapter_*` family of the log fields,
     * so a meter and a log line can be correlated on sight: the value is exactly what the exchange event and
     * this module's transport lines carry as `adapter_name`.
     *
     * **The value is the mesh ENDPOINT alias, not an adapter class.** Four values today
     * (`api-example-com-GET-v5-access-profiles-by-involved-party` and three siblings), so no cardinality concern -
     * but note that `InvolvedPartyAdapter` owns two of them (`ALIAS_LOOKUP`, `ALIAS_GET`), so a filter on this
     * tag selects one CALL, not one adapter.
     */
    const val ADAPTER_TAG: String = "adapter"

    /**
     * Build (static prefix, template) pairs from the path templates: the prefix is the template up to its
     * first path variable (`/v6/involved-parties/{uuid}` -> `/v6/involved-parties/`). Longest prefix first,
     * ties broken by the template itself.
     *
     * The tie-break is not cosmetic. Two templates can share a prefix - `/a/{id}` and `/a/{id}/details` both
     * reduce to `/a/` - and with equal lengths a length-only sort leaves their order unspecified, so which of
     * the two a URI gets labelled with would depend on the iteration order of the input. Ordering by template
     * makes the outcome at least deterministic and reproducible; [uriTagger] is what stops the ambiguity from
     * arising on the production path in the first place.
     */
    fun tagsOf(pathTemplates: Collection<String>): List<Pair<String, String>> =
        pathTemplates
            .map { it.substringBefore("{") to it }
            .sortedWith(compareByDescending<Pair<String, String>> { (prefix, _) -> prefix.length }.thenBy { (_, t) -> t })

    /**
     * The URI->tag mapper for ONE endpoint's HttpClient: its own template, or [UNKNOWN].
     *
     * Each endpoint gets its own [HttpClient] ([ResilientWebClientFactory.buildHttpClient] runs per endpoint),
     * and each such client only ever calls its own path - the adapters build the URI from
     * `ResilientWebClient.pathTemplate`. Handing it the templates of ALL endpoints, as this once did, gave a
     * request the chance to match a FOREIGN template first and be counted under another endpoint's series.
     * Scoping the mapper to the one template the client can actually call removes that by construction, rather
     * than relying on the prefix ordering to resolve it correctly.
     *
     * A URI that does not match falls to [UNKNOWN], which is the truthful answer: this client called something
     * other than its endpoint.
     */
    fun uriTagger(pathTemplate: String): (String) -> String {
        val tags = tagsOf(listOf(pathTemplate))
        return { rawUri -> uriTag(rawUri, tags) }
    }

    /** Maps a request URI to its template tag value, or [UNKNOWN] if no prefix matches. */
    fun uriTag(
        rawUri: String,
        tags: List<Pair<String, String>>,
    ): String {
        for ((prefix, template) in tags) {
            if (matchesAtSegmentBoundary(rawUri, prefix)) return template
        }
        return UNKNOWN
    }

    /**
     * A prefix match that may only end ON a path-segment boundary. A template WITHOUT a path variable yields a
     * prefix that is the whole path (`/v8/agreements/products/lookup`), and a plain `startsWith` would then also
     * claim `/v8/agreements/products/lookup-v2` - a different endpoint, silently folded into the wrong series.
     * A prefix that already ends in `/` is at a boundary by construction; otherwise the remainder must be empty
     * (exact hit) or start a new segment (`/`) or the query (`?`).
     */
    private fun matchesAtSegmentBoundary(
        rawUri: String,
        prefix: String,
    ): Boolean {
        if (!rawUri.startsWith(prefix)) return false
        if (prefix.endsWith("/")) return true
        val next = rawUri.getOrNull(prefix.length) ?: return true
        return next == '/' || next == '?'
    }
}
