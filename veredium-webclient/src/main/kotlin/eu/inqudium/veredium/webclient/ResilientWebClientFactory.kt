package eu.inqudium.veredium.webclient

import eu.inqudium.legatium.webclient.logging.ClientRequestLoggingFilter
import eu.inqudium.veredium.core.client.ResponseBodyMetrics
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import eu.inqudium.veredium.core.validation.ResponseValidation
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.observation.ObservationRegistry
import jakarta.annotation.PreDestroy
import jakarta.validation.ValidatorFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ResourceLoader
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ClientHttpConnector
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeoutException

/**
 * Produces fully assembled [ResilientWebClient]s - one per mesh endpoint - bundling the HTTP transport (the
 * backend's engine on resources shared per engine, per-target timeouts, base URL, JSON headers, bounded codec)
 * with the [ReactiveResilienceDecorator] pipeline. The backend name is reused as the Resilience4j instance name,
 * so `product-agreements` needs matching `resilience4j.*.instances.product-agreements` config.
 *
 * ## Engines
 *
 * Which library carries a backend's exchanges is `app.webclient.engine` ([WebClientProperties.Engine],
 * [WebClientEngine]): Reactor Netty by default, the JDK client, Jetty or Apache HttpClient 5 per backend. The
 * choice stays inside this factory. It builds the connector itself, on resources it owns - created when the
 * first client of an engine is built, shared by that engine's backends, released in [shutdown] - and sets it on
 * its clone of Boot's builder. It never reads or writes Boot's `spring.http.clients.reactive.*` switches,
 * registers no connector, HTTP-client or settings bean, does not use the host's `ReactorResourceFactory` bean and
 * leaves Boot's builder bean as it found it, so every other WebClient of the host keeps the engine Boot gave it.
 * Reactor Netty included: its pool, event loops and resolver are the factory's, not the process-wide
 * `HttpResources` the host's own Reactor Netty clients share.
 *
 * The construction fails, naming the key, when an override names no mesh alias, when a selected engine's library
 * is missing, when the JDK engine cannot send the Host header in this JVM, and when Jetty is asked for a request
 * queue of 0. One INFO line names the engine per backend, one per engine in use states which transport and pool
 * settings it applies and which it cannot, which transport meters it exposes and what cancellation does.
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
    // Nullable ON PURPOSE: an optional dependency, so tests can construct the factory directly; Spring injects
    // the auto-configured factory when a Bean Validation provider is on the classpath. Absent -> validation
    // cannot run for ANY backend, whatever the properties say, which the startup line reports rather than
    // leaving it to be discovered. See `ResponseValidation` for why the FACTORY rather than a Validator.
    validatorFactory: ValidatorFactory? = null,
    // Nullable like the two above: an optional dependency, so a direct (test) construction and a minimal
    // context both work without one. Absent -> the response-size summary is simply not registered and the
    // per-call path records nothing; nothing else about the client changes.
    private val meterRegistry: MeterRegistry? = null,
    // Boot's auto-configured builder (prototype scope: this factory owns its instance and clones it per
    // backend). Every WebClientCustomizer the host declares has already been applied to it - Boot's own
    // (observation registry + convention, codecs, connector) and the host's.
    //
    // REQUIRED on purpose, unlike the optional dependencies above: those are optional because the factory
    // works sensibly without them. Without this one it would work DIFFERENTLY - a bare builder from the static
    // factory carries no host customizer and no observation registry, so the http.client.requests span would vanish
    // without a log line. Spring treats a Kotlin default as an optional dependency and would take exactly that
    // silent path when the bean is absent; a required parameter makes a missing bean a context-start failure
    // that names WebClient.Builder. A host that excludes WebClientAutoConfiguration declares its own
    // WebClient.Builder bean. Tests that construct the factory directly pass a bare builder and say so at the
    // call site.
    webClientBuilder: WebClient.Builder,
    // Nullable like the optional dependencies above: the context injects itself, so the engine-library check asks
    // the class loader the context was started with (which is what a test with a filtered class loader varies).
    // Absent -> this class's own loader.
    resourceLoader: ResourceLoader? = null,
) {
    private val logger: Logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java)

    /** Which backends validate, with which validator, and whether a violation fails the call - see [ResponseValidation]. */
    private val validation = ResponseValidation(properties.validation, validatorFactory)

    /** Standalone HTTP observation toggle (see [WebClientProperties.Observability]) - no sync-bridge coupling. */
    private val observationEnabled: Boolean = properties.observability.httpObservationEnabled

    /**
     * The shared base builder: Boot's, so the host's WebClientCustomizers (observation, codecs, connector, own
     * beans) are already on it and every cloned per-endpoint client inherits them. The module toggle no longer
     * WIRES the observation registry - Boot does, through its ObservationWebClientCustomizer, so each outbound
     * request opens an `http.client.requests` observation (a CLIENT span + `traceparent` injection with a host
     * tracing bridge) - it only REMOVES it when the operator switched this module's HTTP observation off. What
     * the toggle achieves in the host at hand is reported by [WebClientObservationReport], not here.
     */
    private val baseBuilder: WebClient.Builder =
        webClientBuilder.let { if (observationEnabled) it else it.observationRegistry(ObservationRegistry.NOOP) }

    init {
        // Which backends actually validate, resolved the same way the clients resolve it - see
        // ResponseValidation.validatedAliases for why this is worth a line.
        logger.info(
            "Response Bean Validation: default={}, validated endpoints={} (provider {})",
            validation.enabledByDefault,
            validation.validatedAliases(meshProperties.resolveAll().values.map { it.alias }),
            validation.providerVerdict,
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
     *
     * The bound is sized for the strongest reading of "drain" - waiting out every in-flight call - although the
     * disposal [shutdown] runs waits only for the idle connections to close (see there). It is therefore
     * generous rather than tight; what it must never be is SHORTER than a call that could still be running.
     */
    internal val poolDisposeTimeout: Duration =
        (meshByBackend.values.maxOfOrNull { it.responseTimeout } ?: Duration.ZERO)
            .plusMillis(meshTransport.connectTimeoutMs + meshTransport.pendingAcquireTimeoutMs)
            .plusSeconds(1)

    /** The engine of every backend, resolved once: its `app.webclient.engine.endpoints` entry or the default. */
    private val engineByBackend: Map<String, WebClientEngine> = meshByBackend.keys.associateWith(properties.engine::engineFor)

    init {
        WebClientEngineChecks.requireKnownAliases(properties.engine, meshByBackend.keys)
        WebClientEngineChecks.requireLibraries(engineByBackend, resourceLoader?.classLoader ?: javaClass.classLoader)
        WebClientEngineChecks.requireJettyQueue(engineByBackend, properties.pool)
        WebClientEngineChecks.requireJdkHostHeader(engineByBackend)
        logger.info(WebClientEngineChecks.engineLine(properties.engine, engineByBackend))
        engineByBackend.values.toSortedSet().forEach { engine ->
            logger.info(WebClientEngineChecks.coverageLine(engine, engineByBackend, properties.pool, meshTransport))
        }
    }

    /**
     * The resources of every engine in use, each created when the first client of its engine is built - so an
     * engine no backend selected is never created, and Reactor Netty's pool is not built for a factory whose
     * backends all run on another engine.
     */
    private val transports =
        EngineTransports(engineByBackend.values.toSet(), logger) { engine ->
            when (engine) {
                WebClientEngine.REACTOR_NETTY -> {
                    ReactorNettyTransport(properties, meshTransport, POOL_NAME, poolDisposeTimeout) { disposal ->
                        drain(disposal, Schedulers.parallel()).block()
                    }
                }

                WebClientEngine.JDK -> {
                    JdkTransport(meshTransport, poolDisposeTimeout, logger)
                }

                WebClientEngine.JETTY -> {
                    JettyTransport(properties, meshTransport)
                }

                WebClientEngine.APACHE -> {
                    ApacheTransport(properties, meshTransport, poolDisposeTimeout, logger)
                }
            }
        }

    /** The connector each backend's client was built with - for the tests that pin which engine carries whom. */
    private val connectors = ConcurrentHashMap<String, ClientHttpConnector>()

    /**
     * `maxInMemorySize` in bytes: the cap on how much of a response body the codec will buffer. Set on every
     * client's codecs, and handed to the client so its buffer-limit line can name the number.
     */
    private val maxInMemoryBytes: Int = properties.maxInMemorySize.toBytes().toInt()

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

        // The backend's engine, on the shared resources of that engine - set on the CLONE only, so Boot's builder
        // bean and every other client of the host keep their own connector.
        val connector = transports.connectorFor(engineByBackend.getValue(backendName), endpoint)
        connectors[backendName] = connector

        val webClient =
            baseBuilder
                .clone()
                .baseUrl(endpoint.baseUrl)
                .clientConnector(connector)
                // Host routes the sidecar to the upstream (from the triplet); set() (not add) so re-applying
                // never accumulates values. User-Agent is the library's token (`veredium/<version>`) or the
                // host's own (app.webclient.user-agent), which every engine honours over its own default, so a
                // peer's access log names what calls it. The JDK client accepts the Host header only with the JVM
                // flag the construction checked for. Extra static headers are applied on top, so an
                // endpoint can override either.
                .defaultHeaders { headers ->
                    headers.set(HttpHeaders.HOST, endpoint.host)
                    headers.set(HttpHeaders.USER_AGENT, properties.effectiveUserAgent)
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
        // carries no toggle (see ResponseValidation).
        return ResilientWebClient(
            webClient,
            backendName,
            endpoint,
            resilience,
            objectMapper,
            maxInMemoryBytes,
            validation.validatorFor(backendName),
            validation.failOnViolation,
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
     * Resolved ONCE per backend here rather than looked up per call, the same way the validator is: the hot path
     * should hold no registry lookup and no toggle it could disagree with.
     */
    private fun responseBodySize(backendName: String): DistributionSummary? =
        meterRegistry?.let { registry ->
            DistributionSummary
                .builder(WebClientMetrics.RESPONSE_BODY_SIZE)
                .description("Decoded response body size in UTF-8 bytes, for sizing app.webclient.max-in-memory-size")
                .baseUnit(ResponseBodyMetrics.BASE_UNIT)
                .tag(ResponseBodyMetrics.ADAPTER_TAG, backendName)
                .register(registry)
        }

    /** The engine of [backendName] as resolved at construction, or `null` for a name with no mesh endpoint. */
    internal fun engineOf(backendName: String): WebClientEngine? = engineByBackend[backendName]

    /** The connector [backendName]'s client was built with, or `null` before [create] was called for it. */
    internal fun connectorOf(backendName: String): ClientHttpConnector? = connectors[backendName]

    /** The engines whose resources exist - created by a first client, released or not. */
    internal fun startedEngines(): Map<WebClientEngine, EngineTransport> = transports.started()

    /**
     * Releases the resources of every engine in use on context shutdown, each bounded by [poolDisposeTimeout]:
     * Reactor Netty's pool is disposed (below) and its event loops once the bound has passed, the JDK client shut
     * down in order, Jetty's client stopped (which
     * aborts its exchanges), Apache's I/O reactor shut down gracefully. An engine that never built a client has
     * nothing to release.
     *
     * For Reactor Netty, what the disposal waits for is narrower than "drain": the pool is built without Reactor
     * Netty's `disposeTimeout`, so `disposeLater()` is not a graceful shutdown. It fails pending acquires, closes the
     * IDLE connections and completes; a connection an in-flight call holds is not cut - the call finishes on it,
     * and the connection is closed when it is released. So an in-flight call does not hold [shutdown] up, and
     * the timeout bounds the closing of the idle connections.
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
        transports.release()
    }

    /**
     * [shutdown]'s bounded wait for [disposal], with the WARN for a disposal that outlives [poolDisposeTimeout].
     *
     * Separate from [shutdown] for the one reason that the timeout branch can only be tested with a disposal
     * that never ends and a clock that can be advanced: the real pool's disposal finishes in milliseconds, and
     * [poolDisposeTimeout] is longer than any response timeout by construction. [timer] is where the timeout is
     * scheduled - Reactor's parallel scheduler in production (what `timeout(Duration)` uses on its own), a
     * virtual-time scheduler in the test.
     */
    internal fun drain(
        disposal: Mono<Void>,
        timer: Scheduler,
    ): Mono<Void> =
        disposal
            .timeout(poolDisposeTimeout, timer)
            .onErrorResume(TimeoutException::class.java) {
                logger.warn(
                    "Connection pool '{}' did not drain within {} - dropping it with connections still open",
                    POOL_NAME,
                    poolDisposeTimeout,
                )
                Mono.empty()
            }

    internal companion object {
        /** Name of the Reactor-Netty pool shared by that engine's backends; also the `name` tag on its pool metrics. */
        const val POOL_NAME = "lap-webclient"
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

    /**
     * Decoded response body size per call - the input for sizing `app.webclient.max-in-memory-size`; unit, tag
     * and measurement are [ResponseBodyMetrics], shared with the blocking client.
     */
    const val RESPONSE_BODY_SIZE: String = "webclient.response.body.size"

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
     * Each endpoint gets its own Reactor-Netty `HttpClient` (the transport builds one per endpoint),
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
