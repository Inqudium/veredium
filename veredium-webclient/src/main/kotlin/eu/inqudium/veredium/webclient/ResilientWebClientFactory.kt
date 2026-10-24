package eu.inqudium.veredium.webclient

import eu.inqudium.legatium.webclient.logging.ClientRequestLoggingFilter
import eu.inqudium.veredium.core.client.MeshClientPlan
import eu.inqudium.veredium.core.client.ResponseBodyMetrics
import eu.inqudium.veredium.core.engine.EngineDrain
import eu.inqudium.veredium.core.engine.EngineSet
import eu.inqudium.veredium.core.mesh.MeshConnectionPool
import eu.inqudium.veredium.core.mesh.MeshConnectionPools
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.observation.ObservationRegistry
import jakarta.annotation.PreDestroy
import jakarta.validation.ValidatorFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.core.io.ResourceLoader
import org.springframework.http.client.reactive.ClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
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
 * choice stays inside this factory. It builds the connector itself, on resources it owns - created with the
 * factory for every engine in use, shared by that engine's backends, released in [shutdown] - and sets it on
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
    /**
     * Monotonic clock for the ONE wait this factory makes - the shared release deadline in [shutdown]. Injectable so
     * the deadline can be reached by a test that controls time; defaults to `System::nanoTime`, as the blocking
     * twin's factory does.
     */
    private val nanoTime: () -> Long = System::nanoTime,
) : MeshConnectionPools {
    private val logger: Logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java)

    /**
     * The endpoints, the drain window, the default headers, the size meter and the validation - derived the same way
     * as on the blocking stack ([MeshClientPlan]).
     */
    private val plan = MeshClientPlan(meshProperties, WebClientMeshRegistrar.STACK, "app.webclient", properties.validation, validatorFactory, logger)

    /** Standalone HTTP observation toggle (see [WebClientProperties.Observability]) - reads only this module's property. */
    private val observationEnabled: Boolean = properties.observability.httpObservationEnabled

    /**
     * The shared base builder: Boot's, so the host's WebClientCustomizers (observation, codecs, connector, own
     * beans) are already on it and every cloned per-endpoint client inherits them. The module toggle does not
     * WIRE the observation registry - Boot does, through its ObservationWebClientCustomizer, so each outbound
     * request opens an `http.client.requests` observation (a CLIENT span + `traceparent` injection with a host
     * tracing bridge) - it only REMOVES it when the operator switched this module's HTTP observation off. What
     * the toggle achieves in the host at hand is reported by [WebClientObservationReport], not here.
     */
    private val baseBuilder: WebClient.Builder =
        webClientBuilder.let { if (observationEnabled) it else it.observationRegistry(ObservationRegistry.NOOP) }

    private val meshByBackend: Map<String, ResolvedTimeouts> = plan.endpoints

    private val meshTransport: MeshProperties.Transport = plan.transport

    /**
     * How long [shutdown] waits for the engines to drain ([MeshClientPlan.drainWindow]). Sized for the strongest
     * reading of "drain" - waiting out every in-flight call, the ones still waiting for a connection included - which
     * is what every engine's release does. Reactor Netty's pool takes it as its graceful `disposeTimeout`.
     */
    internal val poolDisposeTimeout: Duration = plan.drainWindow

    /** The engine of every backend, resolved once: its `app.webclient.engine.endpoints` entry or the default. */
    private val engineByBackend: Map<String, WebClientEngine> = meshByBackend.keys.associateWith(properties.engine::engineFor)

    init {
        WebClientEngineChecks.requireKnownAliases(properties.engine, meshByBackend.keys, plan.foreignEndpoints)
        WebClientEngineChecks.requireLibraries(properties.engine, engineByBackend, resourceLoader?.classLoader ?: javaClass.classLoader)
        WebClientEngineChecks.requireJettyQueue(engineByBackend, properties.pool)
        WebClientEngineChecks.requireJdkHostHeader(engineByBackend)
        logger.info(WebClientEngineChecks.engineLine(properties.engine, engineByBackend))
        engineByBackend.values.toSortedSet().forEach { engine ->
            logger.info(WebClientEngineChecks.coverageLine(engine, engineByBackend, properties.pool, meshTransport))
        }
    }

    /**
     * The resources of every engine in use, created here in a stable order (the enum's) and only for an engine some
     * backend selects - so Reactor Netty's pool is not built for a factory whose backends all run on another engine -
     * with the lifecycle both stacks share ([EngineSet]).
     */
    private val transports: EngineSet<WebClientEngine, EngineTransport> =
        EngineSet(engineByBackend.values.toSortedSet(), "ResilientWebClientFactory", logger) { engine ->
            when (engine) {
                WebClientEngine.REACTOR_NETTY -> {
                    ReactorNettyEngineTransport(properties, meshTransport, POOL_NAME, poolDisposeTimeout, ::awaitPoolDisposal)
                }

                WebClientEngine.JDK -> {
                    JdkEngineTransport(meshTransport, poolDisposeTimeout, logger)
                }

                WebClientEngine.JETTY -> {
                    JettyEngineTransport(properties, meshTransport, poolDisposeTimeout, logger)
                }

                WebClientEngine.APACHE -> {
                    ApacheEngineTransport(properties, meshTransport, poolDisposeTimeout, logger)
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
     *         startup beats a confusing 404/connection error at the first call - and for a client not built
     *         before [shutdown]: building it would create engine resources that nothing releases any more.
     */
    fun create(backendName: String): ResilientWebClient = cache.computeIfAbsent(backendName, ::build)

    private fun build(backendName: String): ResilientWebClient {
        val endpoint = plan.endpoint(backendName)

        // The backend's engine, on the shared resources of that engine - set on the CLONE only, so Boot's builder
        // bean and every other client of the host keep their own connector.
        val connector = transports.use(engineByBackend.getValue(backendName), backendName) { it.connectorFor(endpoint) }
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
                // flag the construction checked for.
                .defaultHeaders { headers ->
                    plan.defaultHeaders(endpoint, properties.effectiveUserAgent).forEach { (name, value) -> headers.set(name, value) }
                }
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
            plan.validation.validatorFor(backendName),
            plan.validation.failOnViolation,
            plan.responseBodySize(meterRegistry, WebClientMetrics.RESPONSE_BODY_SIZE, backendName),
        )
    }

    /**
     * The connection pools the bulkheads are sized against, for the startup guard: Reactor Netty keeps a sub-pool
     * per backend (each backend's client has a configuration of its own), so one pool per backend; Jetty and Apache
     * share one per engine (every backend behind the sidecar is one destination / route); the JDK client has no
     * connection bound.
     */
    override fun connectionPools(): List<MeshConnectionPool> =
        engineByBackend.entries
            .groupBy({ it.value }, { it.key })
            .flatMap { (engine, aliases) ->
                when (engine) {
                    WebClientEngine.REACTOR_NETTY -> aliases.map { MeshConnectionPool("webclient-$engine-$it", meshTransport.maxConnections, setOf(it)) }
                    WebClientEngine.JDK -> listOf(MeshConnectionPool("webclient-$engine", null, aliases.toSet()))
                    WebClientEngine.JETTY, WebClientEngine.APACHE -> listOf(MeshConnectionPool("webclient-$engine", meshTransport.maxConnections, aliases.toSet()))
                }
            }

    /** The engine of [backendName] as resolved at construction, or `null` for a name with no mesh endpoint. */
    internal fun engineOf(backendName: String): WebClientEngine? = engineByBackend[backendName]

    /** The connector [backendName]'s client was built with, or `null` before [create] was called for it. */
    internal fun connectorOf(backendName: String): ClientHttpConnector? = connectors[backendName]

    /** The engine transports this factory created - for the tests that pin which engines exist and that [shutdown] closes them. */
    internal val engineTransports: Map<WebClientEngine, EngineTransport> get() = transports.all

    /**
     * Releases the resources of every engine in use on context shutdown, all of them against ONE deadline
     * [poolDisposeTimeout] from now - the engines are released one after another, and a bound per engine would
     * let the shutdown take that bound once per engine. Reactor Netty's pool is disposed gracefully (below) and its
     * event loops once the bound has passed, the JDK client shut down in order, Jetty's client stopped once its
     * in-flight exchanges finished (or the deadline passed), Apache's I/O reactor shut down gracefully once its
     * exchanges in flight finished (or the deadline passed). Afterwards, no client can be built any more ([create]).
     */
    @PreDestroy
    fun shutdown() {
        transports.shutdown(nanoTime() + poolDisposeTimeout.toNanos(), nanoTime)
    }

    /**
     * The Reactor Netty engine's wait for its pool's [disposal] during [shutdown], bounded by [left] - what is left of
     * the shared deadline when that engine's turn comes, never the full [poolDisposeTimeout] again ([drain]).
     *
     * The wait ends when the calling thread is INTERRUPTED, as every engine's drain does ([EngineDrain]): the disposal
     * keeps running, the interrupt is reported on the one drain line and the flag is left for the caller. Not
     * `block()`, which answers an interrupt with an exception - out of the engine's release, before its event loops
     * were scheduled for disposal. A disposal that already completed (an idle pool) is no drain cut short and is not
     * reported.
     */
    internal fun awaitPoolDisposal(
        disposal: Mono<Void>,
        left: Duration,
    ) {
        val pending = drain(disposal, Schedulers.parallel(), left).toFuture()
        try {
            pending.get()
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            EngineDrain.reportUndrained(logger, WebClientEngine.REACTOR_NETTY.key, poolDisposeTimeout, left = null)
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }
    }

    /**
     * [shutdown]'s bounded wait for [disposal], with the WARN for a disposal that outlives [timeout] - what is
     * left of the shutdown deadline when the Reactor Netty engine's turn comes. The WARN names the drain window
     * [poolDisposeTimeout], as every engine's does, not the part of it that was left for this engine.
     *
     * For Reactor Netty the disposal IS the drain: the pool is built with Reactor Netty's `disposeTimeout`
     * ([poolDisposeTimeout]), so `disposeLater()` refuses new acquires, still serves the calls waiting for a
     * connection, lets every in-flight call finish on its connection, and completes once all connections came back.
     *
     * Expiry is an OUTCOME, not a failure. `block(Duration)` would report it as an `IllegalStateException` -
     * the same shape as a genuine disposal error, and one that Spring can only catch and log as a destroy
     * method that threw. The last line before the process exits would then be a stack trace, at the moment
     * someone is looking for one, saying nothing about what actually happened. `timeout` + `onErrorResume`
     * turns it into the statement it is: the engine did not drain in time, and it is being closed anyway - on
     * the same WARN line as every other engine ([EngineDrain.reportUndrained]), named by its engine key. The bare
     * `block()` in [awaitPoolDisposal] cannot hang - the timeout guarantees a terminal signal.
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
        timeout: Duration = poolDisposeTimeout,
    ): Mono<Void> =
        disposal
            .timeout(timeout, timer)
            .onErrorResume(TimeoutException::class.java) {
                EngineDrain.reportUndrained(logger, WebClientEngine.REACTOR_NETTY.key, poolDisposeTimeout, left = null)
                Mono.empty()
            }

    internal companion object {
        /** Name of the Reactor-Netty pool shared by that engine's backends; also the `name` tag on its pool metrics. */
        const val POOL_NAME = "webclient-reactor-netty"
    }
}

/**
 * Central URI tag mapper for Reactor-Netty HTTP-client metrics. Left raw, a path variable (a UUID per call)
 * would mint a new time series per request - an unbounded cardinality leak. This maps each known path to its
 * low-cardinality TEMPLATE and collapses the rest to a single [UNKNOWN] bucket.
 *
 * The template is the mesh endpoint's path template, so the path lives only in the `app.mesh` triplet. Matched by
 * its static prefix (the template up to its first path variable); a trailing query string sits past the prefix and
 * is ignored.
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
     * The URI->tag mapper for ONE endpoint's HttpClient: its own template, or [UNKNOWN].
     *
     * Each endpoint gets its own Reactor-Netty `HttpClient` (the transport builds one per endpoint),
     * and each such client only ever calls its own path - the adapters build the URI from
     * [ResilientWebClient.pathTemplate]. A mapper holding the templates of ALL endpoints would give a request the
     * chance to match a FOREIGN template first and be counted under another endpoint's series.
     * Scoping the mapper to the one template the client can actually call removes that by construction, rather
     * than ordering the templates of all endpoints and hoping the right one matches first.
     *
     * A URI that does not match falls to [UNKNOWN], which is the truthful answer: this client called something
     * other than its endpoint.
     */
    fun uriTagger(pathTemplate: String): (String) -> String {
        val prefix = pathTemplate.substringBefore("{")
        return { rawUri -> if (matchesAtSegmentBoundary(rawUri, prefix)) pathTemplate else UNKNOWN }
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
