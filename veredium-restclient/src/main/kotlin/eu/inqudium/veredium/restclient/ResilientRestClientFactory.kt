package eu.inqudium.veredium.restclient

import eu.inqudium.legatium.restclient.logging.ClientRequestLoggingInterceptor
import eu.inqudium.veredium.core.client.MeshClientPlan
import eu.inqudium.veredium.core.client.ResponseBodyMetrics
import eu.inqudium.veredium.core.engine.EngineSelection
import eu.inqudium.veredium.core.engine.EngineSet
import eu.inqudium.veredium.core.mesh.MeshConnectionPool
import eu.inqudium.veredium.core.mesh.MeshConnectionPools
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.observation.ObservationRegistry
import jakarta.annotation.PreDestroy
import jakarta.validation.ValidatorFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.client.ClientHttpRequestFactory
import org.springframework.web.client.RestClient
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledExecutorService

/**
 * Produces fully assembled [ResilientRestClient]s - one per mesh endpoint - bundling the HTTP transport (one
 * shared pool per engine, per-target timeouts, base URL, JSON headers) with the [BlockingResilienceDecorator]
 * pipeline. The backend name is reused as the Resilience4j instance name, so `product-agreements` needs
 * matching `resilience4j.*.instances.product-agreements` config.
 *
 * Exchange logging is not assembled here: legatium-restclient-logging's auto-configuration puts its
 * interceptor on Boot's `RestClient.Builder` bean as a `RestClientCustomizer`, and every client below is a
 * clone of that bean. The factory's only contribution is the request attribute the library reads the client's
 * name from.
 *
 * ## Engines
 *
 * Which HTTP engine carries a backend's exchanges is `app.restclient.engine` ([RestClientProperties.Engine]):
 * Apache HttpClient 5 by default, or the JDK
 * client, Jetty or Reactor Netty, for all backends or per alias. Each engine in use is created ONCE, when the
 * factory is - an engine no backend selects is never created - and shared by the backends on it; the factory
 * owns it and closes it in [shutdown]. Its request factory goes onto the backend's clone of Boot's builder and
 * nowhere else: nothing here reads or writes Boot's `spring.http.clients.*` settings or registers a bean a host's
 * other clients could pick up, so the property changes this module's clients and no other client of the host.
 *
 * Everything above the request factory is the same on every engine: the bounded body read over the response
 * stream, the status mapping, the Host / User-Agent / Accept headers, legatium's interceptor, the observation and
 * the decorator. What an engine maps from the mesh ladder and `app.restclient.pool`, and what it cannot express,
 * is one INFO line per engine at startup, next to the line naming each backend's engine.
 *
 * A configuration the factory cannot honour fails its construction, i.e. the context start, naming the key: an
 * override for an alias that is not a mesh endpoint, an engine whose library is not on the classpath, the JDK
 * engine for a backend whose Host header the JDK would replace.
 */
class ResilientRestClientFactory(
    private val properties: RestClientProperties,
    private val meshProperties: MeshProperties,
    private val resilience: BlockingResilienceDecorator,
    private val objectMapper: ObjectMapper,
    // Nullable ON PURPOSE: an optional dependency, so tests can construct the factory directly; Spring injects
    // the auto-configured factory when a Bean Validation provider is on the classpath. Absent -> validation
    // cannot run for ANY backend, whatever the properties say, which the startup line reports rather than
    // leaving it to be discovered. See `ResponseValidation` for why the FACTORY rather than a Validator.
    validatorFactory: ValidatorFactory? = null,
    // Nullable like the two above: an optional dependency, so a direct (test) construction and a minimal
    // context both work without one. Absent -> neither the pool meters nor the response-size summary are
    // registered and the per-call path records nothing; nothing else about the client changes.
    private val meterRegistry: MeterRegistry? = null,
    // Boot's auto-configured builder (prototype scope: this factory owns its instance and clones it per
    // backend). Every RestClientCustomizer the host declares has already been applied to it - Boot's own
    // (observation registry + convention, message converters, request factory) and the host's.
    //
    // REQUIRED on purpose, unlike the optional dependencies above: those are optional because the factory
    // works sensibly without them. Without this one it would work DIFFERENTLY - a bare builder from the static
    // factory carries no host customizer and no observation registry, so the http.client.requests span would
    // vanish without a log line. Spring treats a Kotlin default as an optional dependency and would take exactly
    // that silent path when the bean is absent; a required parameter makes a missing bean a context-start
    // failure that names RestClient.Builder. A host that excludes RestClientAutoConfiguration declares its own
    // RestClient.Builder bean. Tests that construct the factory directly pass a bare builder and say so at the
    // call site.
    restClientBuilder: RestClient.Builder,
    /**
     * Monotonic clock for the ONE wait this factory makes - the drain deadline in [shutdown]. Injectable so the
     * deadline branch can be reached by a test that controls time instead of one that holds a connection open
     * past the derived window; defaults to `System::nanoTime`, so production wiring is unchanged. The same
     * shape as [ResilientRestClient]'s clock, and like it never used for a timestamp.
     */
    private val nanoTime: () -> Long = System::nanoTime,
    /**
     * Whether an engine's library is on the classpath ([RestClientEngine.isPresent] by default). Injectable for
     * the same reason as [nanoTime]: the test classpath carries every engine, and the missing-library failure
     * is reached by a test that says an engine is absent instead of one that builds a classpath without it.
     */
    private val engineLibraryPresent: (RestClientEngine) -> Boolean = { it.isPresent() },
    /**
     * Whether this JVM's JDK client accepts an explicit Host header ([EngineSelection.jdkAllowsHostHeader] by
     * default). Injectable for the same reason as [engineLibraryPresent]: whether the test JVM runs with the flag is
     * the build's choice, and the conflict is reached by a test that says the JDK refuses the header.
     */
    private val jdkAllowsHostHeader: () -> Boolean = EngineSelection::jdkAllowsHostHeader,
) : MeshConnectionPools {
    private val logger: Logger = LoggerFactory.getLogger(ResilientRestClientFactory::class.java)

    /**
     * The endpoints, the drain window, the default headers, the size meter and the validation - derived the same way
     * as on the reactive stack ([MeshClientPlan]).
     */
    private val plan = MeshClientPlan(meshProperties, RestClientMeshRegistrar.STACK, "app.restclient", properties.validation, validatorFactory, logger)

    /** Standalone HTTP observation toggle (see [RestClientProperties.Observability]) - this module's own property alone. */
    private val observationEnabled: Boolean = properties.observability.httpObservationEnabled

    /**
     * The shared base builder: Boot's, so the host's RestClientCustomizers (observation, converters, own beans)
     * are already on it and every cloned per-endpoint client inherits them. The module toggle does not WIRE
     * the observation registry - Boot does, through its ObservationRestClientCustomizer, so each outbound
     * request opens an `http.client.requests` observation (a CLIENT span + `traceparent` injection with a host
     * tracing bridge) - it only REMOVES it when the operator switched this module's HTTP observation off. What
     * the toggle achieves in the host at hand is reported by [RestClientObservationReport], not here.
     */
    private val baseBuilder: RestClient.Builder =
        restClientBuilder.let { if (observationEnabled) it else it.observationRegistry(ObservationRegistry.NOOP) }

    private val meshByBackend: Map<String, ResolvedTimeouts> = plan.endpoints

    private val meshTransport: MeshProperties.Transport = plan.transport

    /** How long [shutdown] waits for the engines to drain ([MeshClientPlan.drainWindow]). */
    private val poolDisposeTimeout: Duration = plan.drainWindow

    /**
     * `maxInMemorySize` in bytes: the cap on how much of a response body the client will buffer. Handed to every
     * client, which enforces it in its bounded read and names it on its buffer-limit line.
     */
    private val maxInMemoryBytes: Int = properties.maxInMemorySize.toBytes().toInt()

    /** Each backend's engine, resolved once and checked ([checkedEngineSelection]) before any engine is created. */
    private val engineByBackend: Map<String, RestClientEngine> = checkedEngineSelection()

    /**
     * The engines in use, one transport each, created here in a stable order (the enum's) and only for an engine
     * some backend selects, with the lifecycle both stacks share ([EngineSet]). Apache, the default, comes first:
     * its pool and its meters come up when the factory does.
     */
    private val transports: EngineSet<RestClientEngine, EngineTransport> =
        EngineSet(engineByBackend.values.toSortedSet(), "ResilientRestClientFactory", logger) { engine ->
            val settings = EngineTransport.Settings(meshTransport, properties.pool, meterRegistry, poolDisposeTimeout, logger)
            when (engine) {
                RestClientEngine.APACHE -> ApacheEngineTransport(settings)
                RestClientEngine.JDK -> JdkEngineTransport(settings)
                RestClientEngine.JETTY -> JettyEngineTransport(settings)
                RestClientEngine.REACTOR_NETTY -> ReactorNettyEngineTransport(settings)
            }
        }

    init {
        // The engine of every backend, and per engine what of the mesh ladder and the pool settings it applies.
        // Worth the lines because an engine without a knob does not complain about the knob: the setting simply
        // has no effect, and the only place that can say so is here.
        logger.info(
            "HTTP engines: default={}, per backend {}",
            properties.engine.default,
            engineByBackend.toSortedMap().entries.joinToString(", ", "[", "]") { (alias, engine) -> "$alias=$engine" },
        )
        transports.all.forEach { (engine, transport) ->
            logger.info(transport.settingsLine(engineByBackend.filterValues { it == engine }.keys.sorted()))
        }
    }

    /**
     * One pool per engine in use, with the backends on it - what the startup guard sizes the bulkheads against.
     * The JDK client has no connection bound, so its pool carries none.
     */
    override fun connectionPools(): List<MeshConnectionPool> =
        transports.all.map { (engine, transport) ->
            MeshConnectionPool(
                name = transport.name,
                maxConnections = meshTransport.maxConnections.takeUnless { engine == RestClientEngine.JDK },
                aliases = engineByBackend.filterValues { it == engine }.keys,
            )
        }

    /** The Apache engine's background eviction, when Apache is in use - see [ApacheEngineTransport.evictor]. */
    internal val evictor: ScheduledExecutorService? get() = (transports.all[RestClientEngine.APACHE] as? ApacheEngineTransport)?.evictor

    /** The engine transports this factory created - for the tests that pin which engines exist and that [shutdown] closes them. */
    internal val engineTransports: Map<RestClientEngine, EngineTransport> get() = transports.all

    /** Caches one client per logical name so [create] is idempotent - repeated calls return the same instance. */
    private val cache = ConcurrentHashMap<String, ResilientRestClient>()

    /** The request factory each built client runs on - what a test reads to prove which engine carries a backend. */
    private val requestFactories = ConcurrentHashMap<String, ClientHttpRequestFactory>()

    /** The request factory the client of [backendName] was built on, or `null` before [create] built it. */
    internal fun requestFactoryOf(backendName: String): ClientHttpRequestFactory? = requestFactories[backendName]

    /**
     * Resolves each backend's engine and fails the start when the selection cannot be honoured, naming the key:
     *
     * - an `app.restclient.engine.endpoints` key that is no mesh alias - a typo would otherwise leave that backend
     *   on the default engine without a word, the same silent failure an unknown validation key has;
     * - an engine whose library is not on the classpath, naming the dependency to add - better at startup than as
     *   a `NoClassDefFoundError` at the first call;
     * - the JDK engine for a backend whose Host header the JDK would replace by the base URL's authority.
     *
     * Each fails with the `IllegalArgumentException` of the shared checks ([EngineSelection]), as on the reactive
     * stack.
     */
    private fun checkedEngineSelection(): Map<String, RestClientEngine> {
        val engines = properties.engine
        EngineSelection.requireKnownAliases(RestClientProperties.Engine.PREFIX, engines.endpoints.keys, meshByBackend.keys, plan.foreignEndpoints)
        val selected = meshByBackend.keys.associateWith(engines::engineFor)
        EngineSelection.requireLibraries(RestClientProperties.Engine.PREFIX, selected, engines.endpoints.keys, engineLibraryPresent)
        val hostAllowed = jdkAllowsHostHeader()
        selected
            .filterValues { it == RestClientEngine.JDK }
            .keys
            .sorted()
            .forEach { alias ->
                val conflict = JdkEngineTransport.hostHeaderConflict(meshByBackend.getValue(alias), hostAllowed)
                require(conflict == null) {
                    "${engines.keyFor(alias)} selects the HTTP engine 'jdk' for '$alias', but the JDK client " +
                        "restricts the Host header: $conflict, so the sidecar would route the call by the wrong " +
                        "host. Select another engine for this backend, point its base-url at a URL whose " +
                        "authority is the Host, or start the JVM with " +
                        "-D${EngineSelection.JDK_ALLOW_RESTRICTED_HEADERS}=host - a JVM-wide setting that " +
                        "changes every java.net.http client of the process, which is why veredium never sets it."
                }
            }
        return selected
    }

    /**
     * Returns the resilient client for [backendName], building it on first request and caching it after.
     * Everything per-endpoint (base URL, Host header, timeouts, engine) comes from the `app.mesh.endpoints[]`
     * entry whose alias equals [backendName] and from its `app.restclient.engine` selection.
     *
     * @throws IllegalStateException if no mesh endpoint with that alias is configured - failing fast at
     *         startup beats a confusing 404/connection error at the first call - or if the factory was already
     *         shut down and the client was not built before.
     */
    fun create(backendName: String): ResilientRestClient = cache.computeIfAbsent(backendName, ::build)

    private fun build(backendName: String): ResilientRestClient {
        val endpoint = plan.endpoint(backendName)

        val requestFactory = transports.use(engineByBackend.getValue(backendName), backendName) { it.requestFactory(endpoint) }
        requestFactories[backendName] = requestFactory

        val restClient =
            baseBuilder
                .clone()
                .baseUrl(endpoint.baseUrl)
                // Replaces whatever request factory Boot put on the builder from its `spring.http.client.*`
                // settings - on this CLONE only, Boot's builder bean and every other client built from it keep
                // theirs: these clients run on the mesh ladder's timeouts and the engine's shared pool, which the
                // host's global HTTP client settings know nothing about. Deliberate, and the same split the
                // reactive twin makes for its connector.
                .requestFactory(requestFactory)
                // Host routes the sidecar to the upstream (from the triplet); set() (not add) so re-applying
                // never accumulates values. Every engine honours an explicit Host header except the JDK's, which
                // the factory refuses at startup where it matters (checkedEngineSelection); the same goes for
                // User-Agent: the library's token (`veredium/<version>`) or the host's own
                // (app.restclient.user-agent) replaces the engine's default so a peer's access log names what
                // calls it.
                .defaultHeaders { headers ->
                    plan.defaultHeaders(endpoint, properties.effectiveUserAgent).forEach { (name, value) -> headers.set(name, value) }
                }
                // The backend name as a request attribute, set ONCE per client so every request carries it.
                // An attribute, not a header: it names the client for legatium's exchange event (adapter_name
                // and the `name` tag of its body meters) and never goes on the wire. The key is the library's
                // public constant; the library's interceptor is already on this builder, put there by Boot's bean.
                .defaultRequest { it.attribute(ClientRequestLoggingInterceptor.ADAPTER_NAME_ATTRIBUTE, backendName) }
                .build()

        // The validator is resolved to null when validation is off for THIS backend, so the per-call path
        // carries no toggle (see ResponseValidation).
        return ResilientRestClient(
            restClient,
            backendName,
            endpoint,
            resilience,
            objectMapper,
            maxInMemoryBytes,
            plan.validation.validatorFor(backendName),
            plan.validation.failOnViolation,
            plan.responseBodySize(meterRegistry, RestClientMetrics.RESPONSE_BODY_SIZE, backendName),
        )
    }

    /**
     * Closes every engine in use on context shutdown, each draining its exchanges in flight until one shared
     * deadline [poolDisposeTimeout] from now (see each engine's `shutdown`): the Apache pool, the Jetty client,
     * the Reactor Netty pool and event loops, the JDK client.
     *
     * An engine that does not drain in time is an OUTCOME, not a failure - reported as the statement it is and
     * closed anyway, never as an exception out of a destroy callback, where Spring could only log a stack trace at
     * the moment someone is looking for one. The wait also ends when the calling thread is INTERRUPTED (a
     * container tearing the context down under a deadline of its own); the flag is left for the caller. An engine
     * whose shutdown throws is reported as such and does not keep the engines after it open.
     *
     * Idempotent: Spring calls it once, tests may call it twice; a closed engine closes again without complaint.
     */
    @PreDestroy
    fun shutdown() {
        transports.shutdown(nanoTime() + poolDisposeTimeout.toNanos(), nanoTime)
    }
}

/**
 * The meter vocabulary of this module. Smaller than the reactive twin's on purpose: the twin also owns a
 * URI-to-template mapper for Reactor Netty's per-request transport metrics, which would mint a series per path
 * variable without it. Apache HttpClient records no per-request meter here; the per-request view is Spring's
 * `http.client.requests` observation, which takes its `uri` tag from the template attribute every client stamps
 * on its requests - so the cardinality guard is the attribute, and a second mapper would guard nothing.
 */
object RestClientMetrics {
    /**
     * Prefix of the shared pool's gauges, as Apache's `ConnPoolMeters` names them: `restclient.pool.leased`,
     * `restclient.pool.available` and `restclient.pool.pending`, each tagged [POOL_TAG] = the pool name. The
     * counterpart of Reactor Netty's `reactor.netty.connection.provider.*` family on the twin.
     */
    const val POOL_METER_PREFIX: String = "restclient"

    /** The tag naming the pool on its gauges. */
    const val POOL_TAG: String = "pool"

    /**
     * The Apache engine's pool name: the [POOL_TAG] value on its gauges and the stem of its evictor thread's name. A
     * constant, not a property: a factory has at most one Apache pool, and no deployment runs two factories in one
     * registry.
     */
    const val POOL_NAME: String = "restclient-apache"

    /**
     * Decoded response body size per call - the input for sizing `app.restclient.max-in-memory-size`; unit, tag
     * and measurement are [ResponseBodyMetrics], shared with the reactive client.
     */
    const val RESPONSE_BODY_SIZE: String = "restclient.response.body.size"
}
