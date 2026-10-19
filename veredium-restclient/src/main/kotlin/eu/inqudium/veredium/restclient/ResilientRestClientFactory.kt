package eu.inqudium.veredium.restclient

import eu.inqudium.legatium.restclient.logging.ClientRequestLoggingInterceptor
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.observation.ObservationRegistry
import jakarta.annotation.PreDestroy
import jakarta.validation.ValidatorFactory
import org.apache.hc.client5.http.config.ConnectionConfig
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder
import org.apache.hc.client5.http.observation.MetricConfig
import org.apache.hc.client5.http.observation.binder.ConnPoolMeters
import org.apache.hc.core5.io.CloseMode
import org.apache.hc.core5.util.TimeValue
import org.apache.hc.core5.util.Timeout
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

/**
 * Produces fully assembled [ResilientRestClient]s - one per mesh endpoint - bundling the Apache HttpClient 5
 * transport (one shared pool, per-target timeouts, base URL, JSON headers) with the
 * [BlockingResilienceDecorator] pipeline. The backend name is reused as the Resilience4j instance name, so
 * `product-agreements` needs matching `resilience4j.*.instances.product-agreements` config.
 *
 * Exchange logging is not assembled here: legatium-restclient-logging's auto-configuration puts its
 * interceptor on Boot's `RestClient.Builder` bean as a `RestClientCustomizer`, and every client below is a
 * clone of that bean. The factory's only contribution is the request attribute the library reads the client's
 * name from.
 *
 * ## The transport, mapped from the reactive twin
 *
 * | Reactor Netty (web-client)                   | Apache HttpClient 5 (here)                                            |
 * |----------------------------------------------|-----------------------------------------------------------------------|
 * | one `ConnectionProvider` for all endpoints   | one `PoolingHttpClientConnectionManager`, shared by every client      |
 * | `maxConnections`                             | `maxConnTotal` AND `maxConnPerRoute` (every endpoint is one route)    |
 * | `pendingAcquireTimeout`                      | `RequestConfig.connectionRequestTimeout`                              |
 * | `pendingAcquireMaxCount`                     | none - Apache's pending queue is bounded in time only                 |
 * | `CONNECT_TIMEOUT_MILLIS`                     | `ConnectionConfig.connectTimeout`                                     |
 * | per-endpoint `responseTimeout`               | per-client `RequestConfig.responseTimeout` (applied as socket timeout) |
 * | `maxIdleTime` / `maxLifeTime` / `evictInBackground` | `closeIdle` / `ConnectionConfig.timeToLive` + `closeExpired`, on the factory's own schedule |
 * | `compress(true)` / `keepAlive(true)`         | Apache's defaults (content compression and keep-alive are on)         |
 * | `.metrics(true)` on the pool                 | Apache's `ConnPoolMeters` (httpclient5-observation), see [RestClientMetrics] |
 * | `.metrics(true)` per request, URI-templated  | none - Spring's `http.client.requests` observation carries the template |
 *
 * What Apache does by default and Reactor Netty does not is switched OFF, so the two clients behave alike on
 * the wire: no automatic retry of a failed idempotent request (the sidecar owns retries, and a second attempt
 * would double-count against the ladder), no redirect following, no cookie jar.
 */
@Component
class ResilientRestClientFactory(
    private val properties: RestClientProperties,
    private val meshProperties: MeshProperties,
    private val resilience: BlockingResilienceDecorator,
    private val objectMapper: tools.jackson.databind.ObjectMapper,
    // Nullable ON PURPOSE: an optional dependency, so tests can construct the factory directly; Spring injects
    // the auto-configured factory when a Bean Validation provider is on the classpath. Absent -> validation
    // cannot run for ANY backend, whatever the properties say, which the startup line reports rather than
    // leaving it to be discovered. See [ResponseValidation] for why the FACTORY rather than a Validator.
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
     * shape as `ResilientRestClient`'s clock, and like it never used for a timestamp.
     */
    private val nanoTime: () -> Long = System::nanoTime,
) {
    private val logger: Logger = LoggerFactory.getLogger(ResilientRestClientFactory::class.java)

    /** Which backends validate, with which validator, and whether a violation fails the call - see [ResponseValidation]. */
    private val validation = ResponseValidation(properties.validation, validatorFactory)

    /** Standalone HTTP observation toggle (see [RestClientProperties.Observability]) - no sync-bridge coupling. */
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

    init {
        // Which backends actually validate, resolved the same way the clients resolve it. Worth a startup line
        // because both ways this silently does nothing are invisible at runtime: a backend simply not listed,
        // or no Bean Validation provider on the classpath at all, in which case no configuration can switch it
        // on for anyone.
        val validated =
            meshProperties
                .resolveAll()
                .values
                .map { it.alias }
                .filter(validation::isEnabledFor)
        logger.info(
            "Response Bean Validation: default={}, validated endpoints={} (provider {})",
            properties.validation.enabled,
            validated.sorted().ifEmpty { listOf("none") },
            validation.providerVerdict,
        )
    }

    /** The mesh-derived ladder per backend, keyed by the endpoint alias (which must equal the backend name). */
    private val meshByBackend: Map<String, ResolvedTimeouts> = meshProperties.resolveAll().values.associateBy { it.alias }

    /** Global mesh transport caps (connect / pending-acquire / max-connections) shared by the pool and clients. */
    private val meshTransport: MeshProperties.Transport = meshProperties.defaults.transport

    /** Name of the single shared Apache pool: the [RestClientMetrics.POOL_TAG] value on its gauges, its evictor's thread name. */
    private val poolName: String = RestClientMetrics.POOL_NAME

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
     * `maxInMemorySize` in bytes: the cap on how much of a response body the client will buffer. Handed to every
     * client, which enforces it in its bounded read and names it on its buffer-limit line.
     */
    private val maxInMemoryBytes: Int = properties.maxInMemorySize.toBytes().toInt()

    /**
     * The single connection pool shared by every target's HttpClient, built once and closed in [shutdown].
     * Apache keeps a separate sub-pool per route, so distinct sidecar ports stay isolated - which is also why
     * the per-route limit is raised to the total: every endpoint IS one route, and Apache's per-route default
     * of five would otherwise cap each backend at five connections regardless of `maxConnections`.
     *
     * `maxConnections` / `pendingAcquireTimeout` come from the mesh transport config; the lifetime bound from
     * [RestClientProperties.Pool]. A zero `maxLifeTime` leaves the time-to-live unset, Apache's "no limit".
     */
    private val connectionManager: PoolingHttpClientConnectionManager =
        PoolingHttpClientConnectionManagerBuilder
            .create()
            .setMaxConnTotal(meshTransport.maxConnections)
            .setMaxConnPerRoute(meshTransport.maxConnections)
            .setDefaultConnectionConfig(
                ConnectionConfig
                    .custom()
                    .setConnectTimeout(Timeout.ofMilliseconds(meshTransport.connectTimeoutMs))
                    .apply {
                        properties.pool.maxLifeTime
                            .takeUnless { it.isZero }
                            ?.let { setTimeToLive(TimeValue.ofMilliseconds(it.toMillis())) }
                    }.build(),
            ).build()

    /**
     * The pool's background eviction, the counterpart of Reactor Netty's `evictInBackground`: every interval,
     * connections idle for longer than `maxIdleTime` and connections past their time-to-live are closed. One
     * schedule for the one pool - Apache's own `evictIdleConnections` would start a thread per HttpClient, i.e.
     * one per endpoint, for the same pool. `null` when the interval is zero, which is "off".
     */
    private val evictor: ScheduledExecutorService? =
        properties.pool.evictInBackground
            .takeUnless { it.isZero }
            ?.let { interval ->
                Executors
                    .newSingleThreadScheduledExecutor(Thread.ofVirtual().name("$poolName-evictor").factory())
                    .also { scheduler ->
                        val maxIdle = TimeValue.ofMilliseconds(properties.pool.maxIdleTime.toMillis())
                        scheduler.scheduleWithFixedDelay(
                            {
                                connectionManager.closeExpired()
                                connectionManager.closeIdle(maxIdle)
                            },
                            interval.toMillis(),
                            interval.toMillis(),
                            TimeUnit.MILLISECONDS,
                        )
                    }
            }

    init {
        // Pool metrics: the same question the reactor twin's `.metrics(true)` answers - how full is the pool.
        // Apache's own binder (httpclient5-observation; Micrometer deprecated its hc5 binder in favour of it)
        // reads the manager off a client BUILDER, so a throwaway builder over the shared manager is handed to
        // it once, here - never per endpoint, which would register the same gauges once per client.
        meterRegistry?.let { registry ->
            ConnPoolMeters.bindTo(
                HttpClients.custom().setConnectionManager(connectionManager),
                registry,
                MetricConfig
                    .builder()
                    .prefix(RestClientMetrics.POOL_METER_PREFIX)
                    .addCommonTag(RestClientMetrics.POOL_TAG, poolName)
                    .build(),
            )
        }
    }

    /** Caches one client per logical name so [create] is idempotent - repeated calls return the same instance. */
    private val cache = ConcurrentHashMap<String, ResilientRestClient>()

    /** The per-endpoint transports, kept so [shutdown] can close them ahead of the pool they share. */
    private val httpClients = ConcurrentHashMap<String, CloseableHttpClient>()

    /**
     * Returns the resilient client for [backendName], building it on first request and caching it after.
     * Everything per-endpoint (base URL, Host header, timeouts) comes from the `app.mesh.endpoints[]` entry
     * whose alias equals [backendName].
     *
     * @throws IllegalStateException if no mesh endpoint with that alias is configured - failing fast at
     *         startup beats a confusing 404/connection error at the first call.
     */
    fun create(backendName: String): ResilientRestClient = cache.computeIfAbsent(backendName, ::build)

    private fun build(backendName: String): ResilientRestClient {
        val endpoint =
            meshByBackend[backendName]
                ?: error(
                    "No mesh endpoint configured for backend '$backendName'. " +
                        "Add an app.mesh.endpoints[] entry whose alias equals '$backendName'.",
                )

        val httpClient = buildHttpClient(endpoint)
        httpClients[backendName] = httpClient

        val restClient =
            baseBuilder
                .clone()
                .baseUrl(endpoint.baseUrl)
                // Replaces whatever request factory Boot put on the builder from its `spring.http.client.*`
                // settings: these clients run on the mesh ladder's timeouts and the shared pool above, which the
                // host's global HTTP client settings know nothing about. Deliberate, and the same split the
                // reactive twin makes for its connector.
                .requestFactory(HttpComponentsClientHttpRequestFactory(httpClient))
                // Host routes the sidecar to the upstream (from the triplet); set() (not add) so re-applying
                // never accumulates values. Apache honours an explicit Host header - its own interceptor only
                // adds one when none is present. Extra static headers are applied on top.
                .defaultHeaders { headers ->
                    headers.set(HttpHeaders.HOST, endpoint.host)
                    endpoint.headers.forEach { (name, value) -> headers.set(name, value) }
                }.defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
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
            validation.validatorFor(backendName),
            validation.failOnViolation,
            responseBodySize(backendName),
        )
    }

    /**
     * The response-size summary for one backend, or `null` when the host wired no [MeterRegistry].
     *
     * **What it is for.** [RestClientProperties.maxInMemorySize] is a ceiling that has to be chosen, and without
     * this meter there is nothing to choose it from. legatium's body capture is bounded by its own
     * `max-body-bytes` and says nothing about this limit; the limit exception says only that the limit was
     * passed and what the peer declared on the wire.
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
                .builder(RestClientMetrics.RESPONSE_BODY_SIZE)
                .description("Decoded response body size in UTF-8 bytes, for sizing app.restclient.max-in-memory-size")
                .baseUnit(RestClientMetrics.RESPONSE_BODY_BASE_UNIT)
                .tag(RestClientMetrics.ADAPTER_TAG, backendName)
                .register(registry)
        }

    /**
     * One HttpClient per endpoint over the shared pool, carrying that endpoint's response timeout. The
     * per-endpoint responseTimeout is tighter than that endpoint's TimeLimiter budget by construction, so it
     * never drifts past it; its expiry surfaces as a `ResourceAccessException` over a `SocketTimeoutException`,
     * which the circuit breaker records (and legatium classifies as `timeout`).
     *
     * Three Apache defaults are switched off because Reactor Netty does not have them and the ladder does not
     * expect them: automatic retries (the sidecar owns retries; an in-app second attempt would spend the ladder's
     * budget twice without telling anyone), redirect following, and the cookie store. Content compression and
     * keep-alive stay on - the twin enables both explicitly, Apache has them on by default.
     */
    private fun buildHttpClient(endpoint: ResolvedTimeouts): CloseableHttpClient =
        HttpClients
            .custom()
            .setConnectionManager(connectionManager)
            .setConnectionManagerShared(true)
            .setDefaultRequestConfig(
                RequestConfig
                    .custom()
                    .setConnectionRequestTimeout(Timeout.ofMilliseconds(meshTransport.pendingAcquireTimeoutMs))
                    .setResponseTimeout(Timeout.ofMilliseconds(endpoint.responseTimeout.toMillis()))
                    .setRedirectsEnabled(false)
                    .build(),
            ).disableAutomaticRetries()
            .disableRedirectHandling()
            .disableCookieManagement()
            .build()

    /**
     * Drains the shared pool on context shutdown, waiting up to [poolDisposeTimeout] for leased connections to
     * come back, then closes it.
     *
     * Apache's pool has no "dispose when drained" of its own: `close` discards leased connections on the spot.
     * The wait is therefore made here, bounded, and its expiry is an OUTCOME, not a failure - reported as the
     * statement it is (the pool did not drain in time and is being dropped anyway), never as an exception out
     * of a destroy callback, where Spring could only log a stack trace at the moment someone is looking for one.
     *
     * The wait also ends when the calling thread is INTERRUPTED - a container tearing the context down under a
     * deadline of its own, a test executor's `shutdownNow`. `parkNanos` returns at once on a thread whose
     * interrupt flag is set and leaves the flag set, so without this exit the loop would spin hot until the
     * deadline instead of waiting. The flag is left for the caller, as an interrupt always is.
     *
     * Idempotent: Spring calls it once, tests may call it twice; a closed pool reports no leased connections and
     * closes again without complaint.
     */
    @PreDestroy
    fun shutdown() {
        evictor?.shutdownNow()
        httpClients.values.forEach { it.close(CloseMode.GRACEFUL) }
        val deadline = nanoTime() + poolDisposeTimeout.toNanos()
        while (connectionManager.totalStats.leased > 0 && nanoTime() < deadline && !Thread.currentThread().isInterrupted) {
            LockSupport.parkNanos(DRAIN_POLL_NANOS)
        }
        val leased = connectionManager.totalStats.leased
        if (leased > 0) {
            logger.warn(
                "Connection pool '{}' did not drain {} - dropping it with {} connection(s) still leased",
                poolName,
                if (Thread.currentThread().isInterrupted) "because the shutdown thread was interrupted" else "within $poolDisposeTimeout",
                leased,
            )
        }
        connectionManager.close(CloseMode.GRACEFUL)
    }

    private companion object {
        /** How often [shutdown] re-checks the pool while waiting for it to drain. */
        val DRAIN_POLL_NANOS: Long = TimeUnit.MILLISECONDS.toNanos(50)
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
     * The one pool's name: the [POOL_TAG] value, the evictor thread's name and the name the shutdown line
     * reports. A constant, not a property: one factory, one pool, and no deployment runs two in one registry.
     */
    const val POOL_NAME: String = "restclient-pool"

    /** Decoded response body size per call - the input for sizing `app.restclient.max-in-memory-size`. */
    const val RESPONSE_BODY_SIZE: String = "restclient.response.body.size"

    /**
     * BYTES - the same unit as `app.restclient.max-in-memory-size`, which is the whole point of the meter.
     *
     * Sampled with [utf8Size], not `String.length`. The two agree on ASCII and diverge in exactly one
     * direction: a `length` of N encodes to between N and 3N UTF-8 bytes, so counting UTF-16 units would
     * UNDER-state the buffered size - the direction that quietly sizes a ceiling too small.
     */
    const val RESPONSE_BODY_BASE_UNIT: String = "bytes"

    /**
     * Identifies which downstream this sample came from. Named after the `adapter_*` family of the log fields,
     * so a meter and a log line can be correlated on sight: the value is exactly what the exchange event and
     * this module's transport lines carry as `adapter_name`.
     *
     * **The value is the mesh ENDPOINT alias, not an adapter class.** An adapter that owns two endpoints owns two
     * values, so a filter on this tag selects one CALL, not one adapter.
     */
    const val ADAPTER_TAG: String = "adapter"

    /**
     * The UTF-8 encoded size of [text], counted in one pass and without allocating.
     *
     * Equivalent to `text.toByteArray(Charsets.UTF_8).size` for every input, including unpaired surrogates -
     * `RestClientMetricsTest` pins that against the JDK encoder rather than against hand-written expectations.
     * The point of not simply calling `toByteArray` is the copy: on the hot path that is a second full buffer
     * per response, up to `max-in-memory-size` each, to produce a single number.
     *
     * The surrogate handling is the part worth reading. A valid high/low pair is ONE code point in four bytes
     * and consumes two UTF-16 units; an UNPAIRED surrogate is not encodable, and the JDK encoder substitutes
     * `?` - one byte - rather than failing. Both cases are counted the way the encoder counts them, so the
     * meter can never disagree with the reader about the same body.
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
}
