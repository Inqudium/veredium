package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import org.apache.hc.client5.http.config.ConnectionConfig
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder
import org.apache.hc.client5.http.observation.MetricConfig
import org.apache.hc.client5.http.observation.binder.ConnPoolMeters
import org.apache.hc.core5.concurrent.Cancellable
import org.apache.hc.core5.http.ClassicHttpRequest
import org.apache.hc.core5.io.CloseMode
import org.apache.hc.core5.util.TimeValue
import org.apache.hc.core5.util.Timeout
import org.slf4j.Logger
import org.springframework.http.HttpMethod
import org.springframework.http.client.ClientHttpRequest
import org.springframework.http.client.ClientHttpRequestFactory
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import java.net.URI
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * The Apache HttpClient 5 engine ([RestClientEngine.APACHE], the default): one shared
 * `PoolingHttpClientConnectionManager` for every backend on it, one `CloseableHttpClient` per backend over that
 * pool carrying the backend's response timeout, wrapped in Spring's [HttpComponentsClientHttpRequestFactory].
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
 * | `maxIdleTime` / `maxLifeTime` / `evictInBackground` | `closeIdle` / `ConnectionConfig.timeToLive` + `closeExpired`, on the transport's own schedule |
 * | `compress(true)` / `keepAlive(true)`         | Apache's defaults (content compression and keep-alive are on)         |
 * | `.metrics(true)` on the pool                 | Apache's `ConnPoolMeters` (httpclient5-observation), see [RestClientMetrics] |
 * | `.metrics(true)` per request, URI-templated  | none - Spring's `http.client.requests` observation carries the template |
 *
 * What Apache does by default and Reactor Netty does not is switched OFF, so the two clients behave alike on
 * the wire: no automatic retry of a failed idempotent request (the sidecar owns retries, and a second attempt
 * would double-count against the ladder), no redirect following, no cookie jar.
 *
 * **Cancellation.** An interrupted worker is released by the JDK socket implementation itself: since JDK 13 a
 * blocking socket read reacts to the interrupt by closing the socket, the read fails at once with
 * `SocketException: Closed by interrupt`, and a pool lease and a connect are interruptible too. Apache then
 * discards the half-used connection.
 *
 * **A body not read to its end.** Apache does NOT discard it on close - it reads it to the end, to keep the
 * connection: Spring's response close consumes the entity, and Apache's content streams drain the rest of the
 * message in their own close. For a body refused at the buffer limit that is the rest of a body nobody wants,
 * read on the worker with its permit and its connection, for as long as the peer takes - past the TimeLimiter
 * budget, a client-side refusal would end as a timeout the breaker records. [requestFactory] therefore hands out
 * an [AbortingRequest]: a response closed before its end cancels the Apache request first, which discards the
 * connection at once.
 */
internal class ApacheEngineTransport(
    settings: Settings,
) : EngineTransport(RestClientEngine.APACHE) {
    private val meshTransport = settings.transport
    private val pool = settings.pool

    /** Name of the single shared Apache pool: the [RestClientMetrics.POOL_TAG] value on its gauges, its evictor's thread name. */
    private val poolName: String = RestClientMetrics.POOL_NAME

    /**
     * The single connection pool shared by every target's HttpClient, built once and closed in [shutdown].
     * Apache keeps a separate sub-pool per route, so distinct sidecar ports stay isolated - which is also why
     * the per-route limit is raised to the total: every endpoint IS one route, and Apache's per-route default
     * of five would otherwise cap each backend at five connections regardless of `maxConnections`.
     *
     * `maxConnections` / `pendingAcquireTimeout` come from the mesh transport config; the lifetime bound from
     * [RestClientProperties.Pool]. A zero `maxLifeTime` leaves the time-to-live unset, Apache's "no limit".
     */
    internal val connectionManager: PoolingHttpClientConnectionManager =
        PoolingHttpClientConnectionManagerBuilder
            .create()
            .setMaxConnTotal(meshTransport.maxConnections)
            .setMaxConnPerRoute(meshTransport.maxConnections)
            .setDefaultConnectionConfig(
                ConnectionConfig
                    .custom()
                    .setConnectTimeout(Timeout.ofMilliseconds(meshTransport.connectTimeoutMs))
                    .apply {
                        pool.maxLifeTime
                            .takeUnless { it.isZero }
                            ?.let { setTimeToLive(TimeValue.ofMilliseconds(it.toMillis())) }
                    }.build(),
            ).build()

    /**
     * The pool's background eviction, the counterpart of Reactor Netty's `evictInBackground`: every interval,
     * connections idle for longer than `maxIdleTime` and connections past their time-to-live are closed. One
     * schedule for the one pool - Apache's own `evictIdleConnections` would start a thread per HttpClient, i.e.
     * one per endpoint, for the same pool. `null` when the interval is zero, which is "off".
     *
     * `internal` so the factory's integration test can see that [shutdown] stops it: the scheduler runs on a
     * virtual thread, which no thread listing shows.
     */
    internal val evictor: ScheduledExecutorService? =
        pool.evictInBackground
            .takeUnless { it.isZero }
            ?.let { interval ->
                Executors
                    .newSingleThreadScheduledExecutor(Thread.ofVirtual().name("$poolName-evictor").factory())
                    .also { scheduler ->
                        val maxIdle = TimeValue.ofMilliseconds(pool.maxIdleTime.toMillis())
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

    /** The pool gauges registered in the host's registry, removed again in [shutdown]. */
    private val boundPoolMeters: List<Meter> =
        settings.meterRegistry?.let { registry ->
            val before = registry.meters.toSet()
            // Pool metrics: the same question the reactor twin's `.metrics(true)` answers - how full is the pool.
            // Apache's own binder (httpclient5-observation; Micrometer deprecated its hc5 binder in favour of it)
            // reads the manager off a client BUILDER, so a throwaway builder over the shared manager is handed to
            // it once, here - never per endpoint, which would register the same gauges once per client. The binder
            // hands back nothing to deregister with, so what it registered is read off the registry.
            ConnPoolMeters.bindTo(
                HttpClients.custom().setConnectionManager(connectionManager),
                registry,
                MetricConfig
                    .builder()
                    .prefix(RestClientMetrics.POOL_METER_PREFIX)
                    .addCommonTag(RestClientMetrics.POOL_TAG, poolName)
                    .build(),
            )
            registry.meters.filterNot { it in before }
        } ?: emptyList()

    private val meterRegistry: MeterRegistry? = settings.meterRegistry

    /** The per-endpoint transports, kept so [shutdown] can close them ahead of the pool they share. */
    private val httpClients = CopyOnWriteArrayList<CloseableHttpClient>()

    /**
     * One HttpClient per endpoint over the shared pool, carrying that endpoint's response timeout, in Spring's
     * request factory. The request factory's own timeouts stay unset, so the client's `RequestConfig` stands.
     */
    override fun requestFactory(endpoint: ResolvedTimeouts): ClientHttpRequestFactory = AbortingRequestFactory(buildHttpClient(endpoint).also { httpClients += it })

    /**
     * The per-endpoint responseTimeout is tighter than that endpoint's TimeLimiter budget by construction, so it
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

    override val applied: List<String> =
        listOf(
            "connect-timeout-ms=${meshTransport.connectTimeoutMs} (ConnectionConfig.connectTimeout)",
            "the endpoint's response timeout (RequestConfig.responseTimeout, the socket timeout of every read)",
            "max-connections=${meshTransport.maxConnections} (maxConnTotal and maxConnPerRoute)",
            "pending-acquire-timeout-ms=${meshTransport.pendingAcquireTimeoutMs} (RequestConfig.connectionRequestTimeout)",
            "pool.max-idle-time=${pool.maxIdleTime} (closeIdle)",
            "pool.max-life-time=${pool.maxLifeTime} (ConnectionConfig.timeToLive, 0 = unbounded)",
            "pool.evict-in-background=${pool.evictInBackground} (the eviction schedule, 0 = off)",
        )

    override val notApplied: List<String> = emptyList()

    override val poolMeters: String =
        if (settings.meterRegistry != null) {
            "${RestClientMetrics.POOL_METER_PREFIX}.pool.{leased,available,pending}{${RestClientMetrics.POOL_TAG}=$poolName}"
        } else {
            "none (no MeterRegistry)"
        }

    override val cancellation: String = "the interrupted worker's socket read fails at once (the JDK closes the socket), the connection is discarded"

    override val isClosed: Boolean get() = connectionManager.isClosed

    override fun connectionsInUse(): Int = connectionManager.totalStats.leased

    /**
     * Drains the shared pool, waiting until the deadline for leased connections to come back, then closes it.
     *
     * Apache's pool has no "dispose when drained" of its own: `close` discards leased connections on the spot.
     * The wait is therefore made here, bounded, and its expiry is an OUTCOME, not a failure - reported as the
     * statement it is (the pool did not drain in time and is being dropped anyway), never as an exception out
     * of a destroy callback, where Spring could only log a stack trace at the moment someone is looking for one.
     *
     * Idempotent: a closed pool reports no leased connections and closes again without complaint. The pool
     * gauges leave the host's registry with the pool, as Reactor Netty's do.
     */
    override fun shutdown(
        deadlineNanos: Long,
        nanoTime: () -> Long,
        window: Duration,
        logger: Logger,
    ) {
        evictor?.shutdownNow()
        httpClients.forEach { it.close(CloseMode.GRACEFUL) }
        drainOrReport(engine, ::connectionsInUse, deadlineNanos, nanoTime, window, logger)
        connectionManager.close(CloseMode.GRACEFUL)
        boundPoolMeters.forEach { meterRegistry?.remove(it) }
    }

    /**
     * Spring's request factory, handing out [AbortingRequest]s whose abort cancels the Apache request (class KDoc,
     * "A body not read to its end"). Still an [HttpComponentsClientHttpRequestFactory], so everything Spring
     * derives from the client - its request config, the context - stays as it was.
     */
    private class AbortingRequestFactory(
        httpClient: CloseableHttpClient,
    ) : HttpComponentsClientHttpRequestFactory(httpClient) {
        private val created = ThreadLocal<Cancellable>()

        override fun createRequest(
            uri: URI,
            httpMethod: HttpMethod,
        ): ClientHttpRequest {
            val request = super.createRequest(uri, httpMethod)
            // Assumption: HttpComponentsClientHttpRequestFactory.createRequest builds its Apache request with
            // createHttpUriRequest on the calling thread, and that request is an HttpUriRequestBase (spring-web
            // 7.0). If that changes, the capture comes back empty and the exchange fails here, naming the cause,
            // rather than silently draining every refused body again.
            val apacheRequest =
                checkNotNull(created.get().also { created.remove() }) {
                    "HttpComponentsClientHttpRequestFactory no longer creates a cancellable request through " +
                        "createHttpUriRequest; veredium's Apache engine cannot abort unfinished responses"
                }
            // Assumption: cancelling an HttpUriRequestBase whose response is being read discards its connection
            // (httpclient5 5.6: InternalExecRuntime.cancel -> discardEndpoint), and is a no-op once the connection
            // went back to the pool.
            return AbortingRequest(request) { _, _ -> apacheRequest.cancel() }
        }

        override fun createHttpUriRequest(
            httpMethod: HttpMethod,
            uri: URI,
        ): ClassicHttpRequest = super.createHttpUriRequest(httpMethod, uri).also { created.set(it as? Cancellable) }
    }
}
