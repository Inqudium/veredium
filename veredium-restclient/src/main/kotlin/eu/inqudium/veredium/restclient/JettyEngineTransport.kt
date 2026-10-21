package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import org.eclipse.jetty.client.AbstractConnectionPool
import org.eclipse.jetty.client.ConnectionPool
import org.eclipse.jetty.client.DuplexConnectionPool
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.Request
import org.eclipse.jetty.client.transport.HttpDestination
import org.eclipse.jetty.http.HttpCookieStore
import org.slf4j.Logger
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatusCode
import org.springframework.http.client.ClientHttpRequest
import org.springframework.http.client.ClientHttpRequestFactory
import org.springframework.http.client.ClientHttpResponse
import org.springframework.http.client.JettyClientHttpRequestFactory
import java.io.FilterInputStream
import java.io.IOException
import java.io.InputStream
import java.net.URI
import java.nio.channels.AsynchronousCloseException
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * The Jetty engine ([RestClientEngine.JETTY]): one Jetty `HttpClient` for every backend on it, started here and
 * stopped in [shutdown], and one Spring [JettyClientHttpRequestFactory] per backend over it carrying the
 * backend's response timeout.
 *
 * ## What maps, and what Jetty cannot express
 *
 * | Setting                          | Here                                                                         |
 * |----------------------------------|------------------------------------------------------------------------------|
 * | `connect-timeout-ms`             | `HttpClient.connectTimeout`                                                  |
 * | per-endpoint response timeout    | Spring's read timeout, which Jetty applies as the request's TOTAL timeout (queueing for a connection, connect, head and body together), plus the request's idle timeout set here to the same value, so a silent connection in use fails at the rung as on Apache |
 * | `max-connections`                | `maxConnectionsPerDestination` (every endpoint behind the sidecar is one destination) |
 * | `pending-acquire-timeout-ms`     | none - Jetty queues a request for a connection without a time bound of its own; the total timeout above bounds the wait. The queue is bounded in COUNT instead, at Jetty's default of 1024 per destination |
 * | `pool.max-idle-time`             | `HttpClient.idleTimeout` - for idle pooled connections; an exchange in flight uses the request's idle timeout above. `0` is Jetty's "no idle timeout" |
 * | `pool.max-life-time`             | the connection pool's `maxDuration`, which Jetty enforces when a connection is LEASED (`0` = unbounded) |
 * | `pool.evict-in-background`       | none - Jetty closes an idle connection on its own scheduler the moment the idle timeout passes; there is no interval |
 * | pool meters                      | none - Jetty has no Micrometer binder                                        |
 *
 * Jetty's defaults that the Apache engine switches off are switched off here too: no redirects, no cookie store.
 * Jetty's own `User-Agent` is removed, because Spring ADDS the client's header rather than replacing Jetty's - a
 * peer would otherwise see two.
 *
 * ## Cancellation, and the one thing Spring's request leaves behind
 *
 * Both blocking points of Spring's Jetty request honour an interrupt - waiting for the head, and reading the body
 * - so an interrupted worker ends at once. But Spring only stops WAITING: the Jetty request itself stays in flight,
 * holding its connection until the response arrives or the total timeout expires. The decorator returned by
 * [requestFactory] closes that gap: it aborts the Jetty request when the exchange fails, when a body read fails
 * (an interrupt included), and when the response is closed before its body was read to the end - the moment
 * Apache would discard its connection. The Jetty request is captured where Spring creates it,
 * `HttpClient.newRequest(URI)`, on the thread that runs the exchange ([RequestCapturingHttpClient]).
 */
internal class JettyEngineTransport(
    settings: Settings,
) : EngineTransport(RestClientEngine.JETTY) {
    private val meshTransport = settings.transport
    private val pool = settings.pool

    /** The one client of this engine, started here. `internal` so a test can see it stop and read its pool. */
    internal val httpClient: RequestCapturingHttpClient =
        RequestCapturingHttpClient().apply {
            connectTimeout = meshTransport.connectTimeoutMs
            maxConnectionsPerDestination = meshTransport.maxConnections
            idleTimeout = pool.maxIdleTime.toMillis()
            isFollowRedirects = false
            httpCookieStore = HttpCookieStore.Empty()
            userAgentField = null
            httpClientTransport.connectionPoolFactory =
                ConnectionPool.Factory { destination ->
                    DuplexConnectionPool(destination, meshTransport.maxConnections).apply {
                        maxDuration = pool.maxLifeTime.toMillis()
                    }
                }
            start()
        }

    override fun requestFactory(endpoint: ResolvedTimeouts): ClientHttpRequestFactory {
        val responseTimeout = endpoint.responseTimeout
        val delegate = JettyClientHttpRequestFactory(httpClient).apply { setReadTimeout(responseTimeout) }
        return ClientHttpRequestFactory { uri, method -> abortable(delegate, uri, method, responseTimeout) }
    }

    /**
     * Spring's request for [uri], decorated so that an abandoned exchange aborts its Jetty request (class KDoc),
     * and with the Jetty request's idle timeout set to the response timeout.
     */
    private fun abortable(
        delegate: JettyClientHttpRequestFactory,
        uri: URI,
        method: HttpMethod,
        responseTimeout: Duration,
    ): ClientHttpRequest {
        val request = delegate.createRequest(uri, method)
        // Assumption: JettyClientHttpRequestFactory.createRequest builds its Jetty request with
        // HttpClient.newRequest(URI) on the calling thread (spring-web 7.0). If that changes, the capture
        // comes back empty and the exchange fails here, naming the cause, rather than silently losing the abort.
        val jettyRequest =
            checkNotNull(httpClient.takeCreated()) {
                "JettyClientHttpRequestFactory no longer creates its request through HttpClient.newRequest(URI); " +
                    "veredium's Jetty engine cannot abort abandoned exchanges"
            }
        jettyRequest.idleTimeout(responseTimeout.toMillis(), TimeUnit.MILLISECONDS)
        return AbortingRequest(request, jettyRequest)
    }

    override val applied: List<String> =
        listOf(
            "connect-timeout-ms=${meshTransport.connectTimeoutMs} (HttpClient.connectTimeout)",
            "the endpoint's response timeout (the request's total and idle timeout)",
            "max-connections=${meshTransport.maxConnections} (maxConnectionsPerDestination)",
            "pool.max-idle-time=${pool.maxIdleTime} (HttpClient.idleTimeout)",
            "pool.max-life-time=${pool.maxLifeTime} (the pool's maxDuration, checked at lease, 0 = unbounded)",
        )

    override val notApplied: List<String> =
        listOf(
            "pending-acquire-timeout-ms=${meshTransport.pendingAcquireTimeoutMs} " +
                "(a queued request waits within its total timeout; the queue is bounded at ${httpClient.maxRequestsQueuedPerDestination} per destination)",
            "pool.evict-in-background=${pool.evictInBackground} (Jetty closes idle connections when their idle timeout passes)",
        )

    override val poolMeters: String = "none (Jetty has no Micrometer binder)"

    override val cancellation: String =
        "an interrupted worker ends at once, and its Jetty request is aborted, releasing the connection"

    override val isClosed: Boolean get() = httpClient.isStopped

    /** Exchanges holding a connection or queued for one, over every destination. */
    override fun connectionsInUse(): Int =
        httpClient.destinations.sumOf { destination ->
            val httpDestination = destination as? HttpDestination
            val active = (httpDestination?.connectionPool as? AbstractConnectionPool)?.activeConnectionCount ?: 0
            active + (httpDestination?.queuedRequestCount ?: 0)
        }

    /**
     * Waits until the deadline for the exchanges in flight to finish, then stops the client - which aborts what
     * is left and stops Jetty's own threads. Same shape and same line as the Apache pool's drain.
     */
    override fun shutdown(
        deadlineNanos: Long,
        nanoTime: () -> Long,
        window: Duration,
        logger: Logger,
    ) {
        if (httpClient.isStopped) return
        drainOrReport(POOL_NAME, ::connectionsInUse, deadlineNanos, nanoTime, window, logger)
        httpClient.stop()
    }

    /**
     * Jetty's `HttpClient`, remembering the request it created last on the calling thread until
     * [takeCreated] collects it - the hook [abortable] needs, because Spring's request does not expose its Jetty
     * request. Set and taken within the same `createRequest` call, so a request never outlives that call here.
     */
    internal class RequestCapturingHttpClient : HttpClient() {
        private val created = ThreadLocal<Request>()

        override fun newRequest(uri: URI): Request = super.newRequest(uri).also(created::set)

        fun takeCreated(): Request? = created.get().also { created.remove() }
    }

    /** Spring's Jetty request; a failed exchange aborts [jettyRequest], a response comes back abort-aware. */
    private class AbortingRequest(
        delegate: ClientHttpRequest,
        private val jettyRequest: Request,
    ) : ForwardingClientHttpRequest(delegate) {
        override fun execute(): ClientHttpResponse {
            val response =
                try {
                    delegate.execute()
                } catch (e: IOException) {
                    // An interrupted wait for the head, a timeout, a transport failure: Spring stopped waiting,
                    // the Jetty request may still be in flight. Abort is a no-op on a request that already ended.
                    jettyRequest.abort(e)
                    throw e
                }
            return AbortingResponse(response, jettyRequest)
        }
    }

    /**
     * The response; aborts [jettyRequest] when a body read fails and when it is closed before the body was read
     * to its end, so the connection is released now instead of at the request's total timeout.
     */
    private class AbortingResponse(
        private val delegate: ClientHttpResponse,
        private val jettyRequest: Request,
    ) : ClientHttpResponse {
        @Volatile
        private var endOfBody = false

        private val guardedBody: InputStream by lazy {
            object : FilterInputStream(delegate.body) {
                override fun read(): Int = guarded { super.read() }

                override fun read(
                    b: ByteArray,
                    off: Int,
                    len: Int,
                ): Int = guarded { super.read(b, off, len) }

                private fun guarded(read: () -> Int): Int =
                    try {
                        read().also { if (it < 0) endOfBody = true }
                    } catch (e: IOException) {
                        jettyRequest.abort(e)
                        throw e
                    }
            }
        }

        override fun getBody(): InputStream = guardedBody

        override fun getHeaders() = delegate.headers

        override fun getStatusCode(): HttpStatusCode = delegate.statusCode

        override fun getStatusText(): String = delegate.statusText

        override fun close() {
            delegate.close()
            if (!endOfBody) jettyRequest.abort(AsynchronousCloseException())
        }
    }

    companion object {
        /** The pool's name on the shutdown line. */
        const val POOL_NAME: String = "restclient-jetty"
    }
}
