package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.engine.EngineDrain
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import org.eclipse.jetty.client.AbstractConnectionPool
import org.eclipse.jetty.client.ConnectionPool
import org.eclipse.jetty.client.DuplexConnectionPool
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.Request
import org.eclipse.jetty.client.transport.HttpDestination
import org.eclipse.jetty.http.HttpCookieStore
import org.springframework.http.HttpMethod
import org.springframework.http.client.ClientHttpRequest
import org.springframework.http.client.ClientHttpRequestFactory
import org.springframework.http.client.ClientHttpResponse
import org.springframework.http.client.JettyClientHttpRequestFactory
import java.io.FilterInputStream
import java.io.InputStream
import java.io.InterruptedIOException
import java.net.URI
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * The Jetty engine ([RestClientEngine.JETTY]): one Jetty `HttpClient` for every backend on it, started here and
 * stopped in [shutdown], and one Spring [JettyClientHttpRequestFactory] per backend over it carrying the
 * backend's response timeout.
 *
 * ## What maps, and what Jetty cannot express
 *
 * The settings it applies, each with the Jetty knob it lands on, are [applied], the ones it cannot express
 * [notApplied] - both on the engine's startup line. Why the latter have no knob, and what needs saying about the
 * former:
 *
 * - The response timeout is Spring's read timeout, which Jetty applies as the request's TOTAL timeout (queueing for
 *   a connection, connect, head and body together); the request's idle timeout is set to the same value, so a
 *   silent connection in use fails at the rung as on Apache.
 * - `pending-acquire-timeout-ms`: Jetty queues a request for a connection without a time bound of its own; the
 *   total timeout bounds the wait. The queue is bounded in COUNT instead, at Jetty's default of 1024 per
 *   destination.
 * - `pool.evict-in-background`: Jetty closes an idle connection on its own scheduler the moment the idle timeout
 *   passes; there is no interval.
 * - Pool meters: Jetty has no Micrometer binder.
 *
 * Assumption: Jetty decodes a gzip body transparently, through the `jetty-compression-gzip` decoder its
 * `HttpClient` discovers on start (a dependency of jetty-client 12.1) - [BoundedBodyReader] counts decoded bytes.
 *
 * Jetty's defaults that the Apache engine switches off are switched off here too: no redirects, no cookie store.
 * Workaround: Jetty's own `User-Agent` is removed, because Spring ADDS the client's header rather than replacing
 * Jetty's (spring-web 7.0) - a peer would otherwise see two.
 *
 * ## Cancellation, and the one thing Spring's request leaves behind
 *
 * Both blocking points of Spring's Jetty request honour an interrupt - waiting for the head, and reading the body
 * - so an interrupted worker ends at once. Workaround: Spring only stops WAITING - the Jetty request itself stays in
 * flight, holding its connection until the response arrives or the total timeout expires (spring-web 7.0). The
 * decorator returned by [requestFactory] closes that gap ([AbortingRequest], shared with the Apache engine): it aborts the Jetty request
 * when the exchange fails, when a body read fails (an interrupt included), and when the response is closed before
 * its body was read to the end. The Jetty request is captured where Spring creates it,
 * `HttpClient.newRequest(URI)`, on the thread that runs the exchange ([RequestCapturingHttpClient]).
 *
 * ## The interrupt a body read consumes
 *
 * Workaround: Jetty's body stream (`InputStreamResponseListener`) answers an interrupt during a read with a bare
 * `InterruptedIOException` - no message, no cause, and the thread's interrupt flag cleared by the
 * `InterruptedException` it caught (Jetty 12.1). The exchange log then has no sign that the caller abandoned the call, and
 * logs the cancellation as a failure. The response's body read therefore sets the flag again before it rethrows
 * that exception ([InterruptRestoringRequest]), as the JDK's own interruptible reads leave it set - the other
 * three engines need nothing of the kind.
 */
internal class JettyEngineTransport(
    settings: Settings,
) : EngineTransport(RestClientEngine.JETTY) {
    private val meshTransport = settings.transport
    private val pool = settings.pool
    private val window = settings.window
    private val logger = settings.logger

    override val name: String = ENGINE_NAME

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
        return AbortingRequest(InterruptRestoringRequest(request)) { _, cause -> jettyRequest.abort(cause) }
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
    override fun exchangesInFlight(): Int =
        httpClient.destinations.sumOf { destination ->
            val httpDestination = destination as? HttpDestination
            val active = (httpDestination?.connectionPool as? AbstractConnectionPool)?.activeConnectionCount ?: 0
            active + (httpDestination?.queuedRequestCount ?: 0)
        }

    /**
     * Waits until the deadline for the exchanges in flight to finish, then stops the client - which aborts what
     * is left and stops Jetty's own threads. Same shape and same line as the Apache pool's drain.
     *
     * A shutdown thread that was interrupted does not block on the stop. Assumption: Jetty's stop waits for its own
     * threads regardless of the interrupt (Jetty 12.1), so it is started on a thread of its own and left to complete - as Reactor
     * Netty's disposal is.
     */
    override fun shutdown(
        deadlineNanos: Long,
        nanoTime: () -> Long,
    ) {
        if (httpClient.isStopped) return
        EngineDrain.drainOrReport(logger, engine.key, ::exchangesInFlight, deadlineNanos, nanoTime, window)
        if (Thread.currentThread().isInterrupted) {
            Thread.ofVirtual().name("$ENGINE_NAME-stop").start { httpClient.stop() }
        } else {
            httpClient.stop()
        }
    }

    /**
     * Spring's Jetty request, whose response sets the thread's interrupt flag again when a body read ends in the
     * interrupt Jetty consumed (class KDoc). Inside [AbortingRequest], so the abort sees the same exception, and
     * inside the interceptors of the `RestClient`, so the exchange log sees the flag.
     */
    internal class InterruptRestoringRequest(
        delegate: ClientHttpRequest,
    ) : ForwardingClientHttpRequest(delegate) {
        override fun execute(): ClientHttpResponse {
            val response = delegate.execute()
            return object : ClientHttpResponse by response {
                private val restoringBody: InputStream by lazy {
                    object : FilterInputStream(response.body) {
                        override fun read(): Int = restoring { super.read() }

                        override fun read(
                            b: ByteArray,
                            off: Int,
                            len: Int,
                        ): Int = restoring { super.read(b, off, len) }
                    }
                }

                override fun getBody(): InputStream = restoringBody
            }
        }

        private companion object {
            fun restoring(read: () -> Int): Int =
                try {
                    read()
                } catch (e: InterruptedIOException) {
                    // Invariant: only the exact shape Jetty raises for an interrupted wait - its subclasses
                    // (a socket timeout among them) and a wrapped failure say nothing about the thread.
                    if (e.javaClass == InterruptedIOException::class.java && e.cause == null) {
                        Thread.currentThread().interrupt()
                    }
                    throw e
                }
        }
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

    companion object {
        /** The engine's [name]: the stem of the thread an interrupted shutdown stops the client on. */
        const val ENGINE_NAME: String = "restclient-jetty"
    }
}
