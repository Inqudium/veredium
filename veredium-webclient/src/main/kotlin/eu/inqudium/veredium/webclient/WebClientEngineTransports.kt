package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import io.netty.channel.ChannelOption
import io.netty.resolver.DefaultAddressResolverGroup
import org.apache.hc.client5.http.config.ConnectionConfig
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.config.TlsConfig
import org.apache.hc.client5.http.impl.IdleConnectionEvictor
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient
import org.apache.hc.client5.http.impl.async.HttpAsyncClients
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder
import org.apache.hc.client5.http.protocol.HttpClientContext
import org.apache.hc.core5.concurrent.DefaultThreadFactory
import org.apache.hc.core5.concurrent.FutureCallback
import org.apache.hc.core5.function.Supplier
import org.apache.hc.core5.http.HttpHost
import org.apache.hc.core5.http.nio.AsyncPushConsumer
import org.apache.hc.core5.http.nio.AsyncRequestProducer
import org.apache.hc.core5.http.nio.AsyncResponseConsumer
import org.apache.hc.core5.http.nio.HandlerFactory
import org.apache.hc.core5.http.protocol.HttpContext
import org.apache.hc.core5.http2.HttpVersionPolicy
import org.apache.hc.core5.io.CloseMode
import org.apache.hc.core5.reactor.IOReactorConfig
import org.apache.hc.core5.reactor.IOReactorStatus
import org.apache.hc.core5.util.TimeValue
import org.apache.hc.core5.util.Timeout
import org.eclipse.jetty.client.AbstractConnectionPool
import org.eclipse.jetty.client.ConnectionPool
import org.eclipse.jetty.client.DuplexConnectionPool
import org.eclipse.jetty.client.Request
import org.eclipse.jetty.client.transport.HttpDestination
import org.eclipse.jetty.http.HttpCookieStore
import org.slf4j.Logger
import org.springframework.core.io.buffer.DataBuffer
import org.springframework.http.HttpMethod
import org.springframework.http.client.reactive.ClientHttpConnector
import org.springframework.http.client.reactive.ClientHttpRequest
import org.springframework.http.client.reactive.ClientHttpResponse
import org.springframework.http.client.reactive.ClientHttpResponseDecorator
import org.springframework.http.client.reactive.HttpComponentsClientHttpConnector
import org.springframework.http.client.reactive.JdkClientHttpConnector
import org.springframework.http.client.reactive.JettyClientHttpConnector
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.netty.resources.ConnectionProvider
import reactor.netty.resources.LoopResources
import java.net.URI
import java.nio.channels.ClosedSelectorException
import java.time.Duration
import java.util.concurrent.CancellationException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import kotlin.concurrent.withLock
import org.eclipse.jetty.client.HttpClient as JettyHttpClient
import reactor.netty.http.client.HttpClient as ReactorHttpClient

/**
 * The resources of ONE engine, owned by one [ResilientWebClientFactory]: the pool, client or I/O reactor every
 * backend of that engine shares, and the connector each backend gets on top of it.
 *
 * An implementation is created only when the first client of its engine is built, which is why every class
 * that names an engine library sits behind this interface: the JVM resolves those classes when the
 * implementation is first used, so an engine nobody selected needs no library on the classpath.
 */
internal interface EngineTransport {
    val engine: WebClientEngine

    /** The connector for one backend - created once per backend, on the shared resources of this engine. */
    fun connectorFor(endpoint: ResolvedTimeouts): ClientHttpConnector

    /**
     * Releases the shared resources, waiting for in-flight exchanges no later than [deadlineNanos] on [nanoTime]'s
     * clock - a deadline SHARED by every engine of the factory, so that releasing several engines one after
     * another takes the drain bound once, not once per engine. Never throws for an outcome the transport can
     * report itself.
     */
    fun release(
        deadlineNanos: Long,
        nanoTime: () -> Long,
    )

    /** Whether [release] has completed - for the tests that pin the lifecycle. */
    fun isReleased(): Boolean

    companion object {
        /** How often a draining release re-checks the exchanges in flight. */
        val DRAIN_POLL_NANOS: Long = TimeUnit.MILLISECONDS.toNanos(50)

        /** The time left until [deadlineNanos], never negative. */
        fun remaining(
            deadlineNanos: Long,
            nanoTime: () -> Long,
        ): Duration = Duration.ofNanos(maxOf(0L, deadlineNanos - nanoTime()))
    }
}

/**
 * The engines in use by one factory, each created on first use and released together.
 *
 * Only the engines named in [enginesInUse] can ever be created; asking for another one is a programming
 * error, because the factory resolved every backend's engine at construction. Once [release] ran, no engine is
 * created any more: one created then would own threads and sockets that nothing releases.
 */
internal class EngineTransports(
    enginesInUse: Set<WebClientEngine>,
    private val logger: Logger,
    create: (WebClientEngine) -> EngineTransport,
) {
    private val transports: Map<WebClientEngine, Lazy<EngineTransport>> = enginesInUse.associateWith { engine -> lazy { create(engine) } }

    /**
     * Guards the creation of an engine against a concurrent [release]: without it, a client built while the
     * factory shuts down could create its engine after release() took its snapshot of the started ones. A lock
     * rather than `synchronized`, because the first client may be built on a virtual thread.
     */
    private val lifecycle = ReentrantLock()

    /** Set by [release]; afterwards [connectorFor] refuses instead of creating an engine nobody would release. */
    private var released = false

    fun connectorFor(
        engine: WebClientEngine,
        endpoint: ResolvedTimeouts,
    ): ClientHttpConnector =
        lifecycle.withLock {
            check(!released) {
                "The ResilientWebClientFactory was shut down; no client can be built after it released its HTTP engines"
            }
            val transport = transports[engine] ?: error("Engine '$engine' was not resolved for any backend of this factory")
            transport.value.connectorFor(endpoint)
        }

    /** The transports created so far - an engine that is in use but has not built a client yet is absent. */
    fun started(): Map<WebClientEngine, EngineTransport> = transports.filterValues { it.isInitialized() }.mapValues { it.value.value }

    /**
     * Releases every transport created so far, each on its own against the one shared [deadlineNanos]: a failing
     * release is logged and does not keep the others from releasing theirs. Idempotent.
     */
    fun release(
        deadlineNanos: Long,
        nanoTime: () -> Long,
    ) {
        val toRelease =
            lifecycle.withLock {
                released = true
                started().values
            }
        toRelease.forEach { transport ->
            try {
                transport.release(deadlineNanos, nanoTime)
            } catch (e: Exception) {
                logger.warn("HTTP engine '{}' failed to release its resources", transport.engine, e)
            }
        }
    }
}

/**
 * Reactor Netty: one [ConnectionProvider] and one event-loop group for every backend of the engine, one
 * [ReactorHttpClient] per backend carrying that backend's response timeout and its URI tagger.
 *
 * Reactor Netty keeps a separate sub-pool inside the provider per remote address AND per client configuration
 * (`PooledConnectionProvider` keys it by the address and the client's `channelHash`), and every backend's
 * [ReactorHttpClient] is configured on its own - its response timeout, its URI tagger. So `maxConnections` and
 * the pending-acquire limits apply per BACKEND (and per remote address), not to the engine as a whole: two
 * backends behind the same sidecar never share a connection (`WebClientEngineIntegrationTest` pins it).
 *
 * Nothing here is Reactor Netty's process-wide `HttpResources`, which every other Reactor Netty client of the
 * JVM shares - and which Boot's `ReactorResourceFactory` manages for the host's own clients: the loops are
 * created here (`runOn`), and the resolver is set explicitly, because `HttpClient`'s default resolver comes from
 * `HttpResources` even with own loops. The resolver is Netty's `DefaultAddressResolverGroup`, a stateless
 * singleton carrying no setting, which resolves through `InetAddress` - as the JDK, Jetty and Apache clients do.
 * The lookup runs on the event loop: nothing for the loopback sidecar (`localhost`), a blocking DNS query for a
 * remote base URL. Until the engine switch existed, this engine ran on the global loops and resolver.
 *
 * The loops are disposed [loopDisposeDelay] after the pool, not with it: closing a loop group closes every
 * channel on it at once, and the pool disposal deliberately lets an in-flight call finish on its connection. No
 * call outlives that delay (it is the factory's drain bound, longer than any response timeout), so the loops go
 * when nothing can still need them; the threads are daemons, so a JVM exiting before that is not held up.
 */
internal class ReactorNettyTransport(
    properties: WebClientProperties,
    private val meshTransport: MeshProperties.Transport,
    poolName: String,
    private val loopDisposeDelay: Duration,
    /** Waits for the pool's disposal, bounded by the time given - the factory's drain, which reports an expiry. */
    private val dispose: (Mono<Void>, Duration) -> Unit,
) : EngineTransport {
    override val engine: WebClientEngine = WebClientEngine.REACTOR_NETTY

    /** The engine's own event loops - named after the pool, daemon threads, Reactor Netty's default count. */
    internal val loops: LoopResources = LoopResources.create(poolName, LoopResources.DEFAULT_IO_WORKER_COUNT, true)

    /** Whether [release] already scheduled the loop disposal - a second shutdown must not schedule another. */
    private val loopsScheduled = AtomicBoolean(false)

    private val connectionProvider: ConnectionProvider =
        ConnectionProvider
            .builder(poolName)
            .maxConnections(meshTransport.maxConnections)
            .pendingAcquireMaxCount(properties.pool.pendingAcquireMaxCount)
            .pendingAcquireTimeout(Duration.ofMillis(meshTransport.pendingAcquireTimeoutMs))
            .maxIdleTime(properties.pool.maxIdleTime)
            .maxLifeTime(properties.pool.maxLifeTime)
            .evictInBackground(properties.pool.evictInBackground)
            // A GRACEFUL disposal, bounded by the factory's drain bound: without it `disposeLater()` fails every
            // call still waiting for a connection at once (`PoolShutdownException`). With it the pool takes no new
            // acquire, serves the waiting ones and completes once every connection came back - or fails with a
            // TimeoutException at the bound, which Reactor Netty logs at ERROR; the factory's own wait, bounded by
            // what is left of the shared deadline, has reported the pool with a WARN before that.
            .disposeTimeout(loopDisposeDelay)
            .metrics(true)
            .build()

    override fun connectorFor(endpoint: ResolvedTimeouts): ClientHttpConnector = ReactorClientHttpConnector(httpClientFor(endpoint))

    /**
     * The per-backend client. Response and connect timeouts are the mesh-derived values; the per-endpoint
     * response timeout is tighter than that endpoint's TimeLimiter budget by construction, so it never drifts
     * past it, and its expiry surfaces as a `WebClientRequestException` the circuit breaker records.
     */
    internal fun httpClientFor(endpoint: ResolvedTimeouts): ReactorHttpClient {
        // Scoped to THIS endpoint's template - see WebClientMetrics.uriTagger for why it is not the full set.
        val uriTag = WebClientMetrics.uriTagger(endpoint.pathTemplate)
        return ReactorHttpClient
            .create(connectionProvider)
            .runOn(loops)
            .resolver(DefaultAddressResolverGroup.INSTANCE)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, meshTransport.connectTimeoutMs.toInt())
            .responseTimeout(endpoint.responseTimeout)
            .compress(true)
            .keepAlive(true)
            // Pool metrics come from the shared ConnectionProvider; these add per-request transport metrics.
            // The URI tag is templated centrally so a path variable cannot leak cardinality.
            .metrics(true) { uri -> uriTag(uri) }
    }

    override fun release(
        deadlineNanos: Long,
        nanoTime: () -> Long,
    ) {
        dispose(connectionProvider.disposeLater(), EngineTransport.remaining(deadlineNanos, nanoTime))
        if (loopsScheduled.compareAndSet(false, true)) {
            Mono
                .delay(loopDisposeDelay)
                .then(loops.disposeLater())
                .subscribe()
        }
    }

    /** The pool is disposed; the loops follow [loopDisposeDelay] later (class KDoc). */
    override fun isReleased(): Boolean = connectionProvider.isDisposed
}

/**
 * The JDK's `java.net.http.HttpClient`: ONE client for every backend of the engine, one connector per backend
 * carrying that backend's response timeout as the request timeout (`HttpRequest.timeout`, which bounds the
 * wait for the response headers).
 *
 * The client is pinned to HTTP/1.1, the protocol Reactor Netty speaks by default: left at its default the JDK
 * client offers an `h2c` upgrade on every plaintext request. Redirects are not followed, as with Reactor Netty.
 * The client owns its executor and selector thread and terminates them in [release].
 *
 * Not applied, because the client has no per-client equivalent: `max-connections`, `pending-acquire-timeout-ms`
 * and the four `pool.*` knobs. The JDK's pool settings exist only as JVM-wide system properties
 * (`jdk.httpclient.connectionPoolSize`, `jdk.httpclient.keepalive.timeout`), and setting one would change every
 * other `java.net.http` client of the process.
 */
internal class JdkTransport(
    meshTransport: MeshProperties.Transport,
    private val logger: Logger,
) : EngineTransport {
    override val engine: WebClientEngine = WebClientEngine.JDK

    private val client: java.net.http.HttpClient =
        java.net.http.HttpClient
            .newBuilder()
            .version(java.net.http.HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofMillis(meshTransport.connectTimeoutMs))
            .followRedirects(java.net.http.HttpClient.Redirect.NEVER)
            .build()

    override fun connectorFor(endpoint: ResolvedTimeouts): ClientHttpConnector = JdkClientHttpConnector(client).apply { setReadTimeout(endpoint.responseTimeout) }

    /**
     * An orderly shutdown: no new exchange is accepted, in-flight ones run to completion, and the wait for that is
     * bounded by the factory's shutdown deadline - past it the client is shut down hard, with a WARN.
     */
    override fun release(
        deadlineNanos: Long,
        nanoTime: () -> Long,
    ) {
        client.shutdown()
        val terminated =
            try {
                client.awaitTermination(EngineTransport.remaining(deadlineNanos, nanoTime))
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                false
            }
        if (!terminated) {
            logger.warn("HTTP engine '{}' did not drain by the factory's shutdown deadline - closing it with exchanges still open", engine)
            client.shutdownNow()
            // shutdownNow() aborts the exchanges and returns before the client's threads have ended; a short,
            // bounded wait makes `isReleased` true on return in the normal case without holding up a shutdown.
            try {
                client.awaitTermination(TERMINATION_AFTER_ABORT)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
    }

    private companion object {
        /** How long [release] waits for the client to terminate after it aborted the exchanges in flight. */
        val TERMINATION_AFTER_ABORT: Duration = Duration.ofSeconds(1)
    }

    override fun isReleased(): Boolean = client.isTerminated
}

/**
 * Jetty's `HttpClient`: ONE client for every backend of the engine, started here and stopped in [release]; one
 * connector per backend that puts that backend's response timeout on each request as Jetty's per-request idle
 * timeout.
 *
 * Mapped: connect timeout, `max-connections` (per destination, i.e. per remote address like Reactor Netty's
 * sub-pools), `pool.pending-acquire-max-count` (as the per-destination request queue beyond the connections,
 * see [jettyQueueCapacity]), `pool.max-idle-time`
 * (the connection idle timeout) and `pool.max-life-time` (the connection pool's maximum connection duration).
 * Redirects are not followed and no cookie is stored, as with Reactor Netty.
 *
 * Not applied: `pending-acquire-timeout-ms` - Jetty has no timeout for a request waiting in its queue - and
 * `pool.evict-in-background`, because Jetty closes an idle connection on that connection's own idle timer
 * rather than in a background sweep.
 *
 * Shutdown drains, as on every other engine: Jetty's `stop` aborts the exchanges still running, so [release] first
 * waits for them - the same wait, on the same counters, as the blocking module's Jetty engine.
 */
internal class JettyTransport(
    properties: WebClientProperties,
    meshTransport: MeshProperties.Transport,
    private val logger: Logger,
) : EngineTransport {
    override val engine: WebClientEngine = WebClientEngine.JETTY

    private val client: JettyHttpClient =
        JettyHttpClient().apply {
            // Jetty names its thread pool after the client: the prefix of this engine's I/O threads.
            name = THREAD_NAME_PREFIX
            connectTimeout = meshTransport.connectTimeoutMs
            idleTimeout = properties.pool.maxIdleTime.toMillis()
            maxConnectionsPerDestination = meshTransport.maxConnections
            maxRequestsQueuedPerDestination = jettyQueueCapacity(meshTransport.maxConnections, properties.pool.pendingAcquireMaxCount)
            isFollowRedirects = false
            httpCookieStore = HttpCookieStore.Empty()
            val maxLifeMillis = properties.pool.maxLifeTime.toMillis()
            transport.connectionPoolFactory =
                ConnectionPool.Factory { destination ->
                    DuplexConnectionPool(destination, meshTransport.maxConnections).apply { maxDuration = maxLifeMillis }
                }
            start()
        }

    private val connector = JettyClientHttpConnector(client)

    override fun connectorFor(endpoint: ResolvedTimeouts): ClientHttpConnector = JettyEndpointConnector(connector, endpoint.responseTimeout)

    /**
     * Waits until [deadlineNanos] for the exchanges in flight to finish, then stops the client - its connections
     * closed, its thread pool stopped, and whatever is still running aborted, with one WARN saying how many.
     *
     * The wait also ends when the calling thread is INTERRUPTED; the flag is left for the caller. Jetty's stop then
     * runs on a thread of its own and is left to complete: it waits for Jetty's own threads regardless of an
     * interrupt, and the shutdown thread must not.
     */
    override fun release(
        deadlineNanos: Long,
        nanoTime: () -> Long,
    ) {
        if (client.isStopped) return
        while (exchangesInFlight() > 0 && nanoTime() - deadlineNanos < 0 && !Thread.currentThread().isInterrupted) {
            LockSupport.parkNanos(EngineTransport.DRAIN_POLL_NANOS)
        }
        val interrupted = Thread.currentThread().isInterrupted
        val left = exchangesInFlight()
        if (left > 0) {
            logger.warn(
                "HTTP engine '{}' did not drain {} - stopping it with {} exchange(s) still in flight",
                engine,
                if (interrupted) "because the shutdown thread was interrupted" else "by the factory's shutdown deadline",
                left,
            )
        }
        if (interrupted) {
            Thread.ofVirtual().name("$THREAD_NAME_PREFIX-stop").start { client.stop() }
        } else {
            client.stop()
        }
    }

    /** Exchanges holding a connection or queued for one, over every destination - what [release] waits on. */
    internal fun exchangesInFlight(): Int =
        client.destinations.sumOf { destination ->
            val httpDestination = destination as? HttpDestination
            val active = (httpDestination?.connectionPool as? AbstractConnectionPool)?.activeConnectionCount ?: 0
            active + (httpDestination?.queuedRequestCount ?: 0)
        }

    override fun isReleased(): Boolean = client.isStopped

    companion object {
        /** The client's name, and so the name prefix of this engine's I/O threads. */
        const val THREAD_NAME_PREFIX: String = "veredium-webclient-jetty"

        /**
         * The Jetty queue capacity that means what `pool.pending-acquire-max-count` means on Reactor Netty: how
         * many calls may WAIT beyond the [maxConnections] that hold a connection.
         *
         * Assumption: Jetty puts EVERY request into the destination's queue first and keeps it there until it is
         * on a connection - including while a new connection is being opened for it - and rejects a request once
         * the queue holds `maxRequestsQueuedPerDestination` (`HttpDestination.send` -> `enqueue`, jetty-client
         * 12.1). A capacity of the count alone would therefore reject a burst on a cold pool at the count, though
         * the pool would serve connections + count of it, as Reactor Netty does. So the capacity is
         * [maxConnections] + [pendingAcquireMaxCount], saturating at `Int.MAX_VALUE`; `-1` (unbounded) is the
         * largest capacity Jetty accepts. The price, stated rather than hidden: once every connection is busy,
         * up to [maxConnections] more calls than the count may wait - the overshoot the bulkheads exist to shed
         * before it is reached. `0` cannot be expressed at all and is refused at startup
         * ([WebClientEngineChecks.requireJettyQueue]).
         */
        fun jettyQueueCapacity(
            maxConnections: Int,
            pendingAcquireMaxCount: Int,
        ): Int =
            if (pendingAcquireMaxCount < 0) {
                Int.MAX_VALUE
            } else {
                (maxConnections.toLong() + pendingAcquireMaxCount).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
            }
    }
}

/**
 * One backend's Jetty connector: Spring's [JettyClientHttpConnector] (shared by every backend of the engine), with
 * the backend's response timeout put on the native request as its idle timeout before the request is sent.
 *
 * The idle timeout rather than Jetty's total `timeout`, because it is what Reactor Netty's `responseTimeout`
 * means as well - the longest silence while waiting for and reading the response - and a total timeout would
 * also count the queue and connect phases, which the mesh ladder leaves to the TimeLimiter.
 */
internal class JettyEndpointConnector(
    val delegate: JettyClientHttpConnector,
    private val responseTimeout: Duration,
) : ClientHttpConnector {
    override fun connect(
        method: HttpMethod,
        uri: URI,
        requestCallback: Function<in ClientHttpRequest, Mono<Void>>,
    ): Mono<ClientHttpResponse> =
        Mono.defer {
            val native = AtomicReference<Request?>()
            delegate
                .connect(method, uri) { request ->
                    native.set(request.getNativeRequest<Request>().idleTimeout(responseTimeout.toMillis(), TimeUnit.MILLISECONDS))
                    requestCallback.apply(request)
                }.map<ClientHttpResponse> { response -> AbortJettyOnBodyCancel(response, native) }
        }
}

/**
 * A Jetty response whose body, when cancelled before it completed, aborts the native request - what Spring's
 * connector does not do: cancelling its content publisher only stops the delivery, and Jetty goes on receiving
 * the rest of the body on the connection, for as long as the peer sends it. A codec refusing an over-limit body,
 * or the TimeLimiter cancelling while the body arrives, would keep the connection - and its slot of
 * `max-connections` - until the peer stops.
 */
private class AbortJettyOnBodyCancel(
    response: ClientHttpResponse,
    private val native: AtomicReference<Request?>,
) : ClientHttpResponseDecorator(response) {
    // Assumption: aborting a Jetty request whose response already completed is a no-op returning false
    // (jetty-client 12.1), so a cancel arriving after the last chunk changes nothing.
    override fun getBody(): Flux<DataBuffer> = super.getBody().doOnCancel { native.get()?.abort(CancellationException("response body cancelled")) }
}

/**
 * Apache HttpClient 5's async client: ONE client and connection manager for every backend of the engine, one
 * connector per backend carrying that backend's response timeout and the pending-acquire timeout as the
 * request's `RequestConfig`.
 *
 * Mapped: connect timeout and `pool.max-life-time` (connection config), `max-connections` (per route, i.e. per
 * remote address, shared by the backends behind it; no total cap across routes), and `pool.max-idle-time` swept every
 * `pool.evict-in-background` by an idle-connection evictor this transport owns. Retries, redirects and cookie
 * management are switched off, and the client speaks HTTP/1.1 only - each of these is on by default in Apache
 * and off in Reactor Netty, and an automatic retry would spend the mesh budget behind the TimeLimiter's back.
 *
 * Not applied: `pool.pending-acquire-max-count` - the Apache pool has no bound on its waiting requests - and,
 * with `pool.evict-in-background: 0`, `pool.max-idle-time` (no evictor runs, as with Reactor Netty's "off").
 * `pending-acquire-timeout-ms` is set as the connection-request timeout but does not hold on its own.
 * Assumption: Apache's async pool (`StrictConnPool`, httpcore5 5.4) checks a waiting lease's deadline only
 * while it processes a lease or a release, and no timer fails it in between - with nothing else happening on
 * the route, a call waiting for a connection waits until its TimeLimiter ends it.
 *
 * Cancellation needs this transport's help: Spring's `HttpComponentsClientHttpConnector` drops the `Future` of
 * the exchange it starts, so a cancelled subscription would leave the exchange running until the response or the
 * response timeout. [ApacheEndpointConnector] and [AbortableAsyncClient] keep that `Future` and cancel it.
 */
internal class ApacheTransport(
    properties: WebClientProperties,
    private val meshTransport: MeshProperties.Transport,
    private val logger: Logger,
) : EngineTransport {
    override val engine: WebClientEngine = WebClientEngine.APACHE

    private val connectionManager: PoolingAsyncClientConnectionManager =
        PoolingAsyncClientConnectionManagerBuilder
            .create()
            .setMaxConnPerRoute(meshTransport.maxConnections)
            .setMaxConnTotal(Int.MAX_VALUE)
            .setDefaultConnectionConfig(
                ConnectionConfig
                    .custom()
                    .setConnectTimeout(Timeout.ofMilliseconds(meshTransport.connectTimeoutMs))
                    .setTimeToLive(TimeValue.of(properties.pool.maxLifeTime))
                    .build(),
            ).setDefaultTlsConfig(TlsConfig.custom().setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_1).build())
            .build()

    /** Set when [release] starts - from then on a closed selector is the expected end of a worker, not a fault. */
    @Volatile
    private var releasing = false

    private val client: AbortableAsyncClient =
        AbortableAsyncClient(
            HttpAsyncClients
                .custom()
                .setConnectionManager(connectionManager)
                .setIoReactorExceptionCallback(::onReactorException)
                // Named after the module, like the other engines' threads, so a thread dump (and a test) tells
                // this engine's I/O threads from a host's own Apache client; daemons, as Apache's default ones.
                .setThreadFactory(DefaultThreadFactory(THREAD_NAME_PREFIX, true))
                .disableAutomaticRetries()
                .disableRedirectHandling()
                .disableCookieManagement()
                // Apache checks socket and connect timeouts when its selector wakes, every second by default -
                // late enough to let the response timeout fire AFTER the TimeLimiter rung above it, whose
                // headroom is only connect + pending-acquire + 100 ms. Half the fixed headroom keeps the expiry
                // within it.
                .setIOReactorConfig(
                    IOReactorConfig
                        .custom()
                        .setSelectInterval(TimeValue.ofMilliseconds(MeshProperties.Transport.TIME_LIMITER_HEADROOM_MS / 2))
                        .build(),
                ).build(),
        ).also { it.start() }

    private val evictor: IdleConnectionEvictor? =
        properties.pool.evictInBackground.takeUnless { it.isZero }?.let { interval ->
            IdleConnectionEvictor(connectionManager, TimeValue.of(interval), TimeValue.of(properties.pool.maxIdleTime)).also { it.start() }
        }

    override fun connectorFor(endpoint: ResolvedTimeouts): ClientHttpConnector =
        ApacheEndpointConnector(
            client,
            RequestConfig
                .custom()
                .setResponseTimeout(Timeout.of(endpoint.responseTimeout))
                .setConnectionRequestTimeout(Timeout.ofMilliseconds(meshTransport.pendingAcquireTimeoutMs))
                .build(),
        )

    /**
     * Waits until [deadlineNanos] for the exchanges in flight to finish, then shuts the I/O reactor down
     * gracefully, bounded by the same deadline; past it the client is closed immediately, with a WARN. Exchanges
     * still in flight then are cancelled - with a WARN naming how many - so their callers do not wait out their
     * budget for a connection that is gone. The evictor is stopped first, and waited for (briefly) so that
     * [isReleased] means its thread is gone.
     *
     * The wait comes BEFORE the reactor shutdown, as Jetty's does before its stop: a reactor in `SHUTTING_DOWN`
     * lets an exchange already on a connection finish, but opens no connection any more - a call still waiting
     * for one (max-connections reached, or a connect not yet made) would fail with an `IOReactorShutdownException`
     * although the deadline left it all the time it needed. The wait also ends when the calling thread is
     * INTERRUPTED; the flag is left for the caller.
     */
    override fun release(
        deadlineNanos: Long,
        nanoTime: () -> Long,
    ) {
        releasing = true
        while (client.inFlightCount() > 0 && nanoTime() - deadlineNanos < 0 && !Thread.currentThread().isInterrupted) {
            LockSupport.parkNanos(EngineTransport.DRAIN_POLL_NANOS)
        }
        evictor?.let { running ->
            running.shutdown()
            try {
                // Assumption: shutdown() only interrupts the evictor's thread, which leaves its sleep at once
                // (httpclient5 5.6); the join is bounded because a Timeout of 0 would mean "forever" to it.
                running.awaitTermination(EVICTOR_JOIN)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
        client.initiateShutdown()
        try {
            client.awaitShutdown(TimeValue.of(EngineTransport.remaining(deadlineNanos, nanoTime)))
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
        // Assumption: the multi-core reactor reports SHUTTING_DOWN until it is closed, and its awaitShutdown
        // returns either when every worker ended or at the deadline - so whether the deadline has passed, not the
        // status, says whether the reactor drained.
        val timedOut = nanoTime() - deadlineNanos >= 0
        // Assumption: an exchange the drain above left behind does not complete on its own once the reactor is
        // shutting down - one waiting for a connection gets none, and a hard close ends one on a connection
        // without completing its callback (httpclient5 5.6, httpcore5 5.4) - so the caller's Mono would wait for
        // its TimeLimiter. Cancelling them hands each caller its cancellation now.
        val aborted = client.abortInFlight()
        if (timedOut) {
            logger.warn("HTTP engine '{}' did not drain by the factory's shutdown deadline - closing it with exchanges still open", engine)
        } else if (aborted > 0) {
            logger.warn("HTTP engine '{}' shut down with {} exchange(s) still in flight - cancelling them", engine, aborted)
        }
        // Once nothing holds it, the reactor can end by itself; it is closed hard only if it still has not. A worker
        // still shutting down then finds its selector closed under it - `onReactorException` takes that.
        if (client.status != IOReactorStatus.SHUT_DOWN) {
            try {
                client.awaitShutdown(TimeValue.of(SHUTDOWN_AFTER_ABORT))
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
        client.close(if (client.status == IOReactorStatus.SHUT_DOWN) CloseMode.GRACEFUL else CloseMode.IMMEDIATE)
    }

    /**
     * The I/O reactor's exception callback, in place of Apache's default, which logs every exception at ERROR.
     *
     * Assumption: closing the multi-core reactor while one of its workers is still shutting down closes that
     * worker's selector under it, and the worker's termination (`closePendingChannels`) reports a
     * [ClosedSelectorException] to this callback (httpcore5 5.4). The multi-core reactor reports `SHUT_DOWN` only
     * once it is closed, so [release] cannot tell a worker that already ended from one that has not - on a slow
     * machine the hard close regularly meets one that has not. During [release] that exception is the expected end
     * of the worker, logged at DEBUG; any other exception, and one outside a release, stays an ERROR.
     */
    private fun onReactorException(ex: Exception) {
        if (releasing && ex is ClosedSelectorException) {
            logger.debug("HTTP engine '{}': an I/O worker ended on its closed selector during the release", engine)
        } else {
            logger.error("HTTP engine '{}': I/O reactor failure", engine, ex)
        }
    }

    override fun isReleased(): Boolean = client.status == IOReactorStatus.SHUT_DOWN && evictor?.isRunning != true

    companion object {
        /** The name prefix of this engine's I/O threads. */
        const val THREAD_NAME_PREFIX: String = "veredium-webclient-apache"

        /** How long [release] waits for the interrupted evictor thread to end. */
        private val EVICTOR_JOIN: Timeout = Timeout.ofSeconds(1)

        /** How long [release] lets the reactor finish by itself once the exchanges in flight were cancelled. */
        private val SHUTDOWN_AFTER_ABORT: Duration = Duration.ofSeconds(1)
    }
}

/**
 * One backend's Apache connector: a Spring [HttpComponentsClientHttpConnector] per EXCHANGE over the shared
 * client, so the exchange's own [AbortableExchange] can travel in its `HttpClientContext` to
 * [AbortableAsyncClient], which records the exchange's `Future` there.
 *
 * Cancelling the subscription - the TimeLimiter's abort, a caller's cancellation - cancels that `Future`, which
 * makes Apache abort the exchange and discard its connection. Both phases are covered: waiting for the response
 * (the cancellation reaches the connector's `Mono`) and reading the body (it reaches the body `Flux`).
 *
 * A connector per exchange costs one small object and an idempotent `start()` on the already running client;
 * it is what lets the context carry per-exchange state without relying on when Spring invokes its context
 * provider.
 */
internal class ApacheEndpointConnector(
    private val client: AbortableAsyncClient,
    // Not named `requestConfig`: inside the context's `apply` below that name resolves to the CONTEXT's own
    // (still null) property, and the endpoint's timeouts would silently never reach the exchange.
    private val endpointConfig: RequestConfig,
) : ClientHttpConnector {
    override fun connect(
        method: HttpMethod,
        uri: URI,
        requestCallback: Function<in ClientHttpRequest, Mono<Void>>,
    ): Mono<ClientHttpResponse> =
        Mono.defer {
            val exchange = AbortableExchange()
            HttpComponentsClientHttpConnector(client) { _, _ ->
                HttpClientContext.create().apply {
                    setRequestConfig(endpointConfig)
                    setAttribute(AbortableExchange.CONTEXT_ATTRIBUTE, exchange)
                }
            }.connect(method, uri, requestCallback)
                .map<ClientHttpResponse> { response -> AbortOnBodyCancel(response, exchange) }
                .doOnCancel(exchange::abort)
        }
}

/** A response whose body, when cancelled before it completed, aborts the exchange it belongs to. */
private class AbortOnBodyCancel(
    response: ClientHttpResponse,
    private val exchange: AbortableExchange,
) : ClientHttpResponseDecorator(response) {
    override fun getBody(): Flux<DataBuffer> = super.getBody().doOnCancel(exchange::abort)
}

/**
 * The link between one exchange's subscription and the `Future` Apache returns for it. Either side may come
 * first: an abort before the `Future` is attached cancels it on attachment.
 */
internal class AbortableExchange {
    private val future = AtomicReference<Future<*>?>()

    @Volatile
    private var aborted = false

    fun attach(started: Future<*>) {
        future.set(started)
        if (aborted) started.cancel(true)
    }

    fun abort() {
        aborted = true
        future.get()?.cancel(true)
    }

    companion object {
        /** The `HttpClientContext` attribute [ApacheEndpointConnector] puts the exchange under. */
        const val CONTEXT_ATTRIBUTE: String = "eu.inqudium.veredium.webclient.AbortableExchange"
    }
}

/**
 * The shared Apache client, delegating everything and recording each exchange's `Future` in the
 * [AbortableExchange] of its context - the `Future` Spring's connector does not keep - and in the set of
 * exchanges in flight, which [abortInFlight] cancels.
 */
internal class AbortableAsyncClient(
    private val delegate: CloseableHttpAsyncClient,
) : CloseableHttpAsyncClient() {
    /** The exchanges started and not yet completed; each removes itself when its callback fires. */
    private val inFlight: MutableSet<Future<*>> = ConcurrentHashMap.newKeySet()

    /**
     * Cancels every exchange still in flight, so its caller receives the cancellation now, and returns how many
     * it cancelled. A cancelled future completes the callback of Spring's connector with a
     * `CancellationException`, which ends the call at once (see [ApacheTransport.release] for why that is needed).
     */
    fun abortInFlight(): Int = inFlight.toList().count { it.cancel(true) }

    /** The exchanges started and not yet completed - waiting for a connection included; what a draining release waits on. */
    fun inFlightCount(): Int = inFlight.size

    override fun start() = delegate.start()

    override fun getStatus(): IOReactorStatus = delegate.status

    override fun awaitShutdown(waitTime: TimeValue) = delegate.awaitShutdown(waitTime)

    override fun initiateShutdown() = delegate.initiateShutdown()

    override fun register(
        hostname: String?,
        uriPattern: String,
        supplier: Supplier<AsyncPushConsumer>,
    ) = delegate.register(hostname, uriPattern, supplier)

    override fun close(closeMode: CloseMode) = delegate.close(closeMode)

    override fun close() = delegate.close()

    override fun <T> doExecute(
        target: HttpHost?,
        requestProducer: AsyncRequestProducer,
        responseConsumer: AsyncResponseConsumer<T>,
        pushHandlerFactory: HandlerFactory<AsyncPushConsumer>?,
        context: HttpContext?,
        callback: FutureCallback<T>?,
    ): Future<T> {
        val exchange = context?.getAttribute(AbortableExchange.CONTEXT_ATTRIBUTE) as? AbortableExchange
        val started = AtomicReference<Future<*>?>()
        val untracking =
            object : FutureCallback<T> {
                override fun completed(result: T) {
                    started.get()?.let(inFlight::remove)
                    callback?.completed(result)
                }

                override fun failed(ex: Exception) {
                    started.get()?.let(inFlight::remove)
                    callback?.failed(ex)
                }

                override fun cancelled() {
                    started.get()?.let(inFlight::remove)
                    callback?.cancelled()
                }
            }
        val future = delegate.execute(target, requestProducer, responseConsumer, pushHandlerFactory, context, untracking)
        started.set(future)
        inFlight += future
        // An exchange that completed before it was registered would otherwise stay in the set for good.
        if (future.isDone) inFlight -= future
        exchange?.attach(future)
        return future
    }
}
