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
import org.eclipse.jetty.client.ConnectionPool
import org.eclipse.jetty.client.DuplexConnectionPool
import org.eclipse.jetty.client.Request
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
import java.time.Duration
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Function
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

    /** Releases the shared resources; bounded, never throws for an outcome the transport can report itself. */
    fun release()

    /** Whether [release] has completed - for the tests that pin the lifecycle. */
    fun isReleased(): Boolean
}

/**
 * The engines in use by one factory, each created on first use and released together.
 *
 * Only the engines named in [enginesInUse] can ever be created; asking for another one is a programming
 * error, because the factory resolved every backend's engine at construction.
 */
internal class EngineTransports(
    enginesInUse: Set<WebClientEngine>,
    private val logger: Logger,
    create: (WebClientEngine) -> EngineTransport,
) {
    private val transports: Map<WebClientEngine, Lazy<EngineTransport>> = enginesInUse.associateWith { engine -> lazy { create(engine) } }

    fun connectorFor(
        engine: WebClientEngine,
        endpoint: ResolvedTimeouts,
    ): ClientHttpConnector {
        val transport = transports[engine] ?: error("Engine '$engine' was not resolved for any backend of this factory")
        return transport.value.connectorFor(endpoint)
    }

    /** The transports created so far - an engine that is in use but has not built a client yet is absent. */
    fun started(): Map<WebClientEngine, EngineTransport> = transports.filterValues { it.isInitialized() }.mapValues { it.value.value }

    /**
     * Releases every transport created so far, each on its own: a failing release is logged and does not keep
     * the others from releasing theirs.
     */
    fun release() {
        started().values.forEach { transport ->
            try {
                transport.release()
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
 * Reactor Netty keeps a separate sub-pool per remote address inside the provider, so distinct sidecar ports
 * stay isolated; `maxConnections` and the pending-acquire limits therefore apply per remote address.
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
    private val dispose: (Mono<Void>) -> Unit,
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

    override fun release() {
        dispose(connectionProvider.disposeLater())
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
    private val releaseTimeout: Duration,
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
     * bounded by the factory's drain timeout - past it the client is shut down hard, with a WARN.
     */
    override fun release() {
        client.shutdown()
        val terminated =
            try {
                client.awaitTermination(releaseTimeout)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                false
            }
        if (!terminated) {
            logger.warn("HTTP engine '{}' did not drain within {} - closing it with exchanges still open", engine, releaseTimeout)
            client.shutdownNow()
        }
    }

    override fun isReleased(): Boolean = client.isTerminated
}

/**
 * Jetty's `HttpClient`: ONE client for every backend of the engine, started here and stopped in [release]; one
 * connector per backend that puts that backend's response timeout on each request as Jetty's per-request idle
 * timeout.
 *
 * Mapped: connect timeout, `max-connections` (per destination, i.e. per remote address like Reactor Netty's
 * sub-pools), `pool.pending-acquire-max-count` (as the per-destination request queue), `pool.max-idle-time`
 * (the connection idle timeout) and `pool.max-life-time` (the connection pool's maximum connection duration).
 * Redirects are not followed and no cookie is stored, as with Reactor Netty.
 *
 * Not applied: `pending-acquire-timeout-ms` - Jetty has no timeout for a request waiting in its queue - and
 * `pool.evict-in-background`, because Jetty closes an idle connection on that connection's own idle timer
 * rather than in a background sweep.
 */
internal class JettyTransport(
    properties: WebClientProperties,
    meshTransport: MeshProperties.Transport,
) : EngineTransport {
    override val engine: WebClientEngine = WebClientEngine.JETTY

    private val client: JettyHttpClient =
        JettyHttpClient().apply {
            name = "veredium-webclient-jetty"
            connectTimeout = meshTransport.connectTimeoutMs
            idleTimeout = properties.pool.maxIdleTime.toMillis()
            maxConnectionsPerDestination = meshTransport.maxConnections
            maxRequestsQueuedPerDestination = jettyQueueCapacity(properties.pool.pendingAcquireMaxCount)
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

    /** Stops the client: its exchanges are aborted, its connections closed and its thread pool stopped. */
    override fun release() = client.stop()

    override fun isReleased(): Boolean = client.isStopped

    companion object {
        /**
         * The Jetty queue capacity for `pool.pending-acquire-max-count`: `-1` (unbounded) becomes the largest
         * capacity Jetty accepts. `0` is rejected before this is ever reached (see the factory's engine checks),
         * because Jetty queues EVERY request before it gets a connection, so a capacity of 0 would reject all.
         */
        fun jettyQueueCapacity(pendingAcquireMaxCount: Int): Int = if (pendingAcquireMaxCount < 0) Int.MAX_VALUE else pendingAcquireMaxCount
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
        delegate.connect(method, uri) { request ->
            request.getNativeRequest<Request>().idleTimeout(responseTimeout.toMillis(), TimeUnit.MILLISECONDS)
            requestCallback.apply(request)
        }
}

/**
 * Apache HttpClient 5's async client: ONE client and connection manager for every backend of the engine, one
 * connector per backend carrying that backend's response timeout and the pending-acquire timeout as the
 * request's `RequestConfig`.
 *
 * Mapped: connect timeout and `pool.max-life-time` (connection config), `max-connections` (per route, i.e. per
 * remote address like Reactor Netty's sub-pools; no total cap across routes, again like Reactor Netty),
 * `pending-acquire-timeout-ms` (the connection-request timeout), and `pool.max-idle-time` swept every
 * `pool.evict-in-background` by an idle-connection evictor this transport owns. Retries, redirects and cookie
 * management are switched off, and the client speaks HTTP/1.1 only - each of these is on by default in Apache
 * and off in Reactor Netty, and an automatic retry would spend the mesh budget behind the TimeLimiter's back.
 *
 * Not applied: `pool.pending-acquire-max-count` - the Apache pool has no bound on its waiting requests - and,
 * with `pool.evict-in-background: 0`, `pool.max-idle-time` (no evictor runs, as with Reactor Netty's "off").
 *
 * Cancellation needs this transport's help: Spring's `HttpComponentsClientHttpConnector` drops the `Future` of
 * the exchange it starts, so a cancelled subscription would leave the exchange running until the response or the
 * response timeout. [ApacheEndpointConnector] and [AbortableAsyncClient] keep that `Future` and cancel it.
 */
internal class ApacheTransport(
    properties: WebClientProperties,
    private val meshTransport: MeshProperties.Transport,
    private val releaseTimeout: Duration,
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

    private val client: AbortableAsyncClient =
        AbortableAsyncClient(
            HttpAsyncClients
                .custom()
                .setConnectionManager(connectionManager)
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
     * A graceful shutdown of the I/O reactor, bounded by the factory's drain timeout; past it the client is closed
     * immediately, with a WARN. The evictor is stopped first.
     */
    override fun release() {
        evictor?.shutdown()
        val started = System.nanoTime()
        client.initiateShutdown()
        try {
            client.awaitShutdown(TimeValue.of(releaseTimeout))
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
        // Assumption: the multi-core reactor reports SHUTTING_DOWN until it is closed, and its awaitShutdown
        // returns either when every worker ended or at the deadline - so the elapsed time, not the status, says
        // whether the reactor drained.
        if (Duration.ofNanos(System.nanoTime() - started) >= releaseTimeout) {
            logger.warn("HTTP engine '{}' did not drain within {} - closing it with exchanges still open", engine, releaseTimeout)
        }
        client.close(CloseMode.IMMEDIATE)
    }

    override fun isReleased(): Boolean = client.status == IOReactorStatus.SHUT_DOWN && evictor?.isRunning != true
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
 * [AbortableExchange] of its context - the `Future` Spring's connector does not keep.
 */
internal class AbortableAsyncClient(
    private val delegate: CloseableHttpAsyncClient,
) : CloseableHttpAsyncClient() {
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
        return if (exchange == null) {
            delegate.execute(target, requestProducer, responseConsumer, pushHandlerFactory, context, callback)
        } else {
            delegate
                .execute(target, requestProducer, responseConsumer, pushHandlerFactory, context, callback)
                .also(exchange::attach)
        }
    }
}
