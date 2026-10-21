package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.netty.channel.ChannelOption
import io.netty.resolver.DefaultAddressResolverGroup
import org.slf4j.Logger
import org.springframework.http.HttpStatusCode
import org.springframework.http.client.ClientHttpRequestFactory
import org.springframework.http.client.ClientHttpResponse
import org.springframework.http.client.ReactorClientHttpRequestFactory
import reactor.netty.Metrics
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionPoolMetrics
import reactor.netty.resources.ConnectionProvider
import reactor.netty.resources.LoopResources
import java.io.IOException
import java.io.InputStream
import java.io.InterruptedIOException
import java.net.SocketAddress
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * The Reactor Netty engine ([RestClientEngine.REACTOR_NETTY]): one Reactor Netty `HttpClient` for every backend
 * on it, over a `ConnectionProvider` and `LoopResources` this class creates and disposes - never Reactor Netty's
 * global `HttpResources`, which every other Reactor Netty client of the JVM shares - and one Spring
 * [ReactorClientHttpRequestFactory] per backend carrying the backend's response timeout.
 *
 * ## What maps
 *
 * | Setting                          | Here                                                                         |
 * |----------------------------------|------------------------------------------------------------------------------|
 * | `connect-timeout-ms`             | `ChannelOption.CONNECT_TIMEOUT_MILLIS`                                       |
 * | per-endpoint response timeout    | `HttpClient.responseTimeout` - the longest silence between reads from sending the request until the response is complete |
 * | `max-connections`                | `ConnectionProvider.maxConnections` (per remote address; every endpoint behind the sidecar is one) |
 * | `pending-acquire-timeout-ms`     | `ConnectionProvider.pendingAcquireTimeout`                                   |
 * | `pool.max-idle-time` / `max-life-time` / `evict-in-background` | `maxIdleTime` / `maxLifeTime` (`0` = unbounded) / `evictInBackground` (`0` = off) |
 * | pool meters                      | Reactor Netty's own gauge names (`reactor.netty.connection.provider.*`), registered in the HOST's registry by [PoolMeters] instead of Micrometer's global one |
 *
 * The pending-acquire queue is bounded in count at Reactor Netty's default of twice `max-connections`;
 * `app.restclient` has no key for it (the reactive sibling's `pending-acquire-max-count` was dropped here because
 * Apache has no counterpart). Reactor Netty's defaults that the Apache engine does not share are switched off: the
 * one automatic retry of a request whose pooled connection was closed while sending (`disableRetry`). Redirects
 * are off by default; there is no cookie store. Compression is on, as on Apache. Name resolution uses the JDK's
 * resolver like the other three engines - Reactor Netty's default resolver would come from the global
 * `HttpResources`. The pool's timers run on Reactor's shared `parallel` scheduler, as for every Reactor Netty
 * pool; that scheduler holds no connection or state of this engine.
 *
 * ## Cancellation, and the one place Spring's request does not honour an interrupt
 *
 * Waiting for the response head honours an interrupt: Reactor's `block()` cancels the subscription, which
 * closes the connection. Reading the BODY does not: Spring's body stream (`SubscriberInputStream`, spring-web 7.0)
 * waits with `LockSupport.park` in a loop that ignores the interrupt, so an interrupted worker would spin until
 * the next chunk, the end of the body or the response timeout. And closing the response DRAINS the rest of the
 * body. [requestFactory] therefore hands out a decorator whose body reads each run on a short-lived virtual
 * thread while the worker waits for them interruptibly ([HandOffInputStream]): on an interrupt the worker closes
 * Spring's stream, which wakes the reader and cancels the inbound, and fails the read with an
 * `InterruptedIOException`. Closing a response whose body was not read to its end closes the stream first, so
 * the connection is released instead of drained - the moment Apache would discard its connection.
 */
internal class ReactorNettyEngineTransport(
    settings: Settings,
) : EngineTransport(RestClientEngine.REACTOR_NETTY) {
    private val meshTransport = settings.transport
    private val pool = settings.pool

    /** The pools' statistics for the drain, and their gauges when the host has a registry. */
    private val pools = PoolMeters(settings.meterRegistry)

    /** This engine's event loops, daemon threads named after the pool; disposed in [shutdown]. */
    internal val loops: LoopResources = LoopResources.create(POOL_NAME, LoopResources.DEFAULT_IO_WORKER_COUNT, true)

    /** This engine's connection pool; disposed in [shutdown]. */
    internal val provider: ConnectionProvider =
        ConnectionProvider
            .builder(POOL_NAME)
            .maxConnections(meshTransport.maxConnections)
            .pendingAcquireTimeout(Duration.ofMillis(meshTransport.pendingAcquireTimeoutMs))
            .maxIdleTime(pool.maxIdleTime)
            .apply { pool.maxLifeTime.takeUnless { it.isZero }?.let { maxLifeTime(it) } }
            .apply { pool.evictInBackground.takeUnless { it.isZero }?.let { evictInBackground(it) } }
            // Always on, with this class's registrar: it is how the drain sees the leased connections, and the
            // custom registrar keeps Reactor Netty from registering anything in Micrometer's global registry.
            .metrics(true) { pools }
            .build()

    /** Spring's request-body writer and the body readers of [HandOffInputStream]; virtual, owned here. */
    private val offload: ExecutorService = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("$POOL_NAME-io-", 0).factory())

    private val httpClient: HttpClient =
        HttpClient
            .create(provider)
            .runOn(loops)
            .resolver(DefaultAddressResolverGroup.INSTANCE)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, meshTransport.connectTimeoutMs.toInt())
            .compress(true)
            .disableRetry(true)

    override fun requestFactory(endpoint: ResolvedTimeouts): ClientHttpRequestFactory {
        val requests =
            ReactorClientHttpRequestFactory(httpClient).apply {
                setReadTimeout(endpoint.responseTimeout)
                setExecutor(offload)
            }
        return ClientHttpRequestFactory { uri, method ->
            object : ForwardingClientHttpRequest(requests.createRequest(uri, method)) {
                override fun execute(): ClientHttpResponse = HandOffResponse(delegate.execute(), offload)
            }
        }
    }

    override val applied: List<String> =
        listOf(
            "connect-timeout-ms=${meshTransport.connectTimeoutMs} (CONNECT_TIMEOUT_MILLIS)",
            "the endpoint's response timeout (HttpClient.responseTimeout, the longest silence between reads)",
            "max-connections=${meshTransport.maxConnections} (maxConnections per remote address)",
            "pending-acquire-timeout-ms=${meshTransport.pendingAcquireTimeoutMs} (pendingAcquireTimeout)",
            "pool.max-idle-time=${pool.maxIdleTime} (maxIdleTime)",
            "pool.max-life-time=${pool.maxLifeTime} (maxLifeTime, 0 = unbounded)",
            "pool.evict-in-background=${pool.evictInBackground} (evictInBackground, 0 = off)",
        )

    override val notApplied: List<String> =
        listOf("a pending-acquire count limit (app.restclient has no key for it; Reactor Netty's default of 2 x max-connections applies)")

    override val poolMeters: String =
        if (settings.meterRegistry != null) {
            "${Metrics.CONNECTION_PROVIDER_PREFIX}.*{${Metrics.NAME}=$POOL_NAME}"
        } else {
            "none (no MeterRegistry)"
        }

    override val cancellation: String =
        "an interrupted worker cancels the exchange; body reads are handed off so the interrupt also ends a read"

    override val isClosed: Boolean get() = provider.isDisposed && loops.isDisposed

    override fun connectionsInUse(): Int = pools.acquired()

    /**
     * Waits until the deadline for the leased connections to come back (the same drain and line as the Apache
     * pool's), then disposes the pool and the event loops. A shutdown thread that was interrupted does not block
     * on the disposal: it is triggered and left to complete.
     */
    override fun shutdown(
        deadlineNanos: Long,
        nanoTime: () -> Long,
        window: Duration,
        logger: Logger,
    ) {
        if (isClosed) return
        drainOrReport(POOL_NAME, ::connectionsInUse, deadlineNanos, nanoTime, window, logger)
        if (Thread.currentThread().isInterrupted) {
            provider.dispose()
            loops.dispose()
        } else {
            provider.disposeLater().block(DISPOSE_WAIT)
            loops.disposeLater(Duration.ZERO, DISPOSE_WAIT).block(DISPOSE_WAIT)
        }
        offload.shutdownNow()
    }

    /**
     * Reactor Netty's pool registrar: keeps each pool's statistics for the drain and, when the host has a
     * registry, registers the six gauges Reactor Netty's own Micrometer registrar would - same names, same tags
     * (`name`, `id`, `remote.address`) - in THAT registry and not in Micrometer's global one. Reactor Netty calls
     * it once per pool (one per remote address) and deregisters on disposal.
     */
    private class PoolMeters(
        private val registry: MeterRegistry?,
    ) : ConnectionProvider.MeterRegistrar {
        private val pools = ConcurrentHashMap<String, ConnectionPoolMetrics>()
        private val meters = ConcurrentHashMap<String, List<Meter>>()

        fun acquired(): Int = pools.values.sumOf { it.acquiredSize() }

        override fun registerMetrics(
            poolName: String,
            id: String,
            remoteAddress: SocketAddress,
            metrics: ConnectionPoolMetrics,
        ) {
            val key = "$id@$remoteAddress"
            pools[key] = metrics
            val registry = registry ?: return
            val address = Metrics.formatSocketAddress(remoteAddress)

            fun gauge(
                suffix: String,
                description: String,
                value: (ConnectionPoolMetrics) -> Int,
            ): Meter =
                Gauge
                    .builder(Metrics.CONNECTION_PROVIDER_PREFIX + suffix, metrics) { value(it).toDouble() }
                    .description(description)
                    .tags(Metrics.NAME, poolName, Metrics.ID, id, Metrics.REMOTE_ADDRESS, address)
                    // Strong: the gauges must not lose the pool to the garbage collector before it is deregistered.
                    .strongReference(true)
                    .register(registry)
            meters[key] =
                listOf(
                    gauge(Metrics.TOTAL_CONNECTIONS, "The number of all connections, active or idle") { it.allocatedSize() },
                    gauge(Metrics.ACTIVE_CONNECTIONS, "The number of connections in use") { it.acquiredSize() },
                    gauge(Metrics.IDLE_CONNECTIONS, "The number of idle connections") { it.idleSize() },
                    gauge(Metrics.PENDING_CONNECTIONS, "The number of requests waiting for a connection") { it.pendingAcquireSize() },
                    gauge(Metrics.MAX_CONNECTIONS, "The maximum number of connections") { it.maxAllocatedSize() },
                    gauge(Metrics.MAX_PENDING_CONNECTIONS, "The maximum number of requests that may wait") { it.maxPendingAcquireSize() },
                )
        }

        override fun deRegisterMetrics(
            poolName: String,
            id: String,
            remoteAddress: SocketAddress,
        ) {
            val key = "$id@$remoteAddress"
            pools.remove(key)
            meters.remove(key)?.forEach { registry?.remove(it) }
        }
    }

    /** Spring's response, with an interrupt-aware body ([HandOffInputStream]) and a close that does not drain. */
    private class HandOffResponse(
        private val delegate: ClientHttpResponse,
        private val offload: ExecutorService,
    ) : ClientHttpResponse {
        @Volatile
        private var body: HandOffInputStream? = null

        override fun getBody(): InputStream = body ?: HandOffInputStream(delegate.body, offload).also { body = it }

        override fun getHeaders() = delegate.headers

        override fun getStatusCode(): HttpStatusCode = delegate.statusCode

        override fun getStatusText(): String = delegate.statusText

        override fun close() {
            // Spring's close drains what is left of the body; a body not read to its end is closed first, which
            // cancels the inbound - the connection is released rather than read to the end of a body nobody wants.
            body?.closeUnlessAtEnd()
            delegate.close()
        }
    }

    /**
     * A body stream whose reads run on a short-lived virtual thread of [offload] while the calling worker waits
     * on them INTERRUPTIBLY - the worker is the thread the blocking decorator interrupts, and Spring's stream
     * ignores an interrupt (class KDoc). On an interrupt the worker closes [source] (which wakes the parked reader
     * and cancels the inbound), keeps its interrupt flag, and fails the read with an [InterruptedIOException].
     *
     * Reads are sequential - the worker waits for each before issuing the next - which is all Spring's stream
     * admits ("one thread can await"); a fresh thread per read satisfies it.
     */
    private class HandOffInputStream(
        private val source: InputStream,
        private val offload: ExecutorService,
    ) : InputStream() {
        @Volatile
        private var atEnd = false

        override fun read(): Int {
            val one = ByteArray(1)
            val read = read(one, 0, 1)
            return if (read < 0) read else one[0].toInt() and 0xFF
        }

        override fun read(
            b: ByteArray,
            off: Int,
            len: Int,
        ): Int {
            if (len == 0) return 0
            val pending = offload.submit(Callable { source.read(b, off, len) })
            return try {
                pending.get().also { if (it < 0) atEnd = true }
            } catch (e: InterruptedException) {
                // Closing wakes the reader through Spring's own resume, never through an interrupt: the reader
                // parks in the same interrupt-blind loop, which an interrupt would turn into a spin.
                source.close()
                Thread.currentThread().interrupt()
                throw InterruptedIOException("Body read interrupted").apply { initCause(e) }
            } catch (e: ExecutionException) {
                when (val cause = e.cause) {
                    is IOException -> throw cause
                    is RuntimeException -> throw cause
                    is Error -> throw cause
                    else -> throw IOException(cause)
                }
            }
        }

        fun closeUnlessAtEnd() {
            if (!atEnd) source.close()
        }

        override fun close() = source.close()
    }

    companion object {
        /** The pool's name: its `name` tag, the event-loop and offload threads' prefix, the shutdown line's pool. */
        const val POOL_NAME: String = "restclient-reactor-netty"

        /** How long [shutdown] blocks on the disposal after the drain, which has already waited for the exchanges. */
        val DISPOSE_WAIT: Duration = Duration.ofSeconds(5)
    }
}
