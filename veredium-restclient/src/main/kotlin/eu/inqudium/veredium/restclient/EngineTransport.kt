package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatusCode
import org.springframework.http.StreamingHttpOutputMessage
import org.springframework.http.client.ClientHttpRequest
import org.springframework.http.client.ClientHttpRequestFactory
import org.springframework.http.client.ClientHttpResponse
import java.io.FilterInputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.URI
import java.nio.channels.AsynchronousCloseException
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

/**
 * One HTTP engine as [ResilientRestClientFactory] owns it: the engine's shared resources (its pool, its client,
 * its threads), the request factory it hands each backend, the startup line that says which mesh and pool
 * settings it applies, and its shutdown.
 *
 * Created by the factory only for an engine some backend selects, at most once per factory, and shared by every
 * backend on that engine - one pool per engine, as there is one pool today. Nothing of it is a Spring bean, so
 * nothing of it is visible to, or replaceable by, the host's own HTTP client wiring.
 *
 * Every subclass defines [cancellation] honestly: what an interrupted worker (a TimeLimiter expiry, a caller
 * interrupt, a cancelled asynchronous future) does to the exchange on this engine. That is the property the
 * blocking decorator's contract rests on (the worker must end promptly and release its bulkhead permit), and
 * the one property the four engines reach by four different routes.
 */
internal abstract class EngineTransport(
    val engine: RestClientEngine,
) {
    /**
     * The request factory of one backend: the engine's shared client with [endpoint]'s response timeout. Called
     * once per backend, when the factory builds its client.
     */
    abstract fun requestFactory(endpoint: ResolvedTimeouts): ClientHttpRequestFactory

    /** The mesh and pool settings this engine applies, each with the value or the engine knob it lands on. */
    abstract val applied: List<String>

    /** The settings this engine has no knob for, each with what happens instead. Never silently empty. */
    abstract val notApplied: List<String>

    /** Which pool meters the engine registers in the host's registry, or why there are none. */
    abstract val poolMeters: String

    /** What an interrupted worker does to the exchange on this engine (class KDoc). */
    abstract val cancellation: String

    /** Whether the engine's resources are released - read off the engine itself, not a flag of this class. */
    abstract val isClosed: Boolean

    /**
     * The connections the engine has handed to exchanges right now (queued requests included where the engine
     * counts them), or `null` when the engine does not expose the number - the JDK client. What a draining
     * shutdown waits on, and what a test reads to see an abandoned exchange give its connection back.
     */
    abstract fun connectionsInUse(): Int?

    /**
     * Releases the engine's resources, waiting for in-flight exchanges to finish until [deadlineNanos] (on
     * [nanoTime]'s clock) at the latest. An expiry is an outcome reported on [logger], never an exception: this
     * runs from a destroy callback. [window] is the wait the deadline was derived from, for that line.
     */
    abstract fun shutdown(
        deadlineNanos: Long,
        nanoTime: () -> Long,
        window: Duration,
        logger: Logger,
    )

    /**
     * The startup line's text for this engine: settings applied, settings not applied, meters, cancellation.
     * [backends] are the aliases on this engine, sorted.
     */
    fun settingsLine(backends: List<String>): String =
        "HTTP engine '${engine.key}' for ${backends.joinToString(", ")}: applies ${applied.joinToString(", ")}; " +
            "does not apply ${notApplied.ifEmpty { listOf("-") }.joinToString(", ")}; pool meters: $poolMeters; " +
            "cancellation: $cancellation"

    /**
     * What every engine is built from: the mesh transport caps shared by all endpoints, the pool lifecycle of
     * `app.restclient.pool`, and the host's registry for pool meters (`null` = no meters).
     */
    class Settings(
        val transport: MeshProperties.Transport,
        val pool: RestClientProperties.Pool,
        val meterRegistry: MeterRegistry?,
    )

    companion object {
        /** How often a draining shutdown re-checks the pool. */
        val DRAIN_POLL_NANOS: Long = TimeUnit.MILLISECONDS.toNanos(50)

        /**
         * Waits until [inFlight] reports zero, [deadlineNanos] passes or the calling thread is interrupted, then
         * reports a pool that did not drain - the shared shape of the pooled engines' shutdown, written for the
         * Apache pool first.
         *
         * The wait also ends when the calling thread is INTERRUPTED - a container tearing the context down under
         * a deadline of its own, a test executor's `shutdownNow`. `parkNanos` returns at once on a thread whose
         * interrupt flag is set and leaves the flag set, so without this exit the loop would spin hot until the
         * deadline instead of waiting. The flag is left for the caller, as an interrupt always is.
         */
        fun drainOrReport(
            poolName: String,
            inFlight: () -> Int,
            deadlineNanos: Long,
            nanoTime: () -> Long,
            window: Duration,
            logger: Logger,
        ) {
            while (inFlight() > 0 && nanoTime() < deadlineNanos && !Thread.currentThread().isInterrupted) {
                LockSupport.parkNanos(DRAIN_POLL_NANOS)
            }
            val leased = inFlight()
            if (leased > 0) {
                logger.warn(
                    "Connection pool '{}' did not drain {} - dropping it with {} connection(s) still leased",
                    poolName,
                    if (Thread.currentThread().isInterrupted) "because the shutdown thread was interrupted" else "within $window",
                    leased,
                )
            }
        }

        /** The time left until [deadlineNanos], never negative. */
        fun remaining(
            deadlineNanos: Long,
            nanoTime: () -> Long,
        ): Duration = Duration.ofNanos(maxOf(0L, deadlineNanos - nanoTime()))

        /**
         * Shuts down every one of [transports] against the one deadline, each on its own: a shutdown that throws is
         * reported on [logger] and does not keep the engines after it open. This runs from a destroy callback,
         * where an exception would only end the shutdown early and leave a stack trace in place of the line.
         */
        fun shutdownAll(
            transports: Collection<EngineTransport>,
            deadlineNanos: Long,
            nanoTime: () -> Long,
            window: Duration,
            logger: Logger,
        ) {
            transports.forEach { transport ->
                try {
                    transport.shutdown(deadlineNanos, nanoTime, window, logger)
                } catch (e: Exception) {
                    logger.warn("HTTP engine '{}' failed to release its resources", transport.engine, e)
                }
            }
        }

        /**
         * One transport per engine in [engines], created in order by [create]. When a creation fails, the transports
         * created before it are shut down at once - nothing can be in flight on them yet - and the failure is
         * rethrown: it fails the factory's construction, so no destroy callback would ever release them, and a
         * started Jetty client's non-daemon threads would keep the JVM of a failed start alive.
         */
        fun createAll(
            engines: Collection<RestClientEngine>,
            logger: Logger,
            create: (RestClientEngine) -> EngineTransport,
        ): Map<RestClientEngine, EngineTransport> {
            val created = LinkedHashMap<RestClientEngine, EngineTransport>()
            try {
                engines.forEach { engine -> created[engine] = create(engine) }
            } catch (e: Throwable) {
                val now = System.nanoTime()
                shutdownAll(created.values, now, { now }, Duration.ZERO, logger)
                throw e
            }
            return created
        }
    }
}

/**
 * A [ClientHttpRequest] that forwards everything to [delegate] - the base of the engines' request decorators,
 * which change only what [execute] returns or what its failure does.
 *
 * It is a [StreamingHttpOutputMessage] like every request Spring's request factories create, so a body handed
 * over by `RestClient` (or by an interceptor's intercepting request) is streamed to the delegate exactly as
 * it would be without the decorator, never buffered a second time.
 */
internal open class ForwardingClientHttpRequest(
    protected val delegate: ClientHttpRequest,
) : ClientHttpRequest,
    StreamingHttpOutputMessage {
    override fun getMethod(): HttpMethod = delegate.method

    override fun getURI(): URI = delegate.uri

    override fun getHeaders(): HttpHeaders = delegate.headers

    override fun getAttributes(): MutableMap<String, Any> = delegate.attributes

    override fun getBody(): OutputStream = delegate.body

    override fun setBody(body: StreamingHttpOutputMessage.Body) {
        val streaming = delegate as? StreamingHttpOutputMessage
        if (streaming != null) streaming.setBody(body) else body.writeTo(delegate.body)
    }

    override fun execute(): ClientHttpResponse = delegate.execute()
}

/**
 * A request whose exchange [abort]s when it fails, and whose response aborts it when a body read fails and when it
 * is closed before its body was read to the end ([AbortingResponse]) - the decorator of the engines whose own close
 * would otherwise DRAIN the rest of an unwanted body (Apache, the JDK client: Spring's responses of both read it to
 * the end in `close`) or leave the exchange in flight (Jetty).
 *
 * [abort] receives the response once there is one (`null` while the request failed before it), and must be a no-op
 * on an exchange that already ended; the engines' aborts are.
 */
internal class AbortingRequest(
    delegate: ClientHttpRequest,
    private val abort: (response: ClientHttpResponse?, cause: Throwable) -> Unit,
) : ForwardingClientHttpRequest(delegate) {
    override fun execute(): ClientHttpResponse {
        val response =
            try {
                delegate.execute()
            } catch (e: IOException) {
                // An interrupted wait for the head, a timeout, a transport failure: Spring stopped waiting, the
                // engine's exchange may still be in flight.
                abort(null, e)
                throw e
            }
        return AbortingResponse(response) { cause -> abort(response, cause) }
    }
}

/**
 * The response of an [AbortingRequest]. "Read to the end" means a read returned `-1`; a body never opened counts as
 * not read, because the engine would otherwise drain it on close just the same.
 *
 * The abort runs BEFORE the delegate's close: Spring's Apache and JDK responses read the rest of the body in their
 * close, so aborting afterwards would come too late.
 */
internal class AbortingResponse(
    private val delegate: ClientHttpResponse,
    private val abort: (Throwable) -> Unit,
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
                    abort(e)
                    throw e
                }
        }
    }

    override fun getBody(): InputStream = guardedBody

    override fun getHeaders(): HttpHeaders = delegate.headers

    override fun getStatusCode(): HttpStatusCode = delegate.statusCode

    override fun getStatusText(): String = delegate.statusText

    override fun close() {
        if (!endOfBody) abort(AsynchronousCloseException())
        delegate.close()
    }
}
