package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.engine.EngineResources
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

/**
 * One HTTP engine as [ResilientRestClientFactory] owns it: the engine's shared resources (its pool, its client,
 * its threads), the request factory it hands each backend, the startup line that says which mesh and pool
 * settings it applies, and its shutdown.
 *
 * Created by the factory only for an engine some backend selects, at most once per factory, and shared by every
 * backend on that engine - one pool per engine. Nothing of it is a Spring bean, so
 * nothing of it is visible to, or replaceable by, the host's own HTTP client wiring.
 *
 * Every subclass defines [cancellation] honestly: what an interrupted worker (a TimeLimiter expiry, a caller
 * interrupt, a cancelled asynchronous future) does to the exchange on this engine. That is the property the
 * blocking decorator's contract rests on (the worker must end promptly and release its bulkhead permit), and
 * the one property the four engines reach by four different routes.
 */
internal abstract class EngineTransport(
    val engine: RestClientEngine,
) : EngineResources {
    override val key: String get() = engine.key

    /**
     * The request factory of one backend: the engine's shared client with [endpoint]'s response timeout. Called
     * once per backend, when the factory builds its client.
     */
    abstract fun requestFactory(endpoint: ResolvedTimeouts): ClientHttpRequestFactory

    /**
     * The engine's name, `restclient-<engine key>`: the stem of the threads it owns, and the pool name the startup
     * guard sizes the bulkheads against.
     */
    abstract val name: String

    /** The mesh and pool settings this engine applies, each with the value or the engine knob it lands on. */
    abstract val applied: List<String>

    /** The settings this engine has no knob for, each with what happens instead. Never silently empty. */
    abstract val notApplied: List<String>

    /** Which pool meters the engine registers in the host's registry, or why there are none. */
    abstract val poolMeters: String

    /** What an interrupted worker does to the exchange on this engine (class KDoc). */
    abstract val cancellation: String

    /**
     * The exchanges in flight on the engine right now - the connections it has handed out, queued requests included
     * where the engine counts them - or `null` when the engine does not expose the number: the JDK client. What a
     * draining shutdown waits on, and what a test reads to see an abandoned exchange give its connection back.
     */
    abstract fun exchangesInFlight(): Int?

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
     * `app.restclient.pool`, the host's registry for pool meters (`null` = no meters), and for the shutdown the
     * factory's drain [window] (the wait its shutdown deadline is derived from, named in the line of an engine that
     * did not drain) and the [logger] that line goes to.
     */
    class Settings(
        val transport: MeshProperties.Transport,
        val pool: RestClientProperties.Pool,
        val meterRegistry: MeterRegistry?,
        val window: Duration,
        val logger: Logger,
    )
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
