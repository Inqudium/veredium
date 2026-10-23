package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.engine.EngineDrain
import eu.inqudium.veredium.core.engine.EngineSelection
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.client.ClientHttpRequest
import org.springframework.http.client.ClientHttpRequestFactory
import org.springframework.http.client.JdkClientHttpRequestFactory
import java.net.URI
import java.net.http.HttpClient
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * The JDK engine ([RestClientEngine.JDK]): one `java.net.http.HttpClient` for every backend on it, one Spring
 * [JdkClientHttpRequestFactory] per backend over that client carrying the backend's response timeout.
 *
 * ## What maps, and what the JDK client cannot express
 *
 * | Setting                          | Here                                                                         |
 * |----------------------------------|------------------------------------------------------------------------------|
 * | `connect-timeout-ms`             | `HttpClient.Builder.connectTimeout`                                          |
 * | per-endpoint response timeout    | Spring's read timeout - a deadline from sending the request until the response body is closed, i.e. head AND body together (Spring arms it because `HttpRequest.timeout` does not cover the body) |
 * | `max-connections`                | none - the JDK pool has no size limit; a connection is opened per concurrent exchange |
 * | `pending-acquire-timeout-ms`     | none - nothing waits for a connection, so there is nothing to bound          |
 * | `pool.max-idle-time` / `max-life-time` / `evict-in-background` | none - the JDK's keep-alive timeout is the JVM-wide system property `jdk.httpclient.keepalive.timeout` |
 * | pool meters                      | none - the JDK exposes no pool statistics                                    |
 *
 * The JDK knobs that DO exist are JVM-wide system properties (`jdk.httpclient.keepalive.timeout`,
 * `jdk.httpclient.connectionPoolSize`, `jdk.httpclient.disableRetryConnect`, ...). Setting one would change every
 * `java.net.http` client of the process, so this class sets none; the startup line names what therefore does not
 * apply, rather than leaving it to be discovered.
 *
 * What the JDK client does by default and the Apache engine does not is switched off where the JDK lets a client
 * decide: HTTP/1.1 only (the default would offer an h2c upgrade on every plaintext connection), no redirects, no
 * proxy (the default would honour the `http.proxyHost` family of system properties; the sidecar is on loopback).
 * Two JDK behaviours have only JVM-wide switches and stay as the JDK has them: a refused connect is retried once
 * (within the same connect timeout), and an idempotent GET whose pooled connection the peer had closed is re-sent
 * on a fresh one.
 *
 * ## The Host header
 *
 * `Host` is a RESTRICTED header of the JDK client: Spring's request drops it and the JDK sends the authority of
 * the request URI instead, unless the JVM was started with `-Djdk.httpclient.allowRestrictedHeaders=host`
 * ([EngineSelection.JDK_ALLOW_RESTRICTED_HEADERS], read once, by the JDK and by Spring). Behind the sidecar that would route every call by `localhost:<port>` - so the
 * factory refuses to start with this engine for a backend whose Host header differs from the authority of its
 * base URL ([hostHeaderConflict]). The system property is the host's decision, never this module's: it changes
 * every `java.net.http` client of the JVM.
 *
 * **Cancellation.** Both blocking points honour an interrupt: waiting for the response head, Spring cancels the
 * JDK's response future, which aborts the exchange; reading the body, the JDK's stream closes itself and fails
 * the read. The worker ends at once either way.
 *
 * **A body not read to its end.** Spring's response DRAINS the rest of the body in its close, for as long as the
 * peer takes, up to the response deadline - for a body refused at the buffer limit, the rest of a body nobody
 * wants, read on the worker with its permit. [requestFactory] therefore hands out an [AbortingRequest] whose abort
 * closes the JDK's body stream first, which cancels the exchange and drops its connection.
 */
internal class JdkEngineTransport(
    settings: Settings,
) : EngineTransport(RestClientEngine.JDK) {
    private val meshTransport = settings.transport
    private val pool = settings.pool
    private val window = settings.window
    private val logger = settings.logger

    override val name: String = CLIENT_NAME

    /**
     * The client's executor and Spring's request-body writer, owned here and shut down with the client. Virtual
     * threads, named after the engine, instead of the JDK's per-client cached platform pool and Spring's
     * thread-per-write fallback.
     */
    private val executor: ExecutorService = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("$CLIENT_NAME-", 0).factory())

    /** The one client of this engine, shared by every backend on it. `internal` so a test can see it terminate. */
    internal val httpClient: HttpClient =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofMillis(meshTransport.connectTimeoutMs))
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NEVER)
            .proxy(HttpClient.Builder.NO_PROXY)
            .executor(executor)
            .build()

    override fun requestFactory(endpoint: ResolvedTimeouts): ClientHttpRequestFactory = AbortingRequestFactory(httpClient, executor).apply { setReadTimeout(endpoint.responseTimeout) }

    override val applied: List<String> =
        listOf(
            "connect-timeout-ms=${meshTransport.connectTimeoutMs} (HttpClient.connectTimeout)",
            "the endpoint's response timeout (a deadline over head and body together)",
        )

    override val notApplied: List<String> =
        listOf(
            "max-connections=${meshTransport.maxConnections} (the JDK pool is unbounded)",
            "pending-acquire-timeout-ms=${meshTransport.pendingAcquireTimeoutMs} (nothing waits for a connection)",
            "pool.max-idle-time=${pool.maxIdleTime}, pool.max-life-time=${pool.maxLifeTime}, " +
                "pool.evict-in-background=${pool.evictInBackground} (JVM-wide system properties only, not set)",
        )

    override val poolMeters: String = "none (the JDK exposes no pool statistics)"

    override val cancellation: String = "an interrupted worker cancels the JDK's exchange future or fails its body read, at once"

    override val isClosed: Boolean get() = httpClient.isTerminated

    override fun exchangesInFlight(): Int? = null

    /**
     * Stops accepting requests, waits until the deadline for the exchanges in flight, then aborts the rest.
     * The JDK client drains itself (`shutdown` + `awaitTermination`); an expiry or an interrupt of the shutdown
     * thread ends the wait, is reported once, and `shutdownNow` aborts what is left.
     */
    override fun shutdown(
        deadlineNanos: Long,
        nanoTime: () -> Long,
    ) {
        httpClient.shutdown()
        val terminated =
            try {
                httpClient.awaitTermination(EngineDrain.remaining(deadlineNanos, nanoTime))
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                false
            }
        if (!terminated) {
            EngineDrain.reportUndrained(logger, engine.key, window, left = null)
            httpClient.shutdownNow()
        }
        executor.shutdownNow()
    }

    /**
     * Spring's request factory, handing out [AbortingRequest]s whose abort closes the JDK's body stream (class
     * KDoc, "A body not read to its end"). Still a [JdkClientHttpRequestFactory], with its read timeout.
     */
    private class AbortingRequestFactory(
        httpClient: HttpClient,
        executor: ExecutorService,
    ) : JdkClientHttpRequestFactory(httpClient, executor) {
        override fun createRequest(
            uri: URI,
            httpMethod: HttpMethod,
        ): ClientHttpRequest =
            // Assumption: closing the InputStream of a JDK HttpResponse before its end cancels the exchange and
            // closes its connection (java.net.http, JDK 21+), and Spring's response hands out that stream as its body.
            AbortingRequest(super.createRequest(uri, httpMethod)) { response, _ -> response?.body?.close() }
    }

    companion object {
        /** The engine's [name]: its threads' name prefix. */
        const val CLIENT_NAME: String = "restclient-jdk"

        /**
         * Why the JDK engine cannot carry [endpoint]'s Host header, or `null` when it can: the header the factory
         * puts on the backend's requests (the triplet's host, or an endpoint `headers` entry named Host) against
         * the authority the JDK sends instead - the base URL's host, with its port unless it is the scheme's
         * default, exactly as `java.net.http` renders it. Compared case-insensitively, host names being so.
         *
         * [hostAllowed] says whether the JVM's JDK client accepts the header as given
         * ([EngineSelection.jdkAllowsHostHeader]); then both the JDK and Spring send it, and there is no conflict.
         */
        fun hostHeaderConflict(
            endpoint: ResolvedTimeouts,
            hostAllowed: Boolean,
        ): String? {
            if (hostAllowed) return null
            val wanted =
                endpoint.headers.entries
                    .lastOrNull { it.key.equals(HttpHeaders.HOST, ignoreCase = true) }
                    ?.value ?: endpoint.host
            val sent = jdkAuthority(URI.create(endpoint.baseUrl))
            return if (wanted.equals(sent, ignoreCase = true)) null else "it would send Host '$sent' instead of '$wanted'"
        }

        /** The Host value `java.net.http` derives from a request URI: the host, plus the port unless it is the scheme's default. */
        private fun jdkAuthority(uri: URI): String {
            val defaultPort = uri.port == -1 || uri.port == if (uri.scheme.equals("https", ignoreCase = true)) 443 else 80
            return if (defaultPort) uri.host else "${uri.host}:${uri.port}"
        }
    }
}
