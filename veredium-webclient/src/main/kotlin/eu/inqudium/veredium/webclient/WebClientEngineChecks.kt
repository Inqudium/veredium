package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import org.springframework.util.ClassUtils
import java.net.URI

/**
 * The start-time checks and startup lines of `app.webclient.engine` - everything [ResilientWebClientFactory] can
 * say about an engine choice before any engine class is loaded.
 *
 * Each check fails with a message naming the property key, because a wrong engine setting does not announce
 * itself otherwise: a misspelt override key silently leaves its backend on the default engine, a missing
 * library fails with a `NoClassDefFoundError` at the first client, and the JDK client's Host restriction fails
 * every request of the backend with an `IllegalArgumentException` that names no configuration at all.
 */
internal object WebClientEngineChecks {
    private const val KEY = "app.webclient.engine"

    /** The JVM property that lifts the JDK client's restriction on the Host header; never set by this module. */
    const val JDK_RESTRICTED_HEADERS_PROPERTY: String = "jdk.httpclient.allowRestrictedHeaders"

    /** Every override key must be a mesh alias - typo protection the boolean `validation.endpoints` lacks. */
    fun requireKnownAliases(
        engine: WebClientProperties.Engine,
        aliases: Collection<String>,
    ) {
        val unknown =
            engine.endpoints.keys
                .filterNot { it in aliases }
                .sorted()
        require(unknown.isEmpty()) {
            "$KEY.endpoints names no app.mesh endpoint alias: $unknown. Known aliases: ${aliases.sorted()}"
        }
    }

    /** Every engine in use must find its library; the message names what to add, per engine and backend. */
    fun requireLibraries(
        engineByBackend: Map<String, WebClientEngine>,
        classLoader: ClassLoader?,
    ) {
        val missing =
            engineByBackend.values.toSortedSet().filterNot { engine ->
                engine.requiredClasses.all { ClassUtils.isPresent(it, classLoader) }
            }
        require(missing.isEmpty()) {
            missing.joinToString("; ") { engine ->
                "$KEY selects '$engine' for ${backendsOf(engine, engineByBackend)}, but its library is not on the " +
                    "classpath - add ${engine.coordinates}"
            }
        }
    }

    /**
     * Jetty queues EVERY request before it gets a connection, so the "never queue" value `0` of
     * `pool.pending-acquire-max-count` cannot be carried over: it would reject every request of the engine.
     */
    fun requireJettyQueue(
        engineByBackend: Map<String, WebClientEngine>,
        pool: WebClientProperties.Pool,
    ) {
        require(WebClientEngine.JETTY !in engineByBackend.values || pool.pendingAcquireMaxCount != 0) {
            "app.webclient.pool.pending-acquire-max-count=0 cannot be honoured by the jetty engine selected by $KEY " +
                "for ${backendsOf(WebClientEngine.JETTY, engineByBackend)}: Jetty queues every request before it gets " +
                "a connection, so 0 would reject all of them. Use -1 or a positive bound, or another engine."
        }
    }

    /**
     * The JDK client refuses an explicit `Host` header unless the JVM was started with
     * [JDK_RESTRICTED_HEADERS_PROPERTY] naming `host` - and every client of this module sends one, from the mesh
     * triplet. [hostHeaderAllowed] asks the JDK itself rather than reading the property, because the JDK reads it
     * once, when its HTTP client classes initialise: a value set later is visible in `System.getProperty` and
     * ignored by the client.
     */
    fun requireJdkHostHeader(
        engineByBackend: Map<String, WebClientEngine>,
        hostHeaderAllowed: () -> Boolean = ::jdkAllowsHostHeader,
    ) {
        if (WebClientEngine.JDK !in engineByBackend.values) return
        require(hostHeaderAllowed()) {
            "$KEY selects 'jdk' for ${backendsOf(WebClientEngine.JDK, engineByBackend)}, but the JDK HTTP client refuses " +
                "the Host header every client of this module sends (from the app.mesh triplet). Start the JVM with " +
                "-D$JDK_RESTRICTED_HEADERS_PROPERTY=host (it applies to every java.net.http client of the process, which " +
                "is why this module does not set it), or select another engine for these backends."
        }
    }

    /** Whether this JVM's `java.net.http` lets a request carry an explicit `Host` header. */
    fun jdkAllowsHostHeader(): Boolean =
        runCatching {
            java.net.http.HttpRequest
                .newBuilder(URI.create("http://localhost/"))
                .header("Host", "probe")
        }.isSuccess

    /** The startup line naming the engine of every backend, sorted by alias. */
    fun engineLine(
        engine: WebClientProperties.Engine,
        engineByBackend: Map<String, WebClientEngine>,
    ): String {
        val backends =
            engineByBackend.entries
                .sortedBy { it.key }
                .joinToString(", ") { (alias, selected) -> "$alias=$selected" }
                .ifEmpty { "none" }
        return "HTTP engine per backend ($KEY, default ${engine.default}): $backends"
    }

    /**
     * The startup line of one engine in use: which mesh transport and pool settings it applies, which it cannot,
     * which transport meters it exposes and what a cancelled call does to the exchange.
     */
    fun coverageLine(
        engine: WebClientEngine,
        engineByBackend: Map<String, WebClientEngine>,
        pool: WebClientProperties.Pool,
        transport: MeshProperties.Transport,
    ): String {
        val coverage = coverageOf(engine, pool, transport)
        return "HTTP engine '$engine' (${engine.library}) for ${backendsOf(engine, engineByBackend)}: " +
            "applies ${coverage.applied.joinToString(", ")}; " +
            "does NOT apply ${coverage.notApplied.joinToString(", ").ifEmpty { "-" }}; " +
            "transport meters ${coverage.meters}; cancellation ${coverage.cancellation}"
    }

    private class Coverage(
        val applied: List<String>,
        val notApplied: List<String>,
        val meters: String,
        val cancellation: String,
    )

    private fun coverageOf(
        engine: WebClientEngine,
        pool: WebClientProperties.Pool,
        transport: MeshProperties.Transport,
    ): Coverage {
        val connect = "connect-timeout-ms=${transport.connectTimeoutMs}"
        val maxConnections = "max-connections=${transport.maxConnections}"
        val pendingAcquire = "pending-acquire-timeout-ms=${transport.pendingAcquireTimeoutMs}"
        val queue = "pool.pending-acquire-max-count=${pool.pendingAcquireMaxCount}"
        val maxIdle = "pool.max-idle-time=${pool.maxIdleTime}"
        val maxLife = "pool.max-life-time=${pool.maxLifeTime}"
        val evict = "pool.evict-in-background=${pool.evictInBackground}"
        val none = "none (the http.client.requests observation and the resilience4j meters apply to every engine)"
        return when (engine) {
            WebClientEngine.REACTOR_NETTY -> {
                Coverage(
                    applied =
                        listOf(
                            connect,
                            "the response timeout per endpoint (read inactivity)",
                            "$maxConnections (per remote address)",
                            pendingAcquire,
                            queue,
                            maxIdle,
                            maxLife,
                            evict,
                            "gzip",
                        ),
                    notApplied = emptyList(),
                    meters = "reactor.netty.http.client.* (uri tag templated) and reactor.netty.connection.provider.*",
                    cancellation = "closes the exchange's connection",
                )
            }

            WebClientEngine.JDK -> {
                Coverage(
                    applied = listOf(connect, "the response timeout per endpoint (until the response headers arrive)"),
                    notApplied = listOf(maxConnections, pendingAcquire, queue, maxIdle, maxLife, evict, "gzip"),
                    meters = none,
                    cancellation = "cancels the client's future, which aborts the exchange",
                )
            }

            WebClientEngine.JETTY -> {
                Coverage(
                    applied =
                        listOf(
                            connect,
                            "the response timeout per endpoint (per-request idle timeout)",
                            "$maxConnections (per destination)",
                            "$queue (as the request queue per destination)",
                            "$maxIdle (connection idle timeout)",
                            "$maxLife (connection max duration)",
                            "gzip",
                        ),
                    notApplied = listOf(pendingAcquire, evict),
                    meters = none,
                    cancellation = "aborts the request",
                )
            }

            WebClientEngine.APACHE -> {
                val evictorRuns = !pool.evictInBackground.isZero
                Coverage(
                    applied =
                        listOfNotNull(
                            connect,
                            "the response timeout per endpoint (socket inactivity)",
                            "$maxConnections (per route)",
                            "$pendingAcquire (connection-request timeout)",
                            "$maxLife (time to live)",
                            maxIdle.takeIf { evictorRuns },
                            evict.takeIf { evictorRuns },
                            "gzip",
                        ),
                    notApplied = listOfNotNull(queue, maxIdle.takeUnless { evictorRuns }),
                    meters = none,
                    cancellation = "cancels the exchange's future, which aborts it",
                )
            }
        }
    }

    private fun backendsOf(
        engine: WebClientEngine,
        engineByBackend: Map<String, WebClientEngine>,
    ): List<String> = engineByBackend.filterValues { it == engine }.keys.sorted()
}
