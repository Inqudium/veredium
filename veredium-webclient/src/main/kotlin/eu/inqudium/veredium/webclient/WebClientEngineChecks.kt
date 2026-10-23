package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.engine.EngineSelection
import eu.inqudium.veredium.core.mesh.MeshProperties

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
    private const val KEY = WebClientProperties.Engine.PREFIX

    /** Every override key must be a mesh alias - typo protection the boolean `validation.endpoints` lacks. */
    fun requireKnownAliases(
        engine: WebClientProperties.Engine,
        aliases: Collection<String>,
        foreign: Map<String, String> = emptyMap(),
    ) = EngineSelection.requireKnownAliases(KEY, engine.endpoints.keys, aliases, foreign)

    /** Every engine in use must find its library; the message names what to add, per engine and backend. */
    fun requireLibraries(
        engine: WebClientProperties.Engine,
        engineByBackend: Map<String, WebClientEngine>,
        classLoader: ClassLoader?,
    ) = EngineSelection.requireLibraries(KEY, engineByBackend, engine.endpoints.keys) { it.isPresent(classLoader) }

    /**
     * The "no call waits" value `0` of `pool.pending-acquire-max-count` cannot be expressed on Jetty: its request
     * queue holds a request until it is ON a connection, including while a new connection is opened for it, so
     * a capacity small enough to let nothing wait behind busy connections would also reject a burst the pool
     * could serve - and a capacity of 0 rejects every request ([JettyEngineTransport.jettyQueueCapacity]).
     */
    fun requireJettyQueue(
        engineByBackend: Map<String, WebClientEngine>,
        pool: WebClientProperties.Pool,
    ) {
        require(WebClientEngine.JETTY !in engineByBackend.values || pool.pendingAcquireMaxCount != 0) {
            "app.webclient.pool.pending-acquire-max-count=0 cannot be honoured by the jetty engine selected by $KEY " +
                "for ${backendsOf(WebClientEngine.JETTY, engineByBackend)}: Jetty's request queue also holds the requests " +
                "a connection is being opened for, so it has no setting for \"no call waits\". Use -1 or a positive " +
                "bound, or another engine."
        }
    }

    /**
     * The JDK client refuses an explicit `Host` header unless the JVM was started with
     * [EngineSelection.JDK_ALLOW_RESTRICTED_HEADERS] naming `host` - and every client of this module sends one, from
     * the mesh triplet. [hostHeaderAllowed] asks the JDK itself ([EngineSelection.jdkAllowsHostHeader]).
     */
    fun requireJdkHostHeader(
        engineByBackend: Map<String, WebClientEngine>,
        hostHeaderAllowed: () -> Boolean = EngineSelection::jdkAllowsHostHeader,
    ) {
        if (WebClientEngine.JDK !in engineByBackend.values) return
        require(hostHeaderAllowed()) {
            "$KEY selects 'jdk' for ${backendsOf(WebClientEngine.JDK, engineByBackend)}, but the JDK HTTP client refuses " +
                "the Host header every client of this module sends (from the app.mesh triplet). Start the JVM with " +
                "-D${EngineSelection.JDK_ALLOW_RESTRICTED_HEADERS}=host (it applies to every java.net.http client of the " +
                "process, which is why this module does not set it), or select another engine for these backends."
        }
    }

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
    ): Coverage =
        Knobs(pool, transport).run {
            when (engine) {
                WebClientEngine.REACTOR_NETTY -> reactorNetty()
                WebClientEngine.JDK -> jdk()
                WebClientEngine.JETTY -> jetty()
                WebClientEngine.APACHE -> apache(evictorRuns = !pool.evictInBackground.isZero)
            }
        }

    /** The settings as the startup lines name them, with their values - shared by the four engines' coverage. */
    private class Knobs(
        pool: WebClientProperties.Pool,
        transport: MeshProperties.Transport,
    ) {
        val connect = "connect-timeout-ms=${transport.connectTimeoutMs}"
        val maxConnections = "max-connections=${transport.maxConnections}"
        val pendingAcquire = "pending-acquire-timeout-ms=${transport.pendingAcquireTimeoutMs}"
        val queue = "pool.pending-acquire-max-count=${pool.pendingAcquireMaxCount}"
        val maxIdle = "pool.max-idle-time=${pool.maxIdleTime}"
        val maxLife = "pool.max-life-time=${pool.maxLifeTime}"
        val evict = "pool.evict-in-background=${pool.evictInBackground}"
        val none = "none (the http.client.requests observation and the resilience4j meters apply to every engine)"
    }

    private fun Knobs.reactorNetty(): Coverage =
        Coverage(
            applied =
                listOf(
                    connect,
                    "the response timeout per endpoint (read inactivity)",
                    "$maxConnections (per backend: Reactor Netty keeps a sub-pool per remote address and client configuration)",
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

    private fun Knobs.jdk(): Coverage =
        Coverage(
            applied = listOf(connect, "the response timeout per endpoint (until the response headers arrive)"),
            notApplied = listOf(maxConnections, pendingAcquire, queue, maxIdle, maxLife, evict, "gzip"),
            meters = none,
            cancellation =
                "cancels the client's future, which aborts the exchange while it waits for the head; a body " +
                    "cancelled after the head arrived (a codec refusal, a TimeLimiter abort) is read to its end - " +
                    "the JDK client has no per-exchange abort",
        )

    private fun Knobs.jetty(): Coverage =
        Coverage(
            applied =
                listOf(
                    connect,
                    "the response timeout per endpoint (per-request idle timeout)",
                    "$maxConnections (per destination)",
                    "$queue (waiting beyond max-connections, in the request queue per destination)",
                    "$maxIdle (connection idle timeout)",
                    "$maxLife (connection max duration)",
                    "gzip",
                ),
            notApplied = listOf(pendingAcquire, evict),
            meters = none,
            cancellation = "aborts the request",
        )

    private fun Knobs.apache(evictorRuns: Boolean): Coverage =
        Coverage(
            applied =
                listOfNotNull(
                    connect,
                    "the response timeout per endpoint (socket inactivity)",
                    "$maxConnections (per route)",
                    "$maxLife (time to live)",
                    maxIdle.takeIf { evictorRuns },
                    evict.takeIf { evictorRuns },
                    "gzip",
                ),
            notApplied =
                listOfNotNull(
                    "$pendingAcquire (set as the connection-request timeout, but Apache's async pool checks it only when the pool is next used; the TimeLimiter bounds the wait)",
                    queue,
                    maxIdle.takeUnless { evictorRuns },
                ),
            meters = none,
            cancellation = "cancels the exchange's future, which aborts it",
        )

    private fun backendsOf(
        engine: WebClientEngine,
        engineByBackend: Map<String, WebClientEngine>,
    ): List<String> = engineByBackend.filterValues { it == engine }.keys.sorted()
}
