package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.Veredium
import eu.inqudium.veredium.core.engine.EngineSelection
import eu.inqudium.veredium.core.validation.ResponseValidationProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.util.unit.DataSize
import java.time.Duration

/**
 * GLOBAL configuration for the WebClients produced by [ResilientWebClientFactory] - everything NOT
 * per-endpoint. The per-endpoint config (base URL, Host, timeout ladder) lives in `app.mesh.endpoints[]`,
 * so one factory serves several backends from that tree, on the resources of each HTTP [engine] in use.
 *
 * Only knobs with no mesh equivalent stay here: `maxInMemorySize`, the pool lifecycle and the engine. The mesh
 * transport config owns `maxConnections` / `pendingAcquireTimeout`, the ladder owns the response timeout.
 *
 * Every value is checked while BINDING, the way [eu.inqudium.veredium.core.mesh.MeshProperties] and
 * [eu.inqudium.veredium.core.mesh.ResolvedTimeouts] do it - and for the same reason: a wrong number here does not
 * announce itself. It binds, the context starts, and the effect shows up as a runtime symptom that points anywhere
 * but at the configuration. The messages therefore name the PROPERTY KEY, not the Kotlin field, because the key is
 * what the person reading the failure has to edit.
 */
@ConfigurationProperties(prefix = "app.webclient")
data class WebClientProperties(
    /**
     * Upper bound for buffered response bodies. Guards against unbounded memory use on large payloads.
     *
     * **Why 256 KB and not less.** This is Spring's own `ExchangeStrategies` default, so it is the value a
     * reader already expects; a deviation is what needs justifying, not this. 8 KB proved too small for a list
     * response from these backends: the backend answers `200 OK` promptly and the codec aborts mid-buffer, on
     * every retry of the same call.
     *
     * **Why not more.** The limit applies PER CONCURRENTLY BUFFERED RESPONSE, and how many calls run at once is
     * bounded by the mesh bulkheads, whose sum the reference configuration holds at `maxConnections` (100,
     * [eu.inqudium.veredium.core.mesh.MeshProperties.Transport]). At 256 KB the worst case is ~25 MB of response
     * buffers; at the 1 MB some other clients use it would be ~100 MB, which is real money on a service that
     * otherwise needs little heap.
     *
     * **Why a limit at all** - see the range check in `init`: without one, a single degenerate response takes
     * the pod down, and the failure mode of getting it wrong is documented there.
     */
    val maxInMemorySize: DataSize = DataSize.ofKilobytes(256),
    /**
     * The connection-pool settings of every engine. What a pool is shared by depends on the engine: Reactor Netty
     * keeps a sub-pool per BACKEND inside the one provider of the factory (per remote address and per client
     * configuration, and every backend's client is configured on its own), Jetty and Apache share one pool per
     * destination between the backends behind it, and the JDK client has no per-client pool settings at all.
     * Per-backend isolation is owned by the R4j bulkhead / circuit breaker / time limiter on every engine.
     * `maxConnections` and `pendingAcquireTimeout` come from the mesh transport config.
     *
     * Written in Reactor Netty's terms, the default engine, which applies all four knobs. The other engines
     * apply what they have an equivalent for and say at startup which knobs they do not apply
     * ([WebClientEngine]; the configuration guide tabulates it).
     */
    val pool: Pool = Pool(),
    /** HTTP client Micrometer Observation - self-contained; it reads only this module's property. */
    val observability: Observability = Observability(),
    /**
     * Whether a deserialised response is checked against its DTO's Bean Validation constraints. A global default
     * with per-endpoint (backend-alias) overrides - the shared [ResponseValidationProperties] of `veredium-core`.
     */
    val validation: ResponseValidationProperties = ResponseValidationProperties(),
    /**
     * The `User-Agent` every client sends, when the host wants to name itself rather than this library.
     * `null` (the default) sends the library's token `veredium/<version>` ([Veredium.userAgent]); a value
     * replaces it verbatim, trimmed. An endpoint's static `headers` under `app.mesh` still win over either.
     * Validated at bind time ([Veredium.resolveUserAgent]): blank or with a control character is rejected,
     * naming the key, because Reactor Netty would otherwise refuse the header per request with a message that
     * points nowhere near the configuration.
     */
    val userAgent: String? = null,
    /**
     * Which HTTP engine carries the clients of this module - a default with per-backend (mesh alias)
     * overrides, see [Engine]. The code default is Reactor Netty, the engine Boot's own WebClient
     * auto-configuration prefers.
     */
    val engine: Engine = Engine(),
) {
    /** The token the factory puts on every request: [userAgent], or the library's own. */
    val effectiveUserAgent: String = Veredium.resolveUserAgent(userAgent, "app.webclient.user-agent")

    init {
        // Both ends of this range are load-bearing.
        //
        // At the bottom: a zero or negative limit binds silently and then fails EVERY call with a
        // DataBufferLimitException - which WebClientVerdicts.clientSideFailure deliberately keeps off the circuit
        // breaker, so no breaker opens and the picture in monitoring is "all four backends are down" while the
        // cause is one line of YAML. The protection works against the diagnosis here, which is exactly why the
        // value must never get there.
        //
        // At the top: ResilientWebClientFactory narrows this to an Int for the codec. Anything past
        // Int.MAX_VALUE would wrap to a negative number and land back at the bottom case - by a route nobody
        // would suspect from looking at the configured value.
        require(maxInMemorySize.toBytes() in 1..Int.MAX_VALUE.toLong()) {
            "app.webclient.max-in-memory-size must be between 1B and ${Int.MAX_VALUE}B, but was $maxInMemorySize"
        }
    }

    /**
     * The lifecycle knobs of each engine's connection pool, in Reactor Netty's terms.
     *
     * @property pendingAcquireMaxCount How many calls may wait for a connection beyond the ones holding one;
     *   `-1` = unbounded, `0` = none wait.
     * @property maxIdleTime Close a pooled connection that sat idle for longer than this. Must be positive: zero
     *   has no common meaning across the engines (see `init`).
     * @property maxLifeTime Close a connection older than this regardless of use; `0` = unbounded.
     * @property evictInBackground How often idle and expired connections are swept; `0` = off.
     */
    data class Pool(
        val pendingAcquireMaxCount: Int = 100,
        val maxIdleTime: Duration = Duration.ofSeconds(30),
        val maxLifeTime: Duration = Duration.ofMinutes(5),
        val evictInBackground: Duration = Duration.ofSeconds(30),
    ) {
        init {
            // -1 is Reactor Netty's "unbounded" and stays allowed; 0 means "never queue", which is a defensible
            // fail-fast. Anything below -1 the pool builder rejects too, but it does so at factory construction
            // and with a message that names neither the property nor its source.
            require(pendingAcquireMaxCount >= -1) {
                "app.webclient.pool.pending-acquire-max-count must be >= -1 (-1 = unbounded), " +
                    "but was $pendingAcquireMaxCount"
            }
            // Negative durations are accepted by the pool builder and then quietly mean something no operator
            // intended: a connection that is due for eviction the moment it is created. Zero is left alone where
            // every engine reads it the same way - for evictInBackground it is "off", for maxLifeTime "unbounded".
            //
            // Rationale: not for maxIdleTime - the engines read a zero idle time in OPPOSITE ways. Reactor Netty
            // evicts every connection whose idle time is >= the limit, i.e. every released one, so keep-alive is
            // silently gone and each call opens a new connection; Jetty reads 0 as "no idle timeout" and never
            // closes an idle connection; Apache's evictor closes every idle connection on each sweep. There is no
            // one meaning to give the value, so it is refused, naming the key.
            require(maxIdleTime.isPositive) {
                "app.webclient.pool.max-idle-time must be positive (0 would mean no keep-alive on Reactor Netty and " +
                    "no idle timeout on Jetty), but was $maxIdleTime"
            }
            require(!maxLifeTime.isNegative) { "app.webclient.pool.max-life-time must not be negative, but was $maxLifeTime" }
            require(!evictInBackground.isNegative) {
                "app.webclient.pool.evict-in-background must not be negative, but was $evictInBackground"
            }
        }
    }

    /**
     * The HTTP engine per backend: [default] for every mesh alias, [endpoints] overriding it per alias - the same
     * shape as `validation`.
     *
     * **Scoped to this module.** The setting chooses the connector the factory puts on ITS clone of Boot's
     * `WebClient.Builder`; Boot's builder bean, Boot's connector and Boot's own switch
     * (`spring.http.clients.reactive.connector`) are neither read nor changed, so every other WebClient of the
     * host keeps the engine Boot gave it. That is the reason this is a property of its own rather than an
     * instruction to set Boot's: Boot's switch is global to the host.
     *
     * **Checked against the mesh** when the factory is constructed, not here (this class does not know the
     * aliases): an [endpoints] key that names no `app.mesh` alias fails the start - unlike a misspelt
     * `validation.endpoints` key, which would silently fall back to the default - and so does an engine whose
     * library is missing from the classpath, naming the Maven coordinates to add
     * ([WebClientEngine.dependency]).
     *
     * @property default The engine of every alias without an entry in [endpoints]. YAML: `reactor-netty`
     *   (default), `jdk`, `jetty`, `apache`.
     * @property endpoints Per-alias overrides, keyed by the mesh endpoint alias (the backend name).
     */
    data class Engine(
        val default: WebClientEngine = WebClientEngine.REACTOR_NETTY,
        val endpoints: Map<String, WebClientEngine> = emptyMap(),
    ) {
        /** The engine of [alias]: its own override when set, otherwise [default]. */
        fun engineFor(alias: String): WebClientEngine = endpoints[alias] ?: default

        /** The key [alias]'s engine is selected by - its override, or the default - for a message naming it. */
        fun keyFor(alias: String): String = EngineSelection.keyFor(PREFIX, alias, alias in endpoints)

        companion object {
            /** The prefix of this block. */
            const val PREFIX: String = "app.webclient.engine"
        }
    }

    /**
     * Toggle for Spring WebClient's NATIVE Micrometer Observation on the outbound HTTP calls of THIS module.
     * [ResilientWebClientFactory] clones Boot's auto-configured
     * [org.springframework.web.reactive.function.client.WebClient.Builder], on which Boot's
     * `ObservationWebClientCustomizer` has already placed the host's `ObservationRegistry` (and observation
     * convention) - the factory does not wire the registry itself. This flag has two jobs: it gates the JVM-wide
     * Reactor context-propagation hook ([WebClientReactorContextPropagationConfiguration]), and when `false` it
     * REPLACES the registry Boot wired with a NOOP, so this module's clients open no `http.client.requests`
     * observation while every other WebClient of the host keeps its own. When `true`, each request opens that
     * observation; with a tracing bridge on the (host's) classpath that becomes a CLIENT span and injects the
     * W3C `traceparent` into the outgoing request. With only a metrics registry it records the client timer;
     * with no `ObservationRegistry` bean at all Boot wires nothing and it is a NOOP.
     *
     * This is DELIBERATELY self-contained: it resolves from this module's own property and from nothing else of
     * the host, so the module traces its HTTP calls whatever else is on the classpath. The central switch for ALL
     * WebClients of the host is Spring's `management.observations.enable.*` filter, which acts on the registry
     * itself and is orthogonal to this flag.
     *
     * @property httpObservationEnabled Default `false` (opt-in): the module traces its HTTP calls only when the
     *   operator asks for it. Safe to flip on even without a tracer present (NOOP).
     */
    data class Observability(
        val httpObservationEnabled: Boolean = false,
    )
}
