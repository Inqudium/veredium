package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.Veredium
import eu.inqudium.veredium.core.engine.EngineSelection
import eu.inqudium.veredium.core.validation.ResponseValidationProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.util.unit.DataSize
import java.time.Duration

/**
 * GLOBAL configuration for the RestClients produced by [ResilientRestClientFactory] - everything NOT
 * per-endpoint. The per-endpoint config (base URL, Host, timeout ladder) lives in `app.mesh.endpoints[]`,
 * so one factory serves several backends from that tree with one connection pool per engine ([pool]).
 *
 * Only knobs with no mesh equivalent stay here: [maxInMemorySize], the pool lifecycle and the HTTP [engine]. The
 * mesh transport config owns [eu.inqudium.veredium.core.mesh.MeshProperties.Transport.maxConnections] /
 * [eu.inqudium.veredium.core.mesh.MeshProperties.Transport.pendingAcquireTimeoutMs], the ladder owns the response
 * timeout.
 *
 * Every value is checked while BINDING, the way [eu.inqudium.veredium.core.mesh.MeshProperties] and
 * [eu.inqudium.veredium.core.mesh.ResolvedTimeouts] do it - and for the same reason: a wrong number here does not
 * announce itself. It binds, the context starts, and the effect shows up as a runtime symptom that points anywhere
 * but at the configuration. The messages therefore name the PROPERTY KEY, not the Kotlin field, because the key is
 * what the person reading the failure has to edit.
 */
@ConfigurationProperties(prefix = "app.restclient")
data class RestClientProperties(
    /**
     * Upper bound for a buffered response body. Guards against unbounded memory use on large payloads.
     *
     * A blocking RestClient has no codec limit of its own: its message converters read whatever the peer sends.
     * The bound is therefore enforced by [ResilientRestClient] itself, which reads the body through a bounded
     * buffer and fails the call with a [ResponseBodyLimitExceededException] once the limit is passed - the
     * counterpart of the reactive codec's `DataBufferLimitException`, and discounted by the circuit breaker for
     * the same reason (see [RestClientVerdicts.clientSideFailure]).
     *
     * **Why 256 KB and not less.** This is the WebClient twin's ceiling, kept so that the two modules size their
     * buffers identically against the same backends. 8 KB is too small for a typical JSON list response, and a
     * call refused on its buffer spends its whole retry cap on the same answer.
     *
     * **Why not more.** The limit applies PER CONCURRENTLY BUFFERED RESPONSE, and the mesh bulkheads admit
     * `maxConnections` calls in all (100, [eu.inqudium.veredium.core.mesh.MeshProperties.Transport]). At 256 KB the worst
     * case is ~25 MB of response buffers; at the 1 MB some other clients use it would be ~100 MB, which is real
     * money on a service that otherwise needs little heap.
     *
     * **Why a limit at all** - see the range check in `init`: without one, a single degenerate response takes
     * the pod down, and the failure mode of getting it wrong is documented there.
     */
    val maxInMemorySize: DataSize = DataSize.ofKilobytes(256),
    /**
     * One shared connection pool per engine for ALL endpoints on it. Every endpoint connects to the same local
     * sidecar over loopback, so per-endpoint pool isolation is not worth the lost connection reuse; per-backend
     * isolation is owned by the R4j bulkhead / circuit breaker / time limiter instead. `maxConnections` and
     * `pendingAcquireTimeout` come from the mesh transport config. Every knob applies to the Apache engine; Jetty
     * and Reactor Netty take the ones they have an equivalent for, the JDK engine none (see [Pool]).
     */
    val pool: Pool = Pool(),
    /** HTTP client Micrometer Observation - self-contained; reads only this module's own property. */
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
     * naming the key, because the engine would otherwise refuse the header per request with a message that
     * points nowhere near the configuration.
     */
    val userAgent: String? = null,
    /**
     * Which HTTP engine carries the exchanges of this module's clients: a default with per-backend overrides.
     * Isolated by construction - it selects the engine of the clients [ResilientRestClientFactory] builds and of
     * nothing else in the host (see [Engine]).
     */
    val engine: Engine = Engine(),
) {
    /** The token the factory puts on every request: [userAgent], or the library's own. */
    val effectiveUserAgent: String = Veredium.resolveUserAgent(userAgent, "app.restclient.user-agent")

    init {
        // Both ends of this range are load-bearing.
        //
        // At the bottom: a zero or negative limit binds silently and then fails EVERY call with a
        // ResponseBodyLimitExceededException - which RestClientVerdicts.clientSideFailure deliberately keeps off the
        // circuit breaker, so no breaker opens and the picture in monitoring is "all four backends are down"
        // while the cause is one line of YAML. The protection works against the diagnosis here, which is exactly
        // why the value must never get there.
        //
        // At the top: ResilientRestClientFactory narrows this to an Int for the bounded body buffer. Anything
        // past Int.MAX_VALUE would wrap to a negative number and land back at the bottom case - by a route
        // nobody would suspect from looking at the configured value.
        require(maxInMemorySize.toBytes() in 1..Int.MAX_VALUE.toLong()) {
            "app.restclient.max-in-memory-size must be between 1B and ${Int.MAX_VALUE}B, but was $maxInMemorySize"
        }
    }

    /**
     * The lifecycle knobs of the shared connection pool. Written for Apache HttpClient 5, the default engine, and
     * applied there in full; the other engines take what they have an equivalent for - Reactor Netty all three,
     * Jetty [maxIdleTime] (its connection idle timeout) and [maxLifeTime] (its pool's max duration, checked at
     * lease time), the JDK engine none, since its pool is configurable only through JVM-wide system properties.
     * Each engine's startup line names the knobs it applies and the ones it does not.
     *
     * The WebClient twin's `pending-acquire-max-count` has no counterpart here, deliberately: Apache's pool has
     * no bounded pending queue. A caller waiting for a connection is bounded in TIME only, by the mesh transport's
     * `pending-acquire-timeout-ms` (the connection-request timeout), which is also how the bulkheads are sized -
     * they shed load before the pool queue builds up at all.
     */
    data class Pool(
        /**
         * Evict a connection that sat idle in the pool for longer than this. On the Apache engine enforced by its
         * eviction schedule (`closeIdle`), every [evictInBackground]; the other engines apply it by their own
         * mechanism or not at all ([Pool]).
         *
         * Must be positive. Zero has no reading the engines agree on: Reactor Netty evicts every idle connection at
         * once (no keep-alive at all), Jetty takes it as "no idle timeout", Apache closes all idle connections at
         * every sweep - so it is refused at binding rather than meaning three things.
         */
        val maxIdleTime: Duration = Duration.ofSeconds(30),
        /**
         * Evict a connection older than this regardless of use - the pool's connection time-to-live. Zero
         * means no lifetime bound (Apache's default).
         */
        val maxLifeTime: Duration = Duration.ofMinutes(5),
        /**
         * How often the Apache engine's eviction schedule runs `closeIdle` / `closeExpired` on its pool; Reactor
         * Netty's `evictInBackground` takes the same value, Jetty and the JDK engine have no counterpart ([Pool]).
         * Zero switches background eviction off, the same reading Reactor Netty gives the value.
         */
        val evictInBackground: Duration = Duration.ofSeconds(30),
    ) {
        init {
            // Negative durations are accepted by Apache's builders and then quietly mean something no operator
            // intended: a connection that is due for eviction the moment it is created. Zero is left alone where
            // every engine reads it alike - for evictInBackground it is "off", for maxLifeTime it is "unbounded" -
            // and refused for maxIdleTime, where the engines read it in opposite ways (see its KDoc).
            require(maxIdleTime.isPositive) {
                "app.restclient.pool.max-idle-time must be positive (0 would mean no keep-alive on Reactor Netty and " +
                    "no idle timeout on Jetty), but was $maxIdleTime"
            }
            require(!maxLifeTime.isNegative) { "app.restclient.pool.max-life-time must not be negative, but was $maxLifeTime" }
            require(!evictInBackground.isNegative) {
                "app.restclient.pool.evict-in-background must not be negative, but was $evictInBackground"
            }
        }
    }

    /**
     * The HTTP engine of this module's clients - `app.restclient.engine.default` and the per-backend overrides
     * `app.restclient.engine.endpoints.<mesh-alias>`, the same shape as [validation]:
     *
     * ```yaml
     * app:
     *   restclient:
     *     engine:
     *       default: apache                                  # apache | jdk | jetty | reactor-netty
     *       endpoints:
     *         apis-example-com-GET-v6-involved-parties: jetty  # only THIS backend runs on Jetty
     * ```
     *
     * **Isolation.** The selection reaches the clients [ResilientRestClientFactory] builds and nothing else:
     * every engine is an object the factory owns and sets on its own clone of Boot's `RestClient.Builder`. Boot's
     * `spring.http.clients.imperative.factory` is neither read nor written, no request factory or HTTP client
     * bean is registered, and a RestClient the host builds from the same builder bean keeps the engine Boot
     * gave it - a global switch would change every client of the host, which a library has no business doing.
     *
     * **Fail fast.** An override key that is no mesh alias, an engine whose library is not on the classpath and
     * the JDK engine on a backend whose `Host` header it cannot send each fail the context start with a message
     * naming the key, checked by the factory (which knows the aliases) rather than at binding.
     *
     * @property default The engine of every backend without an override. [RestClientEngine.APACHE], the one
     *   engine that applies every knob of the [Pool] block.
     * @property endpoints Per-backend-alias overrides; an alias not present runs on [default].
     */
    data class Engine(
        val default: RestClientEngine = RestClientEngine.APACHE,
        val endpoints: Map<String, RestClientEngine> = emptyMap(),
    ) {
        /** The engine of [alias]: its own override when set, otherwise [default]. */
        fun engineFor(alias: String): RestClientEngine = endpoints[alias] ?: default

        /** The property key that selects the engine of [alias] - what a failure message tells the operator to edit. */
        fun keyFor(alias: String): String = EngineSelection.keyFor(PREFIX, alias, alias in endpoints)

        companion object {
            /** The property prefix of this block. */
            const val PREFIX: String = "app.restclient.engine"
        }
    }

    /**
     * Toggle for Spring RestClient's NATIVE Micrometer Observation on the outbound HTTP calls of THIS module.
     * [ResilientRestClientFactory] clones Boot's auto-configured `RestClient.Builder`, on which Boot's
     * `ObservationRestClientCustomizer` has already placed the host's `ObservationRegistry` (and observation
     * convention) - the factory does not wire the registry itself. When `false` this flag REPLACES the registry
     * Boot wired with a NOOP, so this module's clients open no `http.client.requests` observation while every
     * other RestClient of the host keeps its own. When `true`, each request opens that observation; with a
     * tracing bridge on the (host's) classpath that becomes a CLIENT span and injects the W3C `traceparent` into
     * the outgoing request. With only a metrics registry it records the client timer; with no
     * `ObservationRegistry` bean at all Boot wires nothing and it is a NOOP.
     *
     * Unlike the WebClient twin there is no JVM-wide hook to gate: the observation reaches the exchange across
     * the blocking decorator's thread hand-off because
     * [eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator] reopens the caller's observation scope on
     * the worker thread - a per-call, local act, not process-wide state.
     *
     * This is DELIBERATELY self-contained: it resolves from this module's own property and reads no other
     * module's switch - the adapter traces its HTTP calls whatever else is on the host's classpath. The
     * central switch for ALL RestClients of the host is Spring's `management.observations.enable.*` filter,
     * which acts on the registry itself and is orthogonal to this flag.
     *
     * @property httpObservationEnabled Default `false` (opt-in): the module traces its HTTP calls only when the
     *   operator asks for it. Safe to flip on even without a tracer present (NOOP).
     */
    data class Observability(
        val httpObservationEnabled: Boolean = false,
    )
}
