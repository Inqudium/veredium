package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.validation.ResponseValidationProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.util.unit.DataSize
import java.time.Duration

/**
 * GLOBAL configuration for the RestClients produced by [ResilientRestClientFactory] - everything NOT
 * per-endpoint. The per-endpoint config (base URL, Host, timeout ladder) lives in `app.mesh.endpoints[]`,
 * so one factory serves several backends from that tree with one shared connection [pool].
 *
 * Only knobs with no mesh equivalent stay here: `maxInMemorySize` and the pool lifecycle. The mesh transport
 * config owns `maxConnections` / `pendingAcquireTimeout`, the ladder owns the response timeout.
 *
 * Every value is checked while BINDING, the way `MeshProperties` and `ResolvedTimeouts` do it - and for the
 * same reason: a wrong number here does not announce itself. It binds, the context starts, and the effect
 * shows up as a runtime symptom that points anywhere but at the configuration. The messages therefore name
 * the PROPERTY KEY, not the Kotlin field, because the key is what the person reading the failure has to edit.
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
     * the same reason (see `RestClientVerdicts.clientSideFailure`).
     *
     * **Why 256 KB and not less.** This is the WebClient twin's ceiling, kept so that the two modules size their
     * buffers identically against the same backends: on 2026-08-07 one access-profile response exceeded the 8 KB
     * the reactive module shipped with, and two backfill rows spent their whole retry cap on it. 8 KB is simply
     * too small for a list response from these backends.
     *
     * **Why not more.** The limit applies PER CONCURRENTLY BUFFERED RESPONSE, and the shared pool is bounded
     * at `maxConnections` (100, `mesh.MeshProperties.Transport`). At 256 KB the worst case is ~25 MB of
     * response buffers; at the 1 MB some other clients use it would be ~100 MB, which is real money on a
     * service that otherwise needs little heap.
     *
     * **Why a limit at all** - see the range check in `init`: without one, a single degenerate response takes
     * the pod down, and the failure mode of getting it wrong is documented there.
     */
    val maxInMemorySize: DataSize = DataSize.ofKilobytes(256),
    /**
     * One shared connection pool for ALL endpoints. Every endpoint connects to the same local sidecar over
     * loopback, so per-endpoint pool isolation is not worth the lost connection reuse; per-backend isolation
     * is owned by the R4j bulkhead / circuit breaker / time limiter instead. `maxConnections` and
     * `pendingAcquireTimeout` come from the mesh transport config.
     */
    val pool: Pool = Pool(),
    /** HTTP client Micrometer Observation - standalone, self-contained (does NOT depend on sync-bridge). */
    val observability: Observability = Observability(),
    /**
     * Whether a deserialised response is checked against its DTO's Bean Validation constraints. A global default
     * with per-endpoint (backend-alias) overrides - the shared [ResponseValidationProperties] of `veredium-core`.
     */
    val validation: ResponseValidationProperties = ResponseValidationProperties(),
) {
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
     * The lifecycle knobs of the shared Apache HttpClient 5 connection pool.
     *
     * The WebClient twin's `pending-acquire-max-count` has no counterpart here, deliberately: Apache's pool has
     * no bounded pending queue. A caller waiting for a connection is bounded in TIME only, by the mesh transport's
     * `pending-acquire-timeout-ms` (the connection-request timeout), which is also how the bulkheads are sized -
     * they shed load before the pool queue builds up at all.
     */
    data class Pool(
        /**
         * Evict a connection that sat idle in the pool for longer than this. Enforced by the factory's eviction
         * schedule (`closeIdle`), every [evictInBackground].
         */
        val maxIdleTime: Duration = Duration.ofSeconds(30),
        /**
         * Evict a connection older than this regardless of use - the pool's connection time-to-live. Zero
         * means no lifetime bound (Apache's default).
         */
        val maxLifeTime: Duration = Duration.ofMinutes(5),
        /**
         * How often the factory's eviction schedule runs `closeIdle` / `closeExpired` on the pool. Zero
         * switches background eviction off, the same reading Reactor Netty gives the value.
         */
        val evictInBackground: Duration = Duration.ofSeconds(30),
    ) {
        init {
            // Negative durations are accepted by Apache's builders and then quietly mean something no operator
            // intended: a connection that is due for eviction the moment it is created. Zero is left alone -
            // for evictInBackground it is "off", for maxLifeTime it is "unbounded".
            require(!maxIdleTime.isNegative) { "app.restclient.pool.max-idle-time must not be negative, but was $maxIdleTime" }
            require(!maxLifeTime.isNegative) { "app.restclient.pool.max-life-time must not be negative, but was $maxLifeTime" }
            require(!evictInBackground.isNegative) {
                "app.restclient.pool.evict-in-background must not be negative, but was $evictInBackground"
            }
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
     * the blocking decorator's thread hand-off because `BlockingResilienceDecorator` reopens the caller's
     * observation scope on the worker thread - a per-call, local act, not process-wide state.
     *
     * This is DELIBERATELY self-contained: it resolves from this module's own property and references NOTHING
     * from sync-bridge - the adapter traces its HTTP calls whether or not sync-bridge is on the classpath. The
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
