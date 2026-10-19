package eu.inqudium.veredium.webclient

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.util.unit.DataSize
import java.time.Duration

@Configuration
@EnableConfigurationProperties(WebClientProperties::class)
class WebClientConfiguration

/**
 * GLOBAL configuration for the WebClients produced by [ResilientWebClientFactory] - everything NOT
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
@ConfigurationProperties(prefix = "app.webclient")
data class WebClientProperties(
    /**
     * Upper bound for buffered response bodies. Guards against unbounded memory use on large payloads.
     *
     * **Why 256 KB and not less.** This is Spring's own `ExchangeStrategies` default, so it is the value a
     * reader already expects; a deviation is what needs justifying, not this. The previous default was 8 KB,
     * and on 2026-08-07 one access-profile response exceeded it: the backend answered `200 OK` in 130-260 ms
     * and the codec aborted mid-buffer, ten times over five cycles on the same involved party. Two backfill
     * rows spent their whole retry cap on it and were rejected. 8 KB is simply too small for a list response
     * from these backends.
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
     * with per-endpoint (backend-alias) overrides - see [Validation].
     */
    val validation: Validation = Validation(),
) {
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
            // intended: a connection that is due for eviction the moment it is created. Zero is left alone -
            // for evictInBackground it is Reactor Netty's documented "off".
            require(!maxIdleTime.isNegative) { "app.webclient.pool.max-idle-time must not be negative, but was $maxIdleTime" }
            require(!maxLifeTime.isNegative) { "app.webclient.pool.max-life-time must not be negative, but was $maxLifeTime" }
            require(!evictInBackground.isNegative) {
                "app.webclient.pool.evict-in-background must not be negative, but was $evictInBackground"
            }
        }
    }

    /**
     * Toggle for Spring WebClient's NATIVE Micrometer Observation on the outbound HTTP calls of THIS module.
     * [ResilientWebClientFactory] clones Boot's auto-configured `WebClient.Builder`, on which Boot's
     * `ObservationWebClientCustomizer` has already placed the host's `ObservationRegistry` (and observation
     * convention) - the factory does not wire the registry itself. This flag has two jobs: it gates the JVM-wide
     * Reactor context-propagation hook (`WebClientReactorContextPropagationConfig`), and when `false` it
     * REPLACES the registry Boot wired with a NOOP, so this module's clients open no `http.client.requests`
     * observation while every other WebClient of the host keeps its own. When `true`, each request opens that
     * observation; with a tracing bridge on the (host's) classpath that becomes a CLIENT span and injects the
     * W3C `traceparent` into the outgoing request. With only a metrics registry it records the client timer;
     * with no `ObservationRegistry` bean at all Boot wires nothing and it is a NOOP.
     *
     * This is DELIBERATELY self-contained: it resolves from this module's own property and references NOTHING
     * from sync-bridge - the adapter traces its HTTP calls whether or not sync-bridge is on the classpath. The
     * central switch for ALL WebClients of the host is Spring's `management.observations.enable.*` filter,
     * which acts on the registry itself and is orthogonal to this flag.
     *
     * @property httpObservationEnabled Default `false` (opt-in): the module traces its HTTP calls only when the
     *   operator asks for it. Safe to flip on even without a tracer present (NOOP).
     */
    data class Observability(
        val httpObservationEnabled: Boolean = false,
    )

    /**
     * Governs whether a backend's deserialised response DTO is checked against its Bean Validation constraints.
     *
     * A shared [enabled] default plus per-backend overrides in [endpoints] (keyed by the backend ALIAS, i.e. the
     * mesh-derived R4j instance name): a per-alias entry wins in BOTH directions - it can switch a single backend
     * on above an off default, or off above an on default. This granularity is load-bearing, not cosmetic: the
     * constraints are generated from the published OpenAPI specs, and those specs are demonstrably stricter than
     * reality in places, differently per endpoint. `ProductAgreementsDataResponseV8DTO` declares `@Size(min = 1)`
     * on its result list, so validating that endpoint rejects the empty result a lookup without matches
     * legitimately returns; `AccessProfileAdapter` documents a second, unrelated spec defect (incident
     * INC15288523). Validation is therefore adopted one backend at a time, against real traffic - which is exactly
     * what the per-alias override is for.
     *
     * The check itself lives OUTSIDE the Resilience4j decoration (see [ResilientWebClient]): a response that
     * violates its own schema is a contract problem, not an outage, and must not open the circuit breaker.
     *
     * @property enabled Default `false`: the constraints promise a guarantee the specs do not keep everywhere, so
     *   the safe default is off and each backend opts in deliberately.
     * @property endpoints Per-backend-alias overrides; an alias not present inherits [enabled].
     * @property failOnViolation Whether a detected violation FAILS the call (a [jakarta.validation
     *   .ConstraintViolationException]) or is only REPORTED (logged at WARN, the response returned unchanged).
     *   Default `true` (enforce). SHARED only, deliberately not per-endpoint: report-only is a rollout stance -
     *   turn validation on for a backend and watch the WARN lines against real traffic before enforcing - and
     *   that stance applies to the whole adopting wave, not to one backend at a time. The per-alias granularity
     *   that matters is WHICH backends validate ([endpoints]); WHETHER a violation is fatal is a single posture.
     */
    data class Validation(
        val enabled: Boolean = false,
        val endpoints: Map<String, Boolean> = emptyMap(),
        val failOnViolation: Boolean = true,
    ) {
        /** The effective toggle for [alias]: its own override when set, otherwise the shared [enabled] default. */
        fun isEnabledFor(alias: String): Boolean = endpoints[alias] ?: enabled
    }
}
