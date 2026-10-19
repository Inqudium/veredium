package eu.inqudium.veredium.core.mesh

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Binds the app-side mesh config (`app.mesh`) that Helm renders from the single `meshEndpoints` source. The
 * triplet is the canonical id (the sidecar key without the surrounding []), carried as a list entry because it
 * is unwieldy as a property key. Ported from the PoC (`helm-mesh-timeouts/kotlin`, package
 * `eu.mesh.timeouts.poc`) and extended with the per-endpoint client target fields (`baseUrl` / `headers`; the
 * Host header is derived from the triplet). This is the single per-endpoint tree, feeding [MeshTimeoutConfig]
 * and [ResolvedTimeouts], and it is the same tree for both client stacks: the blocking `veredium-restclient`
 * and the reactive `veredium-webclient` bind it from here, so one deployment's `app.mesh` block reads
 * identically whichever stack the host runs on.
 *
 * ## Configurable properties (all optional except the endpoint `triplet`)
 *
 * Keys are kebab-case in YAML; every value shown is the Kotlin default. The whole `defaults` block may be
 * omitted - each endpoint then falls back to these values.
 *
 * **`app.mesh.defaults`** - shared fallbacks + policy knobs:
 * - `individual-request-timeout-ms` (Long, `4000`, mirrors the chart's `mesh.defaultIndividualMs` fallback) -
 *   fallback MEASURED anchor `p99(single attempt) + margin`;
 *   normally each endpoint sets its own, so this is only used where none is set.
 * - `max-retries` (Int, `0`) - mirrors the sidecar's max-retries; with the anchor it derives `global`.
 *   `0` = no retries (matches the production sidecar helm-lap `max-retries: 0`).
 * - `time-limiter.enabled` (Boolean, `true`) - whether mesh derives + overrides each endpoint's TimeLimiter.
 * - `circuit-breaker.enabled` (Boolean, `false`) - whether mesh derives + overrides each endpoint's CircuitBreaker.
 * - `margins` - see below.
 * - `transport` - see below.
 *
 * **`app.mesh.endpoints[]`** - one entry per backend (the endpoint tree; see [EndpointConfig]):
 * - `triplet` (String, REQUIRED) - canonical id `/endpoint/<host>/<METHOD>/<path...>`; drives both the alias
 *   and the client target (host, path template).
 * - `alias` (String?, `null`) - R4j instance / metric label; when null it is DERIVED from the triplet
 *   (`host-METHOD-static-path`, e.g. `api-example-com-GET-accounts`). Aliases must be unique (enforced in `init`).
 * - `individual-request-timeout-ms` (Long?, `null` -> `defaults`) - per-endpoint measured anchor.
 * - `max-retries` (Int?, `null` -> `defaults`).
 * - `base-url` (String?, `null` -> `transport.sidecar-base-url`) - per-endpoint client base URL.
 * - `headers` (Map<String,String>, empty) - extra static headers (the Host header is derived from the triplet).
 * - `time-limiter.enabled` / `circuit-breaker.enabled` (Boolean?, `null` -> inherit the `defaults` toggle;
 *   tri-state: an explicit `true`/`false` overrides the default). See [BackendToggle] for what `enabled` means -
 *   `false` does NOT disable the backend, it just leaves its config untouched.
 * - `margins` - a PARTIAL override, field-merged onto `defaults.margins` (only the fields you set change).
 *
 * **`margins`** (under `defaults` and, partially, per endpoint - shapes the derived ladder; see [Margins]):
 * - `global-margin-ms` (Long, `200`) - buffer folded into `global = (1 + maxRetries)*individual + globalMargin`;
 *   the ONE margin the sidecar also reads.
 * - `response-timeout-margin-ms` (Long, `200`) - `responseTimeout = global + responseTimeoutMargin`: the
 *   transport's response timeout of the app->sidecar hop, one rung above the sidecar's `global`. The name is
 *   stack-neutral on purpose - it is the same rung whether Apache HttpClient or Reactor Netty applies it.
 * - `p99-margin-ms` (Long, `200`) - `p99 = individual - p99Margin` (recovered from the anchor).
 * - `healthy-retries` (Int, `1`) - routine retries still counting as "healthy" (policy; capped at maxRetries).
 * - `slow-call-backoff-ms` (Long, `200`) - per-attempt backoff feeding the healthy ceiling.
 * - `slow-call-cap-fraction` (Double, `0.8`) - hard cap: `slowCall` never exceeds `global * this`.
 * - NOTE: the TimeLimiter margin is NOT configurable here - it is a computed property of `transport`.
 *
 * **`transport`** (under `defaults` - the app->sidecar pre-response phases; see [Transport]):
 * - `sidecar-base-url` (String, `http://localhost:8042`).
 * - `connect-timeout-ms` (Long, `100`) - small because app->sidecar is localhost + plaintext.
 * - `pending-acquire-timeout-ms` (Long, `200`) - fail fast on pool exhaustion.
 * - `max-connections` (Int, `100`).
 * - NOTE: `timeLimiterMargin` is COMPUTED = `connect + pendingAcquire + 100` headroom (not configurable).
 *
 * ## Example
 * ```yaml
 * app:
 *   mesh:
 *     defaults:                              # optional - omit the whole block to accept the defaults above
 *       max-retries: 3
 *       circuit-breaker:
 *         enabled: false                     # the common case: TimeLimiter on, CircuitBreaker off
 *       margins:
 *         slow-call-cap-fraction: 0.8        # each field optional, field-merged onto the Kotlin defaults
 *       transport:
 *         connect-timeout-ms: 100
 *     endpoints:
 *       - triplet: "/endpoint/api.example.com/GET/v5/access-profiles/{id}"
 *         individual-request-timeout-ms: 300 # alias derived -> api-example-com-GET-v5-access-profiles
 *       - triplet: "/endpoint/apis.example.com/POST/v5/involved-parties/lookup"
 *         individual-request-timeout-ms: 500
 *         max-retries: 2
 *         circuit-breaker:
 *           enabled: true                    # opt THIS endpoint's CircuitBreaker in (overrides the default off)
 *         margins:
 *           slow-call-cap-fraction: 0.7      # partial override; the rest stay at the shared defaults
 * ```
 */
@ConfigurationProperties(prefix = MeshProperties.PREFIX)
data class MeshProperties(
    val defaults: Defaults = Defaults(),
    val endpoints: List<EndpointConfig> = emptyList(),
) {
    init {
        // Aliases (derived or configured) become R4j instance names / metric labels and must be unique.
        val aliases = endpoints.map { it.effectiveAlias() }
        require(aliases.size == aliases.toSet().size) { "Mesh aliases collide: ${duplicatesIn(aliases)}" }
        // The triplet is the canonical id AND the key resolveAll() maps by, where `associate` keeps the LAST
        // entry for a repeated key - so a duplicate silently drops an endpoint instead of failing. The alias
        // check above does not cover this: two entries can carry the same triplet and still pass it, each
        // with its own explicit alias. Without this guard the loss surfaces only at the first call to the
        // dropped backend, as the "No mesh endpoint configured" error the fail-fast is meant to prevent.
        val triplets = endpoints.map { it.triplet }
        require(triplets.size == triplets.toSet().size) { "Mesh triplets collide: ${duplicatesIn(triplets)}" }
    }

    data class Defaults(
        val individualRequestTimeoutMs: Long = 4000,
        val maxRetries: Int = 0,
        val margins: Margins = Margins(),
        val transport: Transport = Transport(),
        val timeLimiter: BackendToggle = BackendToggle(enabled = true),
        val circuitBreaker: BackendToggle = BackendToggle(enabled = false),
    )

    data class EndpointConfig(
        val triplet: String,
        val alias: String? = null,
        val individualRequestTimeoutMs: Long? = null,
        val maxRetries: Int? = null,
        val baseUrl: String? = null,
        val headers: Map<String, String> = emptyMap(),
        val margins: MarginsOverride? = null,
        val timeLimiter: BackendToggleOverride? = null,
        val circuitBreaker: BackendToggleOverride? = null,
    ) {
        /** Configured alias, or a deterministic one derived from the triplet. */
        fun effectiveAlias(): String = alias ?: aliasFromTriplet()

        /** Upstream host from the triplet (`/endpoint/<host>/<METHOD>/...`), sent as the Host header. */
        fun host(): String = tripletParts()[1]

        /** Path template from the triplet (`/endpoint/<host>/<METHOD>/<path...>` -> `/<path...>`), keeps {vars}. */
        fun pathTemplate(): String = "/" + tripletParts().drop(3).joinToString("/")

        /**
         * Split + validate the triplet into its `[endpoint, host, METHOD, seg, ...]` parts.
         *
         * At least FOUR: the path needs a segment of its own. Three parts pass every other check and produce
         * a bare `"/"` - a path template that matches any URI as a metric prefix, an alias that falls back to
         * `root`, and a client that calls the sidecar's root. All three are plausible-looking consequences of
         * a typo, and the first sign of it would be a 404 at runtime pointing nowhere near the configuration.
         */
        private fun tripletParts(): List<String> {
            val parts = triplet.trim('/').split('/')
            require(parts.size >= 4 && parts[0] == "endpoint") {
                "Not a valid endpoint triplet: $triplet - expected /endpoint/<host>/<METHOD>/<path...>"
            }
            return parts
        }

        /**
         * Derive a slash-free R4j instance / metric label from a triplet, in triplet order:
         * "/endpoint/api.example.com/GET/accounts/{accountId}" -> "api-example-com-GET-accounts" (host, method, static
         * segments; {placeholders} dropped). The host prefix avoids collisions across hosts. Keep this STABLE -
         * the alias is a metric label, so changing it renames every derived label.
         */
        private fun aliasFromTriplet(): String {
            val parts = tripletParts()
            // R4j names/tags are case-SENSITIVE, so normalise each part canonically: host -> lowercase (DNS
            // RFC 4343), method -> UPPERCASE, path segments case-sensitive (RFC 3986) so left as-is.
            val host = parts[1].lowercase().replace('.', '-')
            val method = parts[2].uppercase()
            val staticSegments = parts.drop(3).filterNot { it.startsWith("{") } // drop path params
            val path = staticSegments.joinToString("-").ifBlank { "root" }
            return "$host-$method-$path"
        }
    }

    data class Margins(
        val globalMarginMs: Long = DEFAULT_GLOBAL_MARGIN_MS,
        /** The response-timeout margin, bound from `response-timeout-margin-ms` (class KDoc). */
        val responseTimeoutMarginMs: Long = DEFAULT_RESPONSE_TIMEOUT_MARGIN_MS,
        val p99MarginMs: Long = DEFAULT_P99_MARGIN_MS,
        val healthyRetries: Int = DEFAULT_HEALTHY_RETRIES,
        val slowCallBackoffMs: Long = DEFAULT_SLOW_CALL_BACKOFF_MS,
        val slowCallCapFraction: Double = DEFAULT_SLOW_CALL_CAP_FRACTION,
    ) {
        /** The default margin values, exposed as named constants so callers and tests share one source of truth. */
        companion object {
            const val DEFAULT_GLOBAL_MARGIN_MS: Long = 200
            const val DEFAULT_RESPONSE_TIMEOUT_MARGIN_MS: Long = 200
            const val DEFAULT_P99_MARGIN_MS: Long = 200
            const val DEFAULT_HEALTHY_RETRIES: Int = 1
            const val DEFAULT_SLOW_CALL_BACKOFF_MS: Long = 200
            const val DEFAULT_SLOW_CALL_CAP_FRACTION: Double = 0.8
        }

        /** Field-wise merge: each field from the override when present, otherwise this (the shared default). */
        fun overriddenBy(o: MarginsOverride?): Margins =
            if (o == null) {
                this
            } else {
                copy(
                    globalMarginMs = o.globalMarginMs ?: globalMarginMs,
                    responseTimeoutMarginMs = o.responseTimeoutMarginMs ?: responseTimeoutMarginMs,
                    p99MarginMs = o.p99MarginMs ?: p99MarginMs,
                    healthyRetries = o.healthyRetries ?: healthyRetries,
                    slowCallBackoffMs = o.slowCallBackoffMs ?: slowCallBackoffMs,
                    slowCallCapFraction = o.slowCallCapFraction ?: slowCallCapFraction,
                )
            }
    }

    /**
     * A per-endpoint override of [Margins], every field nullable so "not set" differs from "set to the
     * default". resolveAll field-merges it onto defaults.margins, so a partial override touches only its
     * own fields.
     */
    data class MarginsOverride(
        val globalMarginMs: Long? = null,
        val responseTimeoutMarginMs: Long? = null,
        val p99MarginMs: Long? = null,
        val healthyRetries: Int? = null,
        val slowCallBackoffMs: Long? = null,
        val slowCallCapFraction: Double? = null,
    )

    /**
     * One per-endpoint feature switch, defaulted under `defaults` and overridable per endpoint. Each feature is
     * switched SEPARATELY; what `enabled` MEANS is defined by the feature, not by this type:
     *
     * - **`timeLimiter` / `circuitBreaker`** (default on / off): whether mesh AUTO-DERIVES and overrides that
     *   Resilience4j backend's timeout knob for the endpoint. `false` means mesh does NOT touch that backend's
     *   config - it does NOT remove or disable the backend itself. A disabled endpoint keeps whatever config it
     *   already has (per-backend YAML, a named `configs:` block, or the registry default), and the client
     *   factory still resolves an R4j instance by alias exactly as before. The CircuitBreaker toggle governs the
     *   derived `slowCallDurationThreshold` AND its conditional 100% rate pin as ONE unit - the split is along
     *   backend boundaries, never inside a backend (see `circuitBreakerCustomizer` in the `resilience` package),
     *   so mesh can never leave the CB in the incoherent "duration derived, rate fixed" state.
     *
     * Response validation is NOT a mesh feature: it is not derived from the timeout ladder and is switched in
     * the client stack's own properties (`app.restclient.validation` / `app.webclient.validation`) instead.
     */
    data class BackendToggle(
        val enabled: Boolean = true,
    )

    /**
     * Per-endpoint tri-state override of a [BackendToggle]: `null` inherits the [Defaults] toggle, an explicit
     * `true` / `false` overrides it. Modelled as a nullable [enabled] (never a non-null `false` default) so
     * "unset" stays distinct from "deliberately off".
     */
    data class BackendToggleOverride(
        val enabled: Boolean? = null,
    )

    /**
     * Transport-level caps for the app->sidecar hop - the client's pre-response phases that sit OUTSIDE the
     * responseTimeout window, so only the TimeLimiter covers them (hence the computed [timeLimiterMargin] must
     * exceed their worst case). The client factory of each stack reads them directly: `connectTimeoutMs` drives
     * the connect timeout, `pendingAcquireTimeoutMs` / `maxConnections` the shared pool.
     */
    data class Transport(
        val sidecarBaseUrl: String = "http://localhost:8042",
        val connectTimeoutMs: Long = DEFAULT_CONNECT_TIMEOUT_MS,
        val pendingAcquireTimeoutMs: Long = DEFAULT_PENDING_ACQUIRE_TIMEOUT_MS,
        val maxConnections: Int = 100,
    ) {
        init {
            // A wrong value here does not announce itself. Apache's pool builder IGNORES a max-connections of
            // zero or less (`if (maxConnTotal > 0)`) and silently runs on its own default of 25 total - a pool
            // that fills long before the four bulkheads (4 x 25) ever reject anything. A zero or negative
            // timeout means "unbounded" to the transport, which makes the computed timeLimiterMargin - the
            // worst case of exactly these two phases - a lie. Fail at bind time, naming the key.
            require(maxConnections >= 1) {
                "app.mesh.defaults.transport.max-connections must be at least 1, but was $maxConnections"
            }
            require(connectTimeoutMs >= 1) {
                "app.mesh.defaults.transport.connect-timeout-ms must be at least 1, but was $connectTimeoutMs"
            }
            require(pendingAcquireTimeoutMs >= 1) {
                "app.mesh.defaults.transport.pending-acquire-timeout-ms must be at least 1, but was $pendingAcquireTimeoutMs"
            }
        }

        /**
         * By how much the TimeLimiter must overshoot the transport responseTimeout so it also covers the
         * pre-response phases. This is a COMPUTED value, not a configurable margin: it is exactly the worst-case
         * pre-phases (connect + pendingAcquire) plus a fixed headroom, so it can never be set below the ladder
         * invariant. Derived from THIS instance's caps, so a custom transport carries a correspondingly larger margin.
         */
        val timeLimiterMargin: Long get() = connectTimeoutMs + pendingAcquireTimeoutMs + TIME_LIMITER_HEADROOM_MS

        companion object {
            const val DEFAULT_CONNECT_TIMEOUT_MS: Long = 100
            const val DEFAULT_PENDING_ACQUIRE_TIMEOUT_MS: Long = 200

            /** Fixed headroom the computed timeLimiterMargin adds on top of the worst-case pre-response phases. */
            const val TIME_LIMITER_HEADROOM_MS: Long = 100
        }
    }

    companion object {
        /** The property prefix this tree binds from - the one string the registrar and the binding share. */
        const val PREFIX = "app.mesh"
    }

    /** The values occurring more than once - named in the collision messages so the offender is obvious. */
    private fun duplicatesIn(values: List<String>): Set<String> =
        values
            .groupingBy { it }
            .eachCount()
            .filterValues { it > 1 }
            .keys

    /**
     * Every endpoint resolved to its ladder, keyed by the canonical triplet (unique per the `init` guard).
     *
     * Derived ONCE and cached: three consumers ask for it (the client factory, the ladder logger and the R4j
     * registrar), and recomputing per call handed each of them its own [ResolvedTimeouts] instances for the same
     * configuration. Cheap either way at four endpoints - the point is that all consumers now reason about one
     * set of objects. Lazy rather than eager so binding a `MeshProperties` stays free of the derivation, and its
     * `require` diagnostics keep surfacing at the first use rather than deep inside Spring's binder.
     */
    private val resolved: Map<String, ResolvedTimeouts> by lazy { resolveEndpoints() }

    /** Resolve every endpoint to its ladder, keyed by the canonical triplet (unique per the `init` guard). */
    fun resolveAll(): Map<String, ResolvedTimeouts> = resolved

    private fun resolveEndpoints(): Map<String, ResolvedTimeouts> =
        endpoints.associate { e ->
            e.triplet to
                ResolvedTimeouts(
                    triplet = e.triplet,
                    // alias is generated from the triplet unless explicitly overridden
                    alias = e.effectiveAlias(),
                    host = e.host(),
                    pathTemplate = e.pathTemplate(),
                    baseUrl = e.baseUrl ?: defaults.transport.sidecarBaseUrl,
                    headers = e.headers,
                    individualRequestTimeoutMs = e.individualRequestTimeoutMs ?: defaults.individualRequestTimeoutMs,
                    maxRetries = e.maxRetries ?: defaults.maxRetries,
                    margins = defaults.margins.overriddenBy(e.margins),
                    transport = defaults.transport,
                    // tri-state: a per-endpoint flag wins; null inherits the shared default.
                    timeLimiterEnabled = e.timeLimiter?.enabled ?: defaults.timeLimiter.enabled,
                    circuitBreakerEnabled = e.circuitBreaker?.enabled ?: defaults.circuitBreaker.enabled,
                )
        }
}
