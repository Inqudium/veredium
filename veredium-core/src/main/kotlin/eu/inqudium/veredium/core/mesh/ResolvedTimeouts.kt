package eu.inqudium.veredium.core.mesh

import java.time.Duration

/**
 * The full timeout ladder for one endpoint, derived from the measured anchor plus the margins. The app-side
 * guard lives here: a config that cannot form a valid ladder fails fast at construction rather than silently
 * producing an inert slow-call threshold or an inverted timeout.
 *
 * Besides the ladder it carries the client target fields ([host], [baseUrl], [headers]). The two timeout rungs
 * reach Resilience4j through [eu.inqudium.veredium.core.resilience.MeshResilienceRegistrar], whose customizers
 * ([eu.inqudium.veredium.core.resilience.timeLimiterCustomizer] for [timeLimiterTimeout],
 * [eu.inqudium.veredium.core.resilience.circuitBreakerCustomizer] for [slowCallDuration]) install them; the
 * client factory of each stack reads the rest.
 *
 * ## Worked example - anchor 1s..5s at `maxRetries = 0` (all values in ms, default margins/transport)
 *
 * At `maxRetries = 0` the effective healthy-retry count is `min(0, healthyRetries) = 0`, so the healthy ceiling
 * is a single attempt (`p99 = individual - 200`) and `slowCall = min(p99, 0.8 * global)`. Note that across this
 * whole column `slowCall < individual` (the "inversion") - which is exactly why an enabled CircuitBreaker pins
 * its slow-call rate to 100% here (see [eu.inqudium.veredium.core.resilience.circuitBreakerCustomizer]).
 *
 * |    Sidecar |  Sidecar |     Application | Application | Application |
 * |-----------:|---------:|----------------:|------------:|------------:|
 * | individual |   global | responseTimeout |    slowCall | timeLimiter |
 * |       1000 |     1200 |            1400 |         800 |        1800 |
 * |       2000 |     2200 |            2400 |        1760 |        2800 |
 * |       3000 |     3200 |            3400 |        2560 |        3800 |
 * |       4000 |     4200 |            4400 |        3360 |        4800 |
 * |       5000 |     5200 |            5400 |        4160 |        5800 |
 *
 * global = individual + 200; responseTimeout = global + 200; timeLimiter = responseTimeout + 400 (computed
 * transport margin 100+200+100); slowCall = min(individual - 200, 0.8 * global).
 *
 * @property triplet The canonical endpoint id (`/endpoint/<host>/<METHOD>/<path...>`), the key of
 *   [MeshProperties.resolveAll] and the prefix of every guard message.
 * @property alias The Resilience4j instance name and metric label: the configured alias, or the one derived from
 *   the [triplet].
 * @property host Upstream host from the triplet, sent as the Host header so the sidecar routes to the right API.
 * @property pathTemplate Path template from the triplet (with `{vars}`); the adapter builds its request URI from it.
 * @property baseUrl Client base URL (the sidecar): the per-endpoint override, or the shared
 *   [MeshProperties.Transport.sidecarBaseUrl].
 * @property headers Extra static headers beyond Host (usually empty).
 * @property individualRequestTimeoutMs The MEASURED anchor `p99(single attempt) + margin` - the one rung that is
 *   configured rather than derived.
 * @property maxRetries The sidecar's retry count; with the anchor it derives [sidecarGlobalMs].
 * @property margins The shared [MeshProperties.Defaults.margins].
 * @property transport The app->sidecar transport caps; their computed [MeshProperties.Transport.timeLimiterMargin]
 *   lifts [timeLimiterTimeout] above [responseTimeout].
 * @property timeLimiterEnabled Whether mesh derives + overrides this endpoint's TimeLimiter (the resolved tri-state
 *   of [MeshProperties.BackendToggleOverride]).
 * @property circuitBreakerEnabled Whether mesh derives + overrides this endpoint's CircuitBreaker (the resolved
 *   tri-state of [MeshProperties.BackendToggleOverride]).
 * @property stack The client stack that owns the endpoint, or `null` for the only stack in the context
 *   ([MeshProperties.endpointsOwnedBy]).
 */
data class ResolvedTimeouts(
    val triplet: String,
    val alias: String,
    val host: String,
    val pathTemplate: String,
    val baseUrl: String,
    val headers: Map<String, String>,
    val individualRequestTimeoutMs: Long,
    val maxRetries: Int,
    val margins: MeshProperties.Margins,
    val transport: MeshProperties.Transport,
    val timeLimiterEnabled: Boolean,
    val circuitBreakerEnabled: Boolean,
    val stack: String? = null,
) {
    // The guards run BEFORE the rungs below: Kotlin initialises in declaration order, and a rung computed from an
    // unchecked value can overflow before any guard sees it.
    init {
        // Every message names the YAML key, the endpoint and the offending value: a ladder that cannot form is
        // reported at the first resolveAll(), far from the file the value came from. A margin comes from
        // `app.mesh.defaults.margins`; it is checked per endpoint because its bound can depend on the endpoint's own
        // anchor (p99-margin-ms < individual-request-timeout-ms).
        require(individualRequestTimeoutMs >= 1) {
            "[$triplet] individual-request-timeout-ms must be >= 1, but was $individualRequestTimeoutMs"
        }
        require(maxRetries >= 0) { "[$triplet] max-retries must be >= 0, but was $maxRetries" }
        // Rationale: a negative global margin passes every other guard and pulls the sidecar's `global` below the
        // worst-case sequential run of its own attempts - the sidecar would cut off its last retry - and, far
        // enough, drives the slow-call cap to zero or below, which Resilience4j then rejects in its builder with
        // a message that names neither the key nor the endpoint.
        require(margins.globalMarginMs >= 0) {
            "[$triplet] margins.global-margin-ms must be >= 0, but was ${margins.globalMarginMs} ($MARGIN_SOURCES)"
        }
        // Invariant: responseTimeout > global, stated on the margin that produces it.
        require(margins.responseTimeoutMarginMs >= 1) {
            "[$triplet] margins.response-timeout-margin-ms must be >= 1 (the response timeout must sit above the " +
                "sidecar's global), but was ${margins.responseTimeoutMarginMs} ($MARGIN_SOURCES)"
        }
        require(margins.p99MarginMs in 0 until individualRequestTimeoutMs) {
            "[$triplet] margins.p99-margin-ms must be in 0..<individual-request-timeout-ms " +
                "(so the derived p99 single-attempt latency is >= 1), but was ${margins.p99MarginMs} " +
                "against individual-request-timeout-ms=$individualRequestTimeoutMs ($MARGIN_SOURCES)"
        }
        require(margins.healthyRetries >= 0) {
            "[$triplet] margins.healthy-retries must be >= 0, but was ${margins.healthyRetries} ($MARGIN_SOURCES)"
        }
        require(margins.slowCallBackoffMs in 0..MAX_LADDER_MS) {
            "[$triplet] margins.slow-call-backoff-ms must be in 0..$MAX_LADDER_MS, but was ${margins.slowCallBackoffMs} " +
                "($MARGIN_SOURCES)"
        }
        require(margins.slowCallCapFraction > 0.0 && margins.slowCallCapFraction < 1.0) {
            "[$triplet] margins.slow-call-cap-fraction must be in (0,1) (else the slowCall cap is <= 0 or >= global, " +
                "making detection inert), but was ${margins.slowCallCapFraction} ($MARGIN_SOURCES)"
        }
        // The top of the ladder bounds every rung below it (slowCall < global < responseTimeout < timeLimiter), so
        // one check covers them all. Exact arithmetic, because the inputs alone allow `Int.MAX_VALUE` retries and
        // `Long.MAX_VALUE` ms: plain arithmetic wraps to a negative or a small budget that passes every guard and
        // reaches the transports and Resilience4j as something nobody configured.
        val timeLimiterMs = exactTimeLimiterMs()
        require(timeLimiterMs != null && timeLimiterMs <= MAX_LADDER_MS) {
            "[$triplet] the derived timeLimiter, (1 + max-retries) * individual-request-timeout-ms + " +
                "margins.global-margin-ms + margins.response-timeout-margin-ms + the transport's timeLimiter margin, " +
                "must be <= $MAX_LADDER_MS ms, but was ${timeLimiterMs ?: "more than ${Long.MAX_VALUE}"} " +
                "(max-retries=$maxRetries, individual-request-timeout-ms=$individualRequestTimeoutMs; a unit mix-up?)"
        }
        // Invariant: timeLimiter margin > connect + pendingAcquire - held by construction, see
        // `MeshProperties.Transport.timeLimiterMargin`.
    }

    /**
     * The sidecar's `global` budget, DERIVED from the measured per-endpoint anchor rather than configured: the
     * worst-case sequential run of `1 + maxRetries` attempts plus the folded global margin,
     * `global = (1 + maxRetries) * individual + globalMarginMs`.
     */
    val sidecarGlobalMs: Long = (1L + maxRetries) * individualRequestTimeoutMs + margins.globalMarginMs

    /**
     * The transport's response timeout for the app->sidecar hop, one rung above the sidecar's `global`, applied by
     * whichever engine carries the call - on the default engines as Apache's `RequestConfig.responseTimeout` and
     * Reactor Netty's `responseTimeout`; how the other engines map it is in `docs/engine-behaviour.md` (ADR-0004).
     * Same rung, same number, whichever stack and engine the host runs on.
     */
    val responseTimeout: Duration = Duration.ofMillis(sidecarGlobalMs + margins.responseTimeoutMarginMs)

    /**
     * The p99 latency of a single healthy attempt. Not a separate input: the anchor is `p99 + margin`, so p99 is
     * recovered from it (and thus tracks the anchor automatically).
     */
    val p99SingleMs: Long = individualRequestTimeoutMs - margins.p99MarginMs

    /**
     * The circuit breaker's slow-call threshold, `slowCall = min(healthy ceiling, global * slowCallCapFraction)`.
     * The healthy ceiling is the latency of a healthy call incl. a couple of routine retries,
     * `p99 * (1 + eff) + eff * slowCallBackoffMs` with `eff = min(maxRetries, healthyRetries)`; the cap bounds it
     * safely below global.
     */
    val slowCallDuration: Duration =
        run {
            // healthyRetries can never exceed the actual retry budget: capping it at maxRetries means with 0
            // retries none fold in (ceiling = a single attempt p99), and it never assumes more retries than
            // the endpoint permits - more robust than an explicit maxRetries >= 1 guard.
            val effectiveHealthyRetries = minOf(maxRetries, margins.healthyRetries)
            val healthyCeiling = p99SingleMs * (1L + effectiveHealthyRetries) + effectiveHealthyRetries * margins.slowCallBackoffMs
            val cap = (sidecarGlobalMs * margins.slowCallCapFraction).toLong()
            Duration.ofMillis(minOf(healthyCeiling, cap))
        }

    /**
     * The "inversion": the derived slow-call sits BELOW the sidecar's per-attempt timeout. An enabled circuit
     * breaker then pins its slow-call rate to 100 % (see [eu.inqudium.veredium.core.resilience.circuitBreakerCustomizer]) - the derived duration is only
     * safe against a healthy-but-slow endpoint when the rate demands total degradation before opening.
     */
    val slowCallInverted: Boolean = slowCallDuration < Duration.ofMillis(individualRequestTimeoutMs)

    /**
     * The TimeLimiter budget, `timeLimiter = responseTimeout + transport.timeLimiterMargin` - the margin
     * ([MeshProperties.Transport.timeLimiterMargin]) covers the pre-response phases the response timeout does not.
     */
    val timeLimiterTimeout: Duration = responseTimeout.plusMillis(transport.timeLimiterMargin)

    /** The top rung, [timeLimiterTimeout], in ms - or `null` where it does not fit a `Long`. */
    private fun exactTimeLimiterMs(): Long? =
        try {
            var ms = Math.multiplyExact(1L + maxRetries, individualRequestTimeoutMs)
            ms = Math.addExact(ms, margins.globalMarginMs)
            ms = Math.addExact(ms, margins.responseTimeoutMarginMs)
            Math.addExact(ms, transport.timeLimiterMargin)
        } catch (_: ArithmeticException) {
            null
        }

    companion object {
        /**
         * The largest rung the ladder may derive, in ms - the bound of the transport timeouts
         * ([MeshProperties.Transport.MAX_TRANSPORT_TIMEOUT_MS], about 24.8 days). No endpoint waits that long on
         * purpose; past it the value is a unit mix-up, and below it every rung stays representable wherever it is
         * applied, in nanoseconds included.
         */
        const val MAX_LADDER_MS: Long = MeshProperties.Transport.MAX_TRANSPORT_TIMEOUT_MS
    }
}

/** Where a margin of [ResolvedTimeouts] comes from - named in every margin guard's message. */
private const val MARGIN_SOURCES: String = "app.mesh.defaults.margins"
