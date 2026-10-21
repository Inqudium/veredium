package eu.inqudium.veredium.core.mesh

import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import java.time.Duration

/**
 * The full timeout ladder for one endpoint, derived from the measured anchor plus the margins. The app-side
 * guard lives here: a config that cannot form a valid ladder fails fast at construction rather than silently
 * producing an inert slow-call threshold or an inverted timeout.
 *
 * Ported from the PoC (`eu.mesh.timeouts.poc`) and extended with the client target fields ([host], [baseUrl],
 * [headers]). [MeshTimeoutConfig] feeds the two timeout rungs into R4j ([timeLimiterTimeout],
 * [slowCallDuration]); the client factory of each stack reads the rest.
 *
 * ## Worked example - anchor 1s..5s at `maxRetries = 0` (all values in ms, default margins/transport)
 *
 * At `maxRetries = 0` the effective healthy-retry count is `min(0, healthyRetries) = 0`, so the healthy ceiling
 * is a single attempt (`p99 = individual - 200`) and `slowCall = min(p99, 0.8 * global)`. Note that across this
 * whole column `slowCall < individual` (the "inversion") - which is exactly why an enabled CircuitBreaker pins
 * its slow-call rate to 100% here (see `circuitBreakerCustomizer` in the `resilience` package).
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
 */
data class ResolvedTimeouts(
    val triplet: String,
    val alias: String,
    /** Upstream host from the triplet, sent as the Host header so the sidecar routes to the right API. */
    val host: String,
    /** Path template from the triplet (with `{vars}`); the adapter builds its request URI from it. */
    val pathTemplate: String,
    /** Client base URL (the sidecar): the per-endpoint override, or the shared `Transport.sidecarBaseUrl`. */
    val baseUrl: String,
    /** Extra static headers beyond Host (usually empty). */
    val headers: Map<String, String>,
    val individualRequestTimeoutMs: Long,
    val maxRetries: Int,
    val margins: MeshProperties.Margins,
    val transport: MeshProperties.Transport,
    /** Whether mesh derives + overrides this endpoint's TimeLimiter (resolved tri-state; see MeshProperties). */
    val timeLimiterEnabled: Boolean,
    /** Whether mesh derives + overrides this endpoint's CircuitBreaker (resolved tri-state; see MeshProperties). */
    val circuitBreakerEnabled: Boolean,
) {
    // global is DERIVED from the measured per-endpoint anchor (see the pattern doc, 1.1.1), not an input:
    // the worst-case sequential run of (1 + maxRetries) attempts plus the folded global margin.
    val sidecarGlobalMs: Long = (1 + maxRetries) * individualRequestTimeoutMs + margins.globalMarginMs

    /**
     * The transport's response timeout for the app->sidecar hop, one rung above the sidecar's `global`: the
     * blocking stack applies it as Apache's `RequestConfig.responseTimeout`, the reactive stack as Reactor
     * Netty's `responseTimeout`. Same rung, same number, whichever stack the host runs on.
     */
    val responseTimeout: Duration = Duration.ofMillis(sidecarGlobalMs + margins.responseTimeoutMarginMs)

    // p99 of a single healthy attempt is not a separate input: the anchor is p99 + margin, so p99 is recovered
    // from it (and thus tracks the anchor automatically).
    val p99SingleMs: Long = individualRequestTimeoutMs - margins.p99MarginMs

    // slowCall = min(healthy ceiling, global * cap) - the doc's 1.1.2 derivation. The healthy ceiling is the
    // latency of a healthy call incl. a couple of routine retries; the cap bounds it safely below global.
    val slowCallDuration: Duration =
        run {
            // healthyRetries can never exceed the actual retry budget: capping it at maxRetries means with 0
            // retries none fold in (ceiling = a single attempt p99), and it never assumes more retries than
            // the endpoint permits - more robust than an explicit maxRetries >= 1 guard.
            val effectiveHealthyRetries = minOf(maxRetries, margins.healthyRetries)
            val healthyCeiling = p99SingleMs * (1 + effectiveHealthyRetries) + effectiveHealthyRetries * margins.slowCallBackoffMs
            val cap = (sidecarGlobalMs * margins.slowCallCapFraction).toLong()
            Duration.ofMillis(minOf(healthyCeiling, cap))
        }

    /**
     * The "inversion": the derived slow-call sits BELOW the sidecar's per-attempt timeout. An enabled circuit
     * breaker then pins its slow-call rate to 100 % (see `circuitBreakerCustomizer`) - the derived duration is only
     * safe against a healthy-but-slow endpoint when the rate demands total degradation before opening.
     */
    val slowCallInverted: Boolean = slowCallDuration < Duration.ofMillis(individualRequestTimeoutMs)

    // The TimeLimiter margin is a COMPUTED property of transport (connect + pendingAcquire + headroom), so it
    // ALWAYS exceeds the worst-case pre-response phases by construction - no runtime guard needed (see below).
    val timeLimiterTimeout: Duration = responseTimeout.plusMillis(transport.timeLimiterMargin)

    init {
        // Every message names the YAML key, the endpoint and the offending value: a ladder that cannot form is
        // reported at the first resolveAll(), far from the file the value came from. A margin is the merged
        // value - `app.mesh.defaults.margins` with this endpoint's partial `margins` override on top - so the
        // message names both places it can come from.
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
        require(margins.slowCallBackoffMs >= 0) {
            "[$triplet] margins.slow-call-backoff-ms must be >= 0, but was ${margins.slowCallBackoffMs} ($MARGIN_SOURCES)"
        }
        require(margins.slowCallCapFraction > 0.0 && margins.slowCallCapFraction < 1.0) {
            "[$triplet] margins.slow-call-cap-fraction must be in (0,1) (else the slowCall cap is <= 0 or >= global, " +
                "making detection inert), but was ${margins.slowCallCapFraction} ($MARGIN_SOURCES)"
        }
        // NOTE: the ladder invariant "timeLimiter margin > connect + pendingAcquire" (see the pattern doc, section
        // 1.2) needs no runtime check here: transport.timeLimiterMargin IS connect + pendingAcquire + a positive
        // headroom, so it can never race the responseTimeout regardless of how the transport caps are configured.
    }
}

/** Where a margin of [ResolvedTimeouts] comes from - named in every margin guard's message. */
private const val MARGIN_SOURCES = "app.mesh.defaults.margins, overridden per endpoint by app.mesh.endpoints[].margins"

/**
 * Logs the fully derived per-endpoint mesh timeout ladder once, on application startup.
 *
 * The ladder is otherwise implicit: only the MEASURED anchor (`individual-request-timeout-ms`) is maintained
 * in config; every other rung - global / responseTimeout / slowCall / timeLimiter - is DERIVED by
 * [MeshProperties.resolveAll]. This startup line is the app-side inspectability guarantee, so an operator can
 * read the effective budgets straight from the log instead of recomputing them. Ported from the PoC
 * (`eu.mesh.timeouts.poc`); [ladderLines] is pure (no logging) so it can be asserted directly.
 *
 * A bean of [MeshTimeoutConfig], not a scanned component: the mesh wiring is imported by each client stack's
 * configuration, so a host that scans only its stack's package still gets this line.
 */
class MeshLadderLogger(
    private val props: MeshProperties,
) {
    private val log = LoggerFactory.getLogger(MeshLadderLogger::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun logResolvedLadder() = ladderLines().forEach(log::info)

    /** One line per endpoint (sorted by alias) - the fully derived ladder, for the startup log and tests. */
    fun ladderLines(): List<String> =
        props
            .resolveAll()
            .values
            .sortedBy { it.alias }
            .map { t ->
                "mesh ladder [${t.alias}] ${t.triplet}: " +
                    "individual=${t.individualRequestTimeoutMs}ms global=${t.sidecarGlobalMs}ms " +
                    "responseTimeout=${t.responseTimeout.toMillis()}ms " +
                    "slowCall=${t.slowCallDuration.toMillis()}ms timeLimiter=${t.timeLimiterTimeout.toMillis()}ms"
            }
}
