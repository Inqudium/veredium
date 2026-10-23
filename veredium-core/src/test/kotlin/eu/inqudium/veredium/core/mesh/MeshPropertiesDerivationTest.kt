package eu.inqudium.veredium.core.mesh

import eu.inqudium.veredium.core.mesh.MeshProperties.Margins.Companion.DEFAULT_GLOBAL_MARGIN_MS
import eu.inqudium.veredium.core.mesh.MeshProperties.Margins.Companion.DEFAULT_HEALTHY_RETRIES
import eu.inqudium.veredium.core.mesh.MeshProperties.Margins.Companion.DEFAULT_P99_MARGIN_MS
import eu.inqudium.veredium.core.mesh.MeshProperties.Margins.Companion.DEFAULT_RESPONSE_TIMEOUT_MARGIN_MS
import eu.inqudium.veredium.core.mesh.MeshProperties.Margins.Companion.DEFAULT_SLOW_CALL_BACKOFF_MS
import eu.inqudium.veredium.core.mesh.MeshProperties.Margins.Companion.DEFAULT_SLOW_CALL_CAP_FRACTION
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

/**
 * Tests the mesh DERIVATION LOGIC on hand-built inputs: the alias derived from a triplet (host-prefixed R4j
 * instance name / metric label, with the collision guard) and the full timeout ladder derived from the anchor
 * and retry budget. The binding of a complete `app.mesh` block from the test fixture is pinned separately by
 * [MeshFixtureBindingTest].
 */
class MeshPropertiesDerivationTest {
    @Test
    fun `should derive a host-prefixed alias from the triplet, dropping path variables`() {
        // What is tested: `effectiveAlias()` of an endpoint without an explicit alias - the derivation from
        //   the triplet `/endpoint/<host>/<METHOD>/<path>`.
        // Success criteria: the alias is the host with dots turned into hyphens, the UPPERCASE method and the
        //   static path segments, with the `{accountId}` path variable dropped: "api-example-com-GET-accounts".
        // Why it matters: the alias is the Resilience4j instance name and the metric label; a path variable in
        //   it would be a literal brace pattern in every metric, and a missing host prefix lets two backends
        //   with the same path collide.
        //
        // Given: an endpoint with no explicit alias
        val endpoint = MeshProperties.EndpointConfig(TRIPLET)

        // When / Then: host (dots -> hyphens) + UPPERCASE method + static path segments, {accountId} dropped
        assertThat(endpoint.effectiveAlias()).isEqualTo("api-example-com-GET-accounts")
    }

    @Test
    fun `should reject a triplet that carries no path, rather than deriving a bare slash from it`() {
        // What is tested: the boundary of the triplet guard - a triplet consisting of only endpoint, host and
        //   method, which is what a dropped path segment leaves behind.
        //   `/endpoint/api.example.com/GET/x` is asserted alongside it, so the test states where the line runs and
        //   cannot pass by rejecting everything.
        // Success criteria: the three-part form throws with a message naming the expected shape; the
        //   four-part form derives its path template normally.
        // Why it matters: nothing downstream complains about a pathless triplet. Its `pathTemplate()` would be
        //   "/", which is a valid-looking value everywhere it goes - as a metric prefix it matches EVERY URI of that
        //   client, the alias falls back to "root", and the client calls the sidecar's root. The
        //   misconfiguration would first show up as a 404 in production, pointing nowhere near the config line
        //   that caused it. Failing at binding time is the whole point of the guard.
        //
        // Given / When / Then: three parts is one too few
        assertThatThrownBy { MeshProperties.EndpointConfig("/endpoint/api.example.com/GET").pathTemplate() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("/endpoint/<host>/<METHOD>/<path...>")

        // ... and four is enough
        assertThat(MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/x").pathTemplate()).isEqualTo("/x")
    }

    @Test
    fun `should derive distinct, non-colliding aliases for the same path and method on different hosts`() {
        // What is tested: that the host prefix makes two otherwise-identical endpoints (same path + method,
        //   different host) resolve to DIFFERENT aliases, so binding them together does not trip the unique-alias
        //   guard in MeshProperties.init.
        // Success criteria: the two aliases differ AND a MeshProperties over both constructs (its init
        //   runs the uniqueness check) and resolves two endpoints.
        // Why it matters: without the host prefix both would derive "GET-v1-orders" - the config would be
        //   rejected, or (if the guard were absent) the two would silently share one R4j instance / metric series.
        //
        // Given: two endpoints identical but for the host, neither with an explicit alias
        val onApi = MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/v1/orders")
        val onApis = MeshProperties.EndpointConfig("/endpoint/apis.example.com/GET/v1/orders")

        // When
        val aliasOnApi = onApi.effectiveAlias()
        val aliasOnApis = onApis.effectiveAlias()

        // Then: distinct aliases, and both bind together without a collision
        assertThat(aliasOnApi).isEqualTo("api-example-com-GET-v1-orders")
        assertThat(aliasOnApis).isEqualTo("apis-example-com-GET-v1-orders")
        assertThat(aliasOnApi).isNotEqualTo(aliasOnApis)
        val props = MeshProperties(MeshProperties.Defaults(individualRequestTimeoutMs = 1000), listOf(onApi, onApis))
        assertThat(props.resolveAll()).hasSize(2)
    }

    @Test
    fun `should reject two endpoints carrying the same triplet even when their aliases differ`() {
        // What is tested: that a duplicated triplet is rejected at binding time, not tolerated.
        // Success criteria: constructing MeshProperties throws, and the message names the offending
        //   triplet. The two entries deliberately carry DISTINCT explicit aliases, so the alias guard cannot be
        //   what rejects them - only a guard on the triplet itself can, which is exactly what is under test.
        // Why it matters: resolveAll() keys by triplet via `associate`, which keeps the LAST entry for a repeated
        //   key. Without this guard the second endpoint silently replaces the first, the dropped alias is missing
        //   from the factory's map, and the loss surfaces only at the first call to that backend as "No mesh
        //   endpoint configured" - the late, confusing failure the fail-fast exists to prevent.
        //
        // Given: two endpoints with the SAME triplet but different explicit aliases
        val first = MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/v1/orders", alias = "orders-primary")
        val second = MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/v1/orders", alias = "orders-secondary")

        // When / Then: binding them together fails fast and names the duplicate
        assertThatThrownBy { MeshProperties(MeshProperties.Defaults(), listOf(first, second)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("Mesh triplets collide")
            .hasMessageContaining("/endpoint/api.example.com/GET/v1/orders")
    }

    @Test
    fun `should reject two endpoints whose aliases collide`() {
        // What is tested: the unique-alias guard in `MeshProperties.init` for two endpoints with DIFFERENT
        //   triplets that are pinned to the same explicit alias.
        // Success criteria: constructing MeshProperties throws IllegalArgumentException with "Mesh aliases
        //   collide" and the offending alias "shared" in the message.
        // Why it matters: an alias is an R4j instance name and a metric label; sharing one would silently merge
        //   two backends' resilience state and time series - the triplet guard cannot catch this case.
        //
        // Given: the same triplet is not involved - two DIFFERENT endpoints pinned to one alias
        val first = MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/v1/orders", alias = "shared")
        val second = MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/v1/invoices", alias = "shared")

        // When / Then: the alias guard rejects it and names the offending alias
        assertThatThrownBy { MeshProperties(MeshProperties.Defaults(), listOf(first, second)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("Mesh aliases collide")
            .hasMessageContaining("shared")
    }

    @Test
    fun `should reject a transport pool size the Apache builder would silently ignore`() {
        // What is tested: the bottom of the transport's range - max-connections 0 and below.
        //   1 is asserted alongside, so the test states where the line runs and cannot pass by rejecting everything.
        // Success criteria: 0 throws with a message naming the YAML key; 1 binds.
        // Why it matters: Apache's PoolingHttpClientConnectionManagerBuilder applies max-connections only when it
        //   is positive. A zero would not fail and would not mean "no pool" - it would mean Apache's default of
        //   25 total, a pool that fills long before the bulkheads (sized against 100) shed anything, with
        //   pending-acquire timeouts as the only visible symptom.
        //
        // Given / When / Then
        assertThatThrownBy { MeshProperties.Transport(maxConnections = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("app.mesh.defaults.transport.max-connections")
        assertThat(MeshProperties.Transport(maxConnections = 1).maxConnections).isEqualTo(1)
    }

    @Test
    fun `should reject a transport timeout that would leave a pre-response phase unbounded`() {
        // What is tested: the bottom of the two transport timeouts - zero and negative values.
        // Success criteria: each throws with a message naming its YAML key; 1 ms binds for both.
        // Why it matters: both values feed the computed timeLimiterMargin as the WORST CASE of the connect and
        //   pending-acquire phases. A zero is "no timeout" to the transport, so the margin would promise a bound
        //   the ladder does not have; a negative value would reach Apache's Timeout factory with a message that
        //   names neither the key nor the file.
        //
        // Given / When / Then
        assertThatThrownBy { MeshProperties.Transport(connectTimeoutMs = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("app.mesh.defaults.transport.connect-timeout-ms")
        assertThatThrownBy { MeshProperties.Transport(pendingAcquireTimeoutMs = -1) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("app.mesh.defaults.transport.pending-acquire-timeout-ms")
        assertThat(MeshProperties.Transport(connectTimeoutMs = 1, pendingAcquireTimeoutMs = 1).timeLimiterMargin)
            .isEqualTo(2 + MeshProperties.Transport.TIME_LIMITER_HEADROOM_MS)
    }

    @ParameterizedTest(name = "individual={0}ms, maxRetries={1}")
    @MethodSource("anchorAndRetryCombinations")
    fun `should derive the correct ladder across individual 1s-10s and 0-3 retries`(
        individualMs: Long,
        maxRetries: Int,
    ) {
        // What is tested: the whole derivation (global, responseTimeout, timeLimiter, slowCall) for the anchor
        //   sweep individual 1s..10s x maxRetries 0..3, with all other values at their defaults.
        // Success criteria: each rung equals the formula recomputed here with the DEFAULT margin
        //   CONSTANTS - so the test breaks if either the formula or a default changes.
        // Why it matters: the ladder is the core value of the module; a broad parameterized sweep guards the
        //   arithmetic (and the min-cap crossover in slowCall) far better than a couple of hand-picked cases.
        //
        // Given: one endpoint on the swept anchor and retry budget, every margin at its default
        val props =
            MeshProperties(
                defaults = MeshProperties.Defaults(individualRequestTimeoutMs = individualMs, maxRetries = maxRetries),
                endpoints = listOf(MeshProperties.EndpointConfig(TRIPLET)),
            )

        // When: the single endpoint is resolved
        val t = props.resolveAll().getValue(TRIPLET)

        // The derived ladder for this case, rungs sorted ascending by value, carried on every assertion below so a
        // failure shows the whole ladder and the ordering invariant (individual < ... < timeLimiter) at a glance.
        val rungs =
            linkedMapOf(
                "individual" to t.individualRequestTimeoutMs,
                "slowCall" to t.slowCallDuration.toMillis(),
                "global" to t.sidecarGlobalMs,
                "responseTimeout" to t.responseTimeout.toMillis(),
                "timeLimiter" to t.timeLimiterTimeout.toMillis(),
            )
        val ladder =
            "ladder [individual=${individualMs}ms maxRetries=$maxRetries] " +
                rungs.entries.sortedBy { it.value }.joinToString(" < ") { "${it.key}=${it.value}ms" }

        // Then: every rung matches the derivation, using the default-margin CONSTANTS (not literals) so the test
        // tracks any change to a default automatically.
        val expectedGlobal = (1 + maxRetries) * individualMs + DEFAULT_GLOBAL_MARGIN_MS
        val expectedResponseTimeout = expectedGlobal + DEFAULT_RESPONSE_TIMEOUT_MARGIN_MS
        val expectedTimeLimiter = expectedResponseTimeout + MeshProperties.Transport().timeLimiterMargin // computed: 100+200+100
        val p99 = individualMs - DEFAULT_P99_MARGIN_MS
        // healthyRetries is capped at the actual retry budget (mirrors ResolvedTimeouts): with 0 retries none fold
        // in, so the healthy ceiling is a single attempt (p99).
        val effectiveHealthyRetries = minOf(maxRetries, DEFAULT_HEALTHY_RETRIES)
        val healthyCeiling = p99 * (1 + effectiveHealthyRetries) + effectiveHealthyRetries * DEFAULT_SLOW_CALL_BACKOFF_MS
        val expectedSlowCall = minOf(healthyCeiling, (expectedGlobal * DEFAULT_SLOW_CALL_CAP_FRACTION).toLong())

        assertThat(t.sidecarGlobalMs).`as`(ladder).isEqualTo(expectedGlobal)
        assertThat(t.responseTimeout.toMillis()).`as`(ladder).isEqualTo(expectedResponseTimeout)
        assertThat(t.timeLimiterTimeout.toMillis()).`as`(ladder).isEqualTo(expectedTimeLimiter)
        assertThat(t.slowCallDuration.toMillis()).`as`(ladder).isEqualTo(expectedSlowCall)
    }

    @ParameterizedTest(name = "individual={0}ms, maxRetries={1} -> global={2} responseTimeout={3} timeLimiter={4} slowCall={5}")
    @MethodSource("handComputedLaddersOnDistinctMargins")
    fun `should derive hand-computed ladders when every margin differs from every other`(
        individualMs: Long,
        maxRetries: Int,
        global: Long,
        responseTimeout: Long,
        timeLimiter: Long,
        slowCall: Long,
    ) {
        // What is tested: the same derivation as the sweep above, but on margins that are pairwise DISTINCT
        //   (global 50, response-timeout 130, p99 170, slow-call-backoff 90), a non-default healthy-retries (2)
        //   and slow-call-cap-fraction (0.75), and a non-default transport (connect 30 + pending-acquire 70 +
        //   headroom 100 = timeLimiter margin 200).
        // Success criteria: every rung equals a literal computed by hand (the rows of
        //   handComputedLaddersOnDistinctMargins) - not a formula restated in the test. The rows cover both arms
        //   of the slow-call min (the cap at maxRetries 0/1 and at 2500/2; the healthy ceiling at maxRetries 3)
        //   and both sides of the healthy-retries cap (min(1, 2) = 1 and min(3, 2) = 2).
        // Why it matters: the default margins are all 200, so on them a rung computed from the WRONG margin
        //   field gives the right number - the sweep above passes even if responseTimeout adds the p99 margin, or
        //   the healthy ceiling multiplies the p99 margin instead of the backoff. Here each such swap moves at
        //   least one rung, and a mirrored oracle would only repeat the swap.
        //
        // Given
        val props =
            MeshProperties(
                defaults =
                    MeshProperties.Defaults(
                        individualRequestTimeoutMs = individualMs,
                        maxRetries = maxRetries,
                        margins = DISTINCT_MARGINS,
                        transport = MeshProperties.Transport(connectTimeoutMs = 30, pendingAcquireTimeoutMs = 70),
                    ),
                endpoints = listOf(MeshProperties.EndpointConfig(TRIPLET)),
            )

        // When
        val t = props.resolveAll().getValue(TRIPLET)

        // Then
        assertThat(t.sidecarGlobalMs).`as`("global").isEqualTo(global)
        assertThat(t.responseTimeout.toMillis()).`as`("responseTimeout").isEqualTo(responseTimeout)
        assertThat(t.timeLimiterTimeout.toMillis()).`as`("timeLimiter").isEqualTo(timeLimiter)
        assertThat(t.slowCallDuration.toMillis()).`as`("slowCall").isEqualTo(slowCall)
    }

    @Test
    fun `should merge a partial per-endpoint margins override field by field onto the shared defaults`() {
        // What is tested: MarginsOverride / Margins.overriddenBy as resolveAll applies them - an endpoint that
        //   overrides TWO margin fields, next to one that overrides none.
        // Success criteria: the overriding endpoint carries its two values and the shared defaults' values for
        //   the four fields it left unset; the other endpoint carries the shared defaults unchanged; and the
        //   override shows in the derived ladder (its slow-call is computed from the overridden fraction).
        // Why it matters: the documented promise is "only the fields you set change". A merge that took the whole
        //   override (resetting the unset fields to the Kotlin defaults) or ignored it would both produce a valid
        //   ladder - one that silently differs from what the operator wrote for that one endpoint.
        //
        // Given: shared margins that differ from the Kotlin defaults, and an override of two fields
        val tuned = MeshProperties.EndpointConfig(TRIPLET, margins = MeshProperties.MarginsOverride(p99MarginMs = 333, slowCallCapFraction = 0.5))
        val untouched = MeshProperties.EndpointConfig("/endpoint/api.example.com/POST/accounts")
        val props =
            MeshProperties(
                defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000, maxRetries = 3, margins = DISTINCT_MARGINS),
                endpoints = listOf(tuned, untouched),
            )

        // When
        val resolved = props.resolveAll()
        val tunedMargins = resolved.getValue(tuned.triplet).margins
        val untouchedMargins = resolved.getValue(untouched.triplet).margins

        // Then: the two overridden fields, the four inherited ones ...
        assertThat(tunedMargins)
            .isEqualTo(
                MeshProperties.Margins(
                    globalMarginMs = 50,
                    responseTimeoutMarginMs = 130,
                    p99MarginMs = 333,
                    healthyRetries = 2,
                    slowCallBackoffMs = 90,
                    slowCallCapFraction = 0.5,
                ),
            )
        // ... the endpoint without an override on the shared margins as they are ...
        assertThat(untouchedMargins).isEqualTo(DISTINCT_MARGINS)
        assertThat(DISTINCT_MARGINS.overriddenBy(null)).isSameAs(DISTINCT_MARGINS)
        // ... and the ladder follows: global 4050, cap 4050 * 0.5 = 2025 < ceiling (667 * 3 + 2 * 90 = 2181)
        assertThat(resolved.getValue(tuned.triplet).slowCallDuration.toMillis()).isEqualTo(2025)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ladderGuardViolations")
    fun `should reject a ladder that cannot form, naming the key, the endpoint and the value`(
        description: String,
        endpoint: MeshProperties.EndpointConfig,
        expectedFragments: List<String>,
    ) {
        // What is tested: each init guard of ResolvedTimeouts - anchor, retries and every margin - reached the
        //   way a configuration reaches it: bound per endpoint, resolved through resolveAll().
        // Success criteria: resolving throws IllegalArgumentException whose message carries the YAML key, the
        //   triplet and the offending value. The accepting side of each boundary is the next test.
        // Why it matters: these guards are the only place a broken ladder is caught before Resilience4j or the
        //   transport sees it; without them a negative global margin passes silently and a zero slow-call
        //   surfaces as a keyless builder error. The messages are what an operator has to act on, so each one is
        //   rendered and checked here.
        //
        // Given
        val props = MeshProperties(MeshProperties.Defaults(individualRequestTimeoutMs = 1000), listOf(endpoint))

        // When
        val error = catchThrowable { props.resolveAll() }

        // Then
        assertThat(error).`as`(description).isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining(TRIPLET)
        expectedFragments.forEach { assertThat(error).`as`(description).hasMessageContaining(it) }
    }

    @Test
    fun `should accept every guarded value at the edge of its range`() {
        // What is tested: the accepting side of every boundary the previous test rejects - global margin 0,
        //   response-timeout margin 1, p99 margin individual - 1 and 0, healthy-retries 0, backoff 0, retries 0.
        // Success criteria: the endpoint resolves, and the extreme values derive the expected rungs (global =
        //   individual, responseTimeout = global + 1, p99 = 1 ms).
        // Why it matters: a guard that is off by one rejects a legitimate configuration - and the previous test
        //   alone would pass against guards that reject everything.
        //
        // Given
        val edge =
            MeshProperties.EndpointConfig(
                TRIPLET,
                maxRetries = 0,
                margins =
                    MeshProperties.MarginsOverride(
                        globalMarginMs = 0,
                        responseTimeoutMarginMs = 1,
                        p99MarginMs = 999,
                        healthyRetries = 0,
                        slowCallBackoffMs = 0,
                    ),
            )
        val p99AtZero = MeshProperties.EndpointConfig("/endpoint/api.example.com/POST/accounts", margins = MeshProperties.MarginsOverride(p99MarginMs = 0))

        // When
        val resolved = MeshProperties(MeshProperties.Defaults(individualRequestTimeoutMs = 1000), listOf(edge, p99AtZero)).resolveAll()

        // Then
        val t = resolved.getValue(TRIPLET)
        assertThat(t.sidecarGlobalMs).isEqualTo(1000)
        assertThat(t.responseTimeout.toMillis()).isEqualTo(1001)
        assertThat(t.p99SingleMs).isEqualTo(1)
        assertThat(resolved.getValue(p99AtZero.triplet).p99SingleMs).isEqualTo(1000)
    }

    @Test
    fun `should reject a transport timeout the reactive stack could not narrow to an Int`() {
        // What is tested: the top of the two transport timeouts - Int.MAX_VALUE + 1 against Int.MAX_VALUE.
        // Success criteria: one past Int.MAX_VALUE throws for each key, naming it and the value; Int.MAX_VALUE
        //   itself binds.
        // Why it matters: the reactive factory hands connect-timeout-ms to Reactor Netty as an Int via toInt(),
        //   which wraps instead of failing - 2147483648 ms would become a NEGATIVE connect timeout. Nobody types
        //   25 days on purpose, but a unit mix-up (micros for millis) does, and nothing downstream says so.
        //
        // Given
        val tooLarge = Int.MAX_VALUE.toLong() + 1

        // When / Then
        assertThatThrownBy { MeshProperties.Transport(connectTimeoutMs = tooLarge) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("app.mesh.defaults.transport.connect-timeout-ms")
            .hasMessageContaining("$tooLarge")
        assertThatThrownBy { MeshProperties.Transport(pendingAcquireTimeoutMs = tooLarge) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("app.mesh.defaults.transport.pending-acquire-timeout-ms")
            .hasMessageContaining("$tooLarge")
        val atTheLimit = MeshProperties.Transport(connectTimeoutMs = Int.MAX_VALUE.toLong(), pendingAcquireTimeoutMs = Int.MAX_VALUE.toLong())
        assertThat(atTheLimit.connectTimeoutMs).isEqualTo(MeshProperties.Transport.MAX_TRANSPORT_TIMEOUT_MS)
    }

    companion object {
        private const val TRIPLET = "/endpoint/api.example.com/GET/accounts/{accountId}"

        /** Cross product: individual 1000..10000ms (1s steps) x maxRetries 0..3 -> 40 cases. */
        @JvmStatic
        fun anchorAndRetryCombinations(): Stream<Arguments> =
            (1..10)
                .flatMap { seconds ->
                    (0..3).map { retries -> Arguments.of((seconds * 1000).toLong(), retries) }
                }.stream()

        /** Every margin distinct from every other and from its Kotlin default (all 200 / 1 / 0.8). */
        private val DISTINCT_MARGINS =
            MeshProperties.Margins(
                globalMarginMs = 50,
                responseTimeoutMarginMs = 130,
                p99MarginMs = 170,
                healthyRetries = 2,
                slowCallBackoffMs = 90,
                slowCallCapFraction = 0.75,
            )

        /**
         * Hand-computed ladders on [DISTINCT_MARGINS] with transport 30 + 70 (timeLimiter margin 200). Per row:
         * global = (1 + r) * I + 50; responseTimeout = global + 130; timeLimiter = responseTimeout + 200;
         * p99 = I - 170; h = min(r, 2); ceiling = p99 * (1 + h) + h * 90; cap = floor(global * 0.75).
         *
         * | I    | r | global | respT | TL   | p99  | h | ceiling | cap  | slowCall      |
         * |------|---|--------|-------|------|------|---|---------|------|---------------|
         * | 1000 | 0 | 1050   | 1180  | 1380 | 830  | 0 | 830     | 787  | 787 (cap)     |
         * | 1000 | 1 | 2050   | 2180  | 2380 | 830  | 1 | 1750    | 1537 | 1537 (cap)    |
         * | 1000 | 3 | 4050   | 4180  | 4380 | 830  | 2 | 2670    | 3037 | 2670 (ceiling)|
         * | 2000 | 3 | 8050   | 8180  | 8380 | 1830 | 2 | 5670    | 6037 | 5670 (ceiling)|
         * | 2500 | 2 | 7550   | 7680  | 7880 | 2330 | 2 | 7170    | 5662 | 5662 (cap)    |
         */
        @JvmStatic
        fun handComputedLaddersOnDistinctMargins(): Stream<Arguments> =
            Stream.of(
                Arguments.of(1000L, 0, 1050L, 1180L, 1380L, 787L),
                Arguments.of(1000L, 1, 2050L, 2180L, 2380L, 1537L),
                Arguments.of(1000L, 3, 4050L, 4180L, 4380L, 2670L),
                Arguments.of(2000L, 3, 8050L, 8180L, 8380L, 5670L),
                Arguments.of(2500L, 2, 7550L, 7680L, 7880L, 5662L),
            )

        /** One violation per init guard of [ResolvedTimeouts], each with the fragments its message must carry. */
        @JvmStatic
        fun ladderGuardViolations(): Stream<Arguments> {
            fun margins(o: MeshProperties.MarginsOverride) = MeshProperties.EndpointConfig(TRIPLET, margins = o)
            return Stream.of(
                Arguments.of(
                    "anchor 0",
                    MeshProperties.EndpointConfig(TRIPLET, individualRequestTimeoutMs = 0),
                    listOf("individual-request-timeout-ms must be >= 1", "but was 0"),
                ),
                Arguments.of(
                    "negative retries",
                    MeshProperties.EndpointConfig(TRIPLET, maxRetries = -1),
                    listOf("max-retries must be >= 0", "but was -1"),
                ),
                Arguments.of(
                    "negative global margin",
                    margins(MeshProperties.MarginsOverride(globalMarginMs = -1)),
                    listOf("margins.global-margin-ms must be >= 0", "but was -1", "app.mesh.endpoints[].margins"),
                ),
                Arguments.of(
                    "response-timeout margin 0",
                    margins(MeshProperties.MarginsOverride(responseTimeoutMarginMs = 0)),
                    listOf("margins.response-timeout-margin-ms must be >= 1", "but was 0"),
                ),
                Arguments.of(
                    "p99 margin equal to the anchor",
                    margins(MeshProperties.MarginsOverride(p99MarginMs = 1000)),
                    listOf("margins.p99-margin-ms must be in 0..<individual-request-timeout-ms", "but was 1000"),
                ),
                Arguments.of(
                    "negative p99 margin",
                    margins(MeshProperties.MarginsOverride(p99MarginMs = -1)),
                    listOf("margins.p99-margin-ms", "but was -1"),
                ),
                Arguments.of(
                    "negative healthy retries",
                    margins(MeshProperties.MarginsOverride(healthyRetries = -1)),
                    listOf("margins.healthy-retries must be >= 0", "but was -1"),
                ),
                Arguments.of(
                    "negative slow-call backoff",
                    margins(MeshProperties.MarginsOverride(slowCallBackoffMs = -1)),
                    listOf("margins.slow-call-backoff-ms must be >= 0", "but was -1"),
                ),
                Arguments.of(
                    "cap fraction 0",
                    margins(MeshProperties.MarginsOverride(slowCallCapFraction = 0.0)),
                    listOf("margins.slow-call-cap-fraction must be in (0,1)", "but was 0.0"),
                ),
                Arguments.of(
                    "cap fraction 1",
                    margins(MeshProperties.MarginsOverride(slowCallCapFraction = 1.0)),
                    listOf("margins.slow-call-cap-fraction must be in (0,1)", "but was 1.0"),
                ),
            )
        }
    }
}
