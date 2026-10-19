package eu.inqudium.veredium.restclient.mesh

import eu.inqudium.veredium.restclient.mesh.MeshProperties.Margins.Companion.DEFAULT_GLOBAL_MARGIN_MS
import eu.inqudium.veredium.restclient.mesh.MeshProperties.Margins.Companion.DEFAULT_HEALTHY_RETRIES
import eu.inqudium.veredium.restclient.mesh.MeshProperties.Margins.Companion.DEFAULT_P99_MARGIN_MS
import eu.inqudium.veredium.restclient.mesh.MeshProperties.Margins.Companion.DEFAULT_REST_CLIENT_MARGIN_MS
import eu.inqudium.veredium.restclient.mesh.MeshProperties.Margins.Companion.DEFAULT_SLOW_CALL_BACKOFF_MS
import eu.inqudium.veredium.restclient.mesh.MeshProperties.Margins.Companion.DEFAULT_SLOW_CALL_CAP_FRACTION
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

/**
 * Tests the mesh DERIVATION LOGIC on hand-built inputs: the alias derived from a triplet (host-prefixed R4j
 * instance name / metric label, with the collision guard) and the full timeout ladder derived from the anchor
 * and retry budget. The CONTENT of the shipped `application.yml` that feeds this logic is pinned separately by
 * [MeshApplicationYmlBindingTest].
 */
class MeshPropertiesDerivationTest {
    @Test
    fun `derives a host-prefixed alias from the triplet, dropping path variables`() {
        // Given an endpoint with no explicit alias
        val endpoint = MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/accounts/{accountId}")

        // When / Then: host (dots -> hyphens) + UPPERCASE method + static path segments, {accountId} dropped
        assertThat(endpoint.effectiveAlias()).isEqualTo("api-example-com-GET-accounts")
    }

    @Test
    fun `rejects a triplet that carries no path, rather than deriving a bare slash from it`() {
        // What is tested: the boundary of the triplet guard - a triplet consisting of only endpoint, host and
        //   method, which is what a dropped path segment leaves behind.
        //   `/endpoint/api.example.com/GET/x` is asserted alongside it, so the test states where the line runs and
        //   cannot pass by rejecting everything.
        // How success is judged: the three-part form throws with a message naming the expected shape; the
        //   four-part form derives its path template normally.
        // Why this case is important: nothing downstream would have complained. `pathTemplate()` returned "/",
        //   which is a valid-looking value everywhere it goes - as a metric prefix it matches EVERY URI of that
        //   client, the alias falls back to "root", and the RestClient calls the sidecar's root. The
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
    fun `the same path and method on different hosts derive distinct, non-colliding aliases`() {
        // What is tested: that the host prefix makes two otherwise-identical endpoints (same path + method,
        // different host) resolve to DIFFERENT aliases, so binding them together does not trip the unique-alias
        // guard in MeshProperties.init.
        // How it is deemed successful: the two aliases differ AND a MeshProperties over both constructs (its init
        // runs the uniqueness check) and resolves two endpoints.
        // Why it matters: without the host prefix both would derive "GET-v1-orders" - the config would be
        // rejected, or (if the guard were absent) the two would silently share one R4j instance / metric series.

        // Given two endpoints identical but for the host, neither with an explicit alias
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
    fun `rejects two endpoints carrying the same triplet even when their aliases differ`() {
        // What is tested: that a duplicated triplet is rejected at binding time, not tolerated.
        // How it is deemed successful: constructing MeshProperties throws, and the message names the offending
        //   triplet. The two entries deliberately carry DISTINCT explicit aliases, so the alias guard cannot be
        //   what rejects them - only a guard on the triplet itself can, which is exactly what is under test.
        // Why it matters: resolveAll() keys by triplet via `associate`, which keeps the LAST entry for a repeated
        //   key. Without this guard the second endpoint silently replaces the first, the dropped alias is missing
        //   from the factory's map, and the loss surfaces only at the first call to that backend as "No mesh
        //   endpoint configured" - the late, confusing failure the fail-fast exists to prevent.

        // Given two endpoints with the SAME triplet but different explicit aliases
        val first = MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/v1/orders", alias = "orders-primary")
        val second = MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/v1/orders", alias = "orders-secondary")

        // When / Then: binding them together fails fast and names the duplicate
        assertThatThrownBy { MeshProperties(MeshProperties.Defaults(), listOf(first, second)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("Mesh triplets collide")
            .hasMessageContaining("/endpoint/api.example.com/GET/v1/orders")
    }

    @Test
    fun `rejects two endpoints whose aliases collide`() {
        // Given: the same triplet is not involved - two DIFFERENT endpoints pinned to one alias
        val first = MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/v1/orders", alias = "shared")
        val second = MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/v1/invoices", alias = "shared")

        // When / Then: the alias guard rejects it and names the offender (an alias is an R4j instance name and
        // a metric label; sharing one would silently merge two backends' resilience state and time series)
        assertThatThrownBy { MeshProperties(MeshProperties.Defaults(), listOf(first, second)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("Mesh aliases collide")
            .hasMessageContaining("shared")
    }

    @Test
    fun `rejects a transport pool size the Apache builder would silently ignore`() {
        // What is tested: the bottom of the transport's range - max-connections 0 and below.
        //   1 is asserted alongside, so the test states where the line runs and cannot pass by rejecting everything.
        // How success is judged: 0 throws with a message naming the YAML key; 1 binds.
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
    fun `rejects a transport timeout that would leave a pre-response phase unbounded`() {
        // What is tested: the bottom of the two transport timeouts - zero and negative values.
        // How success is judged: each throws with a message naming its YAML key; 1 ms binds for both.
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
    fun `the derived ladder is correct across individual 1s-10s and 0-3 retries`(
        individualMs: Long,
        maxRetries: Int,
    ) {
        // What is tested: the whole derivation (global, responseTimeout, timeLimiter, slowCall) for the anchor
        // sweep individual 1s..10s x maxRetries 0..3, with all other values at their defaults.
        // How it is deemed successful: each rung equals the formula recomputed here with the DEFAULT margin
        // CONSTANTS - so the test breaks if either the formula or a default changes.
        // Why it matters: the ladder is the core value of the module; a broad parameterized sweep guards the
        // arithmetic (and the min-cap crossover in slowCall) far better than a couple of hand-picked cases.
        val props =
            MeshProperties(
                defaults = MeshProperties.Defaults(individualRequestTimeoutMs = individualMs, maxRetries = maxRetries),
                endpoints = listOf(MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/accounts/{accountId}")),
            )

        // When the single endpoint is resolved
        val t = props.resolveAll().getValue("/endpoint/api.example.com/GET/accounts/{accountId}")

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

        // Then every rung matches the derivation, using the default-margin CONSTANTS (not literals) so the test
        // tracks any change to a default automatically.
        val expectedGlobal = (1 + maxRetries) * individualMs + DEFAULT_GLOBAL_MARGIN_MS
        val expectedRestClient = expectedGlobal + DEFAULT_REST_CLIENT_MARGIN_MS
        val expectedTimeLimiter = expectedRestClient + MeshProperties.Transport().timeLimiterMargin // computed: 100+200+100
        val p99 = individualMs - DEFAULT_P99_MARGIN_MS
        // healthyRetries is capped at the actual retry budget (mirrors ResolvedTimeouts): with 0 retries none fold
        // in, so the healthy ceiling is a single attempt (p99).
        val effectiveHealthyRetries = minOf(maxRetries, DEFAULT_HEALTHY_RETRIES)
        val healthyCeiling = p99 * (1 + effectiveHealthyRetries) + effectiveHealthyRetries * DEFAULT_SLOW_CALL_BACKOFF_MS
        val expectedSlowCall = minOf(healthyCeiling, (expectedGlobal * DEFAULT_SLOW_CALL_CAP_FRACTION).toLong())

        assertThat(t.sidecarGlobalMs).`as`(ladder).isEqualTo(expectedGlobal)
        assertThat(t.responseTimeout.toMillis()).`as`(ladder).isEqualTo(expectedRestClient)
        assertThat(t.timeLimiterTimeout.toMillis()).`as`(ladder).isEqualTo(expectedTimeLimiter)
        assertThat(t.slowCallDuration.toMillis()).`as`(ladder).isEqualTo(expectedSlowCall)
    }

    companion object {
        /** Cross product: individual 1000..10000ms (1s steps) x maxRetries 0..3 -> 40 cases. */
        @JvmStatic
        fun anchorAndRetryCombinations(): Stream<Arguments> =
            (1..10)
                .flatMap { seconds ->
                    (0..3).map { retries -> Arguments.of((seconds * 1000).toLong(), retries) }
                }.stream()
    }
}
