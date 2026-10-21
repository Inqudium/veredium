package eu.inqudium.veredium.core.resilience

import eu.inqudium.veredium.core.mesh.MeshProperties
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeoutException

/**
 * Tests for the benign-4xx verdict the circuit-breaker customizer installs - the shared RULE, exercised with the
 * stand-in stack of [TestVerdicts]. Each client module proves the same verdict through its own decorator and
 * with Spring's real response exceptions (`RestClientVerdictsTest`, `WebClientVerdictsTest`).
 *
 * The policy object exists so that two derivations cannot disagree: the synchronous error log's severity (each
 * stack's `Resilient*Client`, pinned by its own log-level assertions) and this breaker record predicate. A
 * policy whose consumers drift apart is worse than no policy: the log would call an exchange benign while the
 * breaker charges it against the backend. (The exchange event legatium writes is not a consumer - it
 * classifies a 4xx answer as `rejected` with its status on its own terms.)
 *
 * The distinction that carries the most risk is 408/429. They are 4xx, so a naive "ignore all client errors"
 * would discount them - and a run of request timeouts or rate-limit rejections is exactly what a breaker must
 * react to.
 *
 * Behaviour is asserted through the customized instance's config, never against the contents of a status set,
 * so tightening the policy stays a one-line change.
 */
class HttpExchangeErrorPolicyTest {
    private val alias = "api-example-com-GET-accounts"
    private val triplet = "/endpoint/api.example.com/GET/accounts/{accountId}"

    @Nested
    inner class Policy {
        @Test
        fun `should read nothing but a response exception of its stack as a client error`() {
            // What is tested: the one thing the stack supplies - the status reader - and what the rule does with a
            //   throwable the reader does not recognise.
            // Success criteria: a response exception with a 4xx is expected, every other throwable (and null)
            //   is not.
            // Why it matters: the predicate runs on every error the breaker sees; a reader that answered a status
            //   for a timeout or a bulkhead rejection would turn an operational fault into a business case.
            //
            // Given: the shared test policy
            val policy = TestVerdicts.httpExchangeError

            // When / Then
            assertThat(policy.isExpectedClientError(TestResponseException(404))).isTrue()
            assertThat(policy.isExpectedClientError(TimeoutException("budget blown"))).isFalse()
            assertThat(policy.isExpectedClientError(TestVerdicts.bulkheadRejection())).isFalse()
            assertThat(policy.isExpectedClientError(null)).isFalse()
        }

        @Test
        fun `should refuse a probe its own policy does not read as a benign client error`() {
            // What is tested: the consistency rule BreakerVerdicts enforces between the probe and the policy.
            // Success criteria: a probe the policy rejects (a 500) fails construction and names the type.
            // Why it matters: the startup guard applies every breaker's record predicate to this probe; a probe
            //   the policy does not recognise would make the guard fail every start of that stack.
            //
            // Given / When / Then
            assertThatThrownBy {
                BreakerVerdicts(TestVerdicts.httpExchangeError, TestVerdicts.clientSideFailure, TestResponseException(500))
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("TestResponseException")
        }
    }

    @Nested
    inner class Customization {
        @Test
        fun `should record a benign 4xx as a success so a business case cannot open the breaker`() {
            // What is tested: that the statuses the adapter contract documents as normal outcomes - a 404 for
            //   "does not exist", a 400 for a malformed body, 403, 409 - are not charged to the backend.
            // Success criteria: the customized instance's RECORD predicate rejects each of them, so they
            //   enter the sliding window as successes rather than being kept out of it altogether.
            // Why it matters: a healthy backend returns 4xx for bad requests all day. Counting those toward the
            //   failure rate lets a 404-heavy lookup open the circuit against a backend that is perfectly up -
            //   a self-inflicted outage caused by ordinary business traffic.

            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()

            // When
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // Then
            assertThat(countsAsFailure(registry, TestResponseException(404))).isFalse()
            assertThat(countsAsFailure(registry, TestResponseException(400))).isFalse()
            assertThat(countsAsFailure(registry, TestResponseException(403))).isFalse()
            assertThat(countsAsFailure(registry, TestResponseException(409))).isFalse()
        }

        @Test
        fun `should still record 408 and 429 although both are 4xx`() {
            // What is tested: the boundary the policy draws INSIDE the 4xx range - 408 Request Timeout and 429
            //   Too Many Requests are operational signals, not business cases, and stay recorded.
            // Success criteria: the predicate rejects both, while a neighbouring status (404) is accepted -
            //   so the assertion cannot pass by the predicate simply ignoring nothing.
            // Why it matters: this is the one distinction a reimplementation would get wrong, and getting it
            //   wrong is invisible: the breaker would sit blind through exactly the two conditions it exists to
            //   react to - a backend timing out under load and a backend shedding load. A 408/429 burst would
            //   never open the circuit, so retries keep hammering a backend that is already asking for relief.

            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()

            // When
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // Then
            assertThat(countsAsFailure(registry, TestResponseException(408))).isTrue()
            assertThat(countsAsFailure(registry, TestResponseException(429))).isTrue()
            assertThat(countsAsFailure(registry, TestResponseException(404)))
                .`as`("the control: the predicate does discount a benign 4xx, so the two above are a real boundary")
                .isFalse()
        }

        @Test
        fun `should still record a 5xx and a resilience timeout`() {
            // What is tested: the record side of the customized breaker for a genuine backend fault - 500, 503 and
            //   a resilience TimeoutException.
            // Success criteria: the record predicate accepts all three, so each counts as a failure.
            // Why it matters: the counterpart that keeps the tests above honest: the verdict must discount ONLY
            //   benign 4xx, not disable failure recording - a breaker that recorded nothing would never open.
            //
            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()

            // When
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // Then
            assertThat(countsAsFailure(registry, TestResponseException(500))).isTrue()
            assertThat(countsAsFailure(registry, TestResponseException(503))).isTrue()
            assertThat(countsAsFailure(registry, TimeoutException("budget blown"))).isTrue()
        }

        @Test
        fun `should preserve an operator's own ignore list instead of replacing it`() {
            // What is tested: that the verdict is ADDITIVE on the record side. `recordException(predicate)`
            //   REPLACES the predicate the builder holds, so a naive customizer drops whatever the operator
            //   configured; the customizer reads the configured predicates first and folds them back in.
            // Success criteria: after the customizer both the operator's exception and a benign 4xx are
            //   discounted, and an unrelated failure is still recorded.
            // Why it matters: the customizer shapes a config it does not own. Clobbering the operator's list
            //   would silently re-arm the breaker against errors they deliberately excluded - visible to them
            //   only as unexplained open circuits.

            // Given: what YAML produced already ignores an exception of the operator's choosing
            val registry = CircuitBreakerRegistry.ofDefaults()
            registry.circuitBreaker(alias, CircuitBreakerConfig.custom().ignoreExceptions(IllegalStateException::class.java).build())

            // When
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // Then
            assertThat(ignores(registry, IllegalStateException("operator's choice"))).isTrue()
            assertThat(countsAsFailure(registry, TestResponseException(404))).isFalse()
            assertThat(countsAsFailure(registry, TestResponseException(500))).isTrue()
        }

        @Test
        fun `should customize every configured mesh backend`() {
            // What is tested: that `CustomizedRegistries.circuitBreakers` installs the benign-4xx rule for EVERY
            //   mesh endpoint, not only the first.
            // Success criteria: with two endpoints, a 404 is not counted as a failure on the breaker of either
            //   derived alias.
            // Why it matters: one customizer is registered per alias; a backend left without the rule would open
            //   its circuit on ordinary 404 traffic while its siblings stay closed.

            // Given: two endpoints, so there is one customizer per alias rather than a single one
            val registry = CircuitBreakerRegistry.ofDefaults()
            val props =
                MeshProperties(
                    defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(triplet),
                            MeshProperties.EndpointConfig("/endpoint/api.example.com/POST/payments"),
                        ),
                )

            // When
            CustomizedRegistries.circuitBreakers(registry, props)

            // Then: every derived alias carries the rule
            assertThat(countsAsFailure(registry, TestResponseException(404))).isFalse()
            assertThat(countsAsFailure(registry, TestResponseException(404), alias = "api-example-com-POST-payments")).isFalse()
        }

        @Test
        fun `should install the rule even when the mesh circuit-breaker toggle is off`() {
            // What is tested: that the benign-4xx rule is NOT gated on ResolvedTimeouts.circuitBreakerEnabled.
            // Success criteria: with the toggle at its default (off), the customized breaker still does not count
            //   a 404 as a failure.
            // Why it matters: the toggle governs only whether mesh DERIVES the slow-call knobs; the breaker is in
            //   the call path unconditionally. Gating this verdict on it would leave the invariant unmet in
            //   exactly the default configuration - the one that ships.

            // Given: mesh defaults, i.e. the toggle off
            val registry = CircuitBreakerRegistry.ofDefaults()
            val props = propsFor()
            assertThat(props.resolveAll().getValue(triplet).circuitBreakerEnabled).isFalse()

            // When
            CustomizedRegistries.circuitBreakers(registry, props)

            // Then
            assertThat(countsAsFailure(registry, TestResponseException(404))).isFalse()
        }

        @Test
        fun `should keep the two verdicts apart in the one customizer`() {
            // What is tested: the one customizer per alias installs BOTH verdicts, and neither widened into a
            //   blanket ignore: a benign 4xx is a success (not recorded, not ignored), a client-side failure is
            //   ignored, and a 5xx or a 429 is recorded.
            // Success criteria: four probes, each landing in its own row of the three-verdict table.
            // Why it matters: the two verdicts answer different questions ("did the backend answer?" versus
            //   "does the call carry evidence at all?") and used to be two contributors; folding them into one
            //   customizer must not fold the answers into one.

            // Given / When
            val registry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // Then
            assertThat(countsAsFailure(registry, TestResponseException(404))).isFalse()
            assertThat(ignores(registry, TestResponseException(404))).`as`("a benign 4xx is a SUCCESS, not an ignore").isFalse()
            assertThat(ignores(registry, TestVerdicts.bulkheadRejection())).isTrue()
            assertThat(ignores(registry, TestBufferLimitException())).isTrue()
            assertThat(countsAsFailure(registry, TestResponseException(500))).isTrue()
            assertThat(countsAsFailure(registry, TestResponseException(429))).isTrue()
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** Whether the registered instance's config classifies [error] as ignorable (not a backend failure). */
    private fun ignores(
        registry: CircuitBreakerRegistry,
        error: Throwable,
        alias: String = this.alias,
    ): Boolean =
        registry
            .circuitBreaker(alias)
            .circuitBreakerConfig.ignoreExceptionPredicate
            .test(error)

    /**
     * Whether the breaker would charge [error] to the backend.
     *
     * Reads the RECORD predicate, which is the whole point of this verdict: a benign 4xx must come back `false`
     * here - not recorded, therefore counted as a SUCCESS and entering the sliding window as one. Asserting
     * through [ignores] instead would pass for a benign 4xx that was merely kept out of the window, which is
     * the behaviour this verdict was built to avoid.
     */
    private fun countsAsFailure(
        registry: CircuitBreakerRegistry,
        error: Throwable,
        alias: String = this.alias,
    ): Boolean =
        registry
            .circuitBreaker(alias)
            .circuitBreakerConfig.recordExceptionPredicate
            .test(error)

    /** Props for the single accounts endpoint, mesh defaults otherwise. */
    private fun propsFor(): MeshProperties =
        MeshProperties(
            defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
            endpoints = listOf(MeshProperties.EndpointConfig(triplet)),
        )
}
