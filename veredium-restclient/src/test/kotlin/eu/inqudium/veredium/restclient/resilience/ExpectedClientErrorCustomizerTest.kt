package eu.inqudium.veredium.restclient.resilience

import eu.inqudium.veredium.restclient.BlockingResilienceDecorator
import eu.inqudium.veredium.restclient.HttpExchangeErrorPolicy
import eu.inqudium.veredium.restclient.ResponseBodyLimitExceededException
import eu.inqudium.veredium.restclient.mesh.MeshProperties
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadFullException
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatusCode
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.client.RestClientResponseException
import java.time.Duration
import java.util.concurrent.TimeoutException

/**
 * Tests for the benign-4xx verdict the circuit-breaker customizer installs - the second consumer of
 * [HttpExchangeErrorPolicy].
 *
 * The policy object exists so that two derivations cannot disagree: the synchronous error log's severity
 * (`ResilientRestClient`, pinned by its own log-level assertions in `ResilientRestClientIntegrationTest`) and
 * this breaker record predicate. A policy whose consumers drift apart is worse than no policy: the log would
 * call an exchange benign while the breaker charges it against the backend. (The exchange event legatium
 * writes is not a consumer - it classifies a 4xx answer as `rejected` with its status on its own terms.)
 *
 * The distinction that carries the most risk is 408/429. They are 4xx, so a naive "ignore all client errors"
 * would discount them - and a run of request timeouts or rate-limit rejections is exactly what a breaker must
 * react to.
 *
 * Behaviour is asserted through the customized instance's config and through the real decorated pipeline, never
 * against the contents of a status set, so tightening the policy stays a one-line change. The exceptions are
 * built the way the client builds them - Spring's status-specific subclasses for 4xx and 5xx - so the predicate
 * is tested against the exact types it will see.
 */
class ExpectedClientErrorCustomizerTest {
    private val alias = "api-example-com-GET-accounts"
    private val triplet = "/endpoint/api.example.com/GET/accounts/{accountId}"

    private val decorators = mutableListOf<BlockingResilienceDecorator>()

    @AfterEach
    fun tearDown() {
        decorators.forEach { it.shutdown() }
    }

    @Nested
    inner class Customization {
        @Test
        fun `should record a benign 4xx as a success so a business case cannot open the breaker`() {
            // What is tested: that the statuses the adapter contract documents as normal outcomes - a 404 for
            //   "does not exist", a 400 for a malformed body, 403, 409 - are not charged to the backend.
            // How success is judged: the customized instance's RECORD predicate rejects each of them, so they
            //   enter the sliding window as successes rather than being kept out of it altogether.
            // Why it matters: a healthy backend returns 4xx for bad requests all day. Counting those toward the
            //   failure rate lets a 404-heavy lookup open the circuit against a backend that is perfectly up -
            //   a self-inflicted outage caused by ordinary business traffic.

            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()

            // When
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // Then
            assertThat(countsAsFailure(registry, response(404))).isFalse()
            assertThat(countsAsFailure(registry, response(400))).isFalse()
            assertThat(countsAsFailure(registry, response(403))).isFalse()
            assertThat(countsAsFailure(registry, response(409))).isFalse()
        }

        @Test
        fun `should still record 408 and 429 although both are 4xx`() {
            // What is tested: the boundary the policy draws INSIDE the 4xx range - 408 Request Timeout and 429
            //   Too Many Requests are operational signals, not business cases, and stay recorded.
            // How success is judged: the predicate rejects both, while a neighbouring status (404) is accepted -
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
            assertThat(countsAsFailure(registry, response(408))).isTrue()
            assertThat(countsAsFailure(registry, response(429))).isTrue()
            assertThat(countsAsFailure(registry, response(404)))
                .`as`("the control: the predicate does discount a benign 4xx, so the two above are a real boundary")
                .isFalse()
        }

        @Test
        fun `should still record a 5xx and a resilience timeout`() {
            // The counterpart that keeps the tests above honest: the verdict must discount ONLY benign 4xx, not
            // disable failure recording.
            val registry = CircuitBreakerRegistry.ofDefaults()

            CustomizedRegistries.circuitBreakers(registry, propsFor())

            assertThat(countsAsFailure(registry, response(500))).isTrue()
            assertThat(countsAsFailure(registry, response(503))).isTrue()
            assertThat(countsAsFailure(registry, TimeoutException("budget blown"))).isTrue()
        }

        @Test
        fun `should preserve an operator's own ignore list instead of replacing it`() {
            // What is tested: that the verdict is ADDITIVE on the record side. `recordException(predicate)`
            //   REPLACES the predicate the builder holds, so a naive customizer drops whatever the operator
            //   configured; the customizer reads the configured predicates first and folds them back in.
            // How success is judged: after the customizer both the operator's exception and a benign 4xx are
            //   discounted, and an unrelated failure is still recorded.
            // Why it matters: the customizer shapes a config it does not own. Clobbering the operator's list
            //   would silently re-arm the breaker against errors they deliberately excluded - visible to them
            //   only as unexplained open circuits.

            // Given: what YAML produced already ignores an exception of the operator's choosing
            val registry = CircuitBreakerRegistry.ofDefaults()
            registry.circuitBreaker(
                alias,
                CircuitBreakerConfig.custom().ignoreExceptions(IllegalStateException::class.java).build(),
            )

            // When
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // Then
            assertThat(ignores(registry, IllegalStateException("operator's choice"))).isTrue()
            assertThat(countsAsFailure(registry, response(404))).isFalse()
            assertThat(countsAsFailure(registry, response(500))).isTrue()
        }

        @Test
        fun `should customize every configured mesh backend`() {
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
            assertThat(countsAsFailure(registry, response(404))).isFalse()
            assertThat(countsAsFailure(registry, response(404), alias = "api-example-com-POST-payments")).isFalse()
        }

        @Test
        fun `should install the rule even when the mesh circuit-breaker toggle is off`() {
            // The toggle governs only whether mesh DERIVES the slow-call knobs; the breaker is in the call path
            // unconditionally. Gating this verdict on it would leave the invariant unmet in exactly the default
            // configuration - the one that ships.

            // Given: mesh defaults, i.e. the toggle off
            val registry = CircuitBreakerRegistry.ofDefaults()
            val props = propsFor()
            assertThat(props.resolveAll().getValue(triplet).circuitBreakerEnabled).isFalse()

            // When
            CustomizedRegistries.circuitBreakers(registry, props)

            // Then
            assertThat(countsAsFailure(registry, response(404))).isFalse()
        }

        @Test
        fun `should keep the two verdicts apart in the one customizer`() {
            // What is tested: the one customizer per alias installs BOTH verdicts, and neither widened into a
            //   blanket ignore: a benign 4xx is a success (not recorded, not ignored), a client-side failure is
            //   ignored, and a 5xx or a 429 is recorded.
            // How success is judged: four probes, each landing in its own row of the three-verdict table.
            // Why it matters: the two verdicts answer different questions ("did the backend answer?" versus
            //   "does the call carry evidence at all?") and used to be two contributors; folding them into one
            //   customizer must not fold the answers into one.

            // Given / When
            val registry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // Then
            assertThat(countsAsFailure(registry, response(404))).isFalse()
            assertThat(ignores(registry, response(404))).`as`("a benign 4xx is a SUCCESS, not an ignore").isFalse()
            assertThat(ignores(registry, bulkheadRejection())).isTrue()
            assertThat(ignores(registry, ResponseBodyLimitExceededException("accounts", 8192, -1))).isTrue()
            assertThat(countsAsFailure(registry, response(500))).isTrue()
            assertThat(countsAsFailure(registry, response(429))).isTrue()
        }
    }

    @Nested
    inner class DecoratedPipeline {
        @Test
        fun `should not count a 404 answer as a circuit-breaker failure`() {
            // End to end through the real decorator rather than against the predicate: the breaker's error hook
            // records every error and defers the record/ignore decision to the config, so this is what proves
            // the verdict reaches the instance the decorator actually uses.

            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(registry)
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // When: the backend answers "not found"
            val failure = catchThrowable { decorator.decorate<String>(alias) { throw response(404) } }

            // Then: the caller still sees the error - the exchange did not succeed ...
            assertThat(failure).isInstanceOf(HttpClientErrorException.NotFound::class.java)
            // ... but the breaker did not hold it against the backend
            assertThat(registry.circuitBreaker(alias).metrics.numberOfFailedCalls).isZero()
        }

        @Test
        fun `should still count a 500 answer as a circuit-breaker failure`() {
            // Without this, a breaker that recorded nothing at all - the far worse defect, since it disables the
            // protection silently - would satisfy the zero-failure assertion above.

            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(registry)
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // When
            val failure = catchThrowable { decorator.decorate<String>(alias) { throw response(500) } }

            // Then
            assertThat(failure).isInstanceOf(HttpServerErrorException.InternalServerError::class.java)
            assertThat(registry.circuitBreaker(alias).metrics.numberOfFailedCalls).isEqualTo(1)
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

    /**
     * A response exception in the shape the pipeline sees it - Spring's status-specific subclass, the way
     * `ResilientRestClient` builds it from a non-2xx answer; status only, no body is needed here.
     */
    private fun response(status: Int): RestClientResponseException {
        val code = HttpStatusCode.valueOf(status)
        val text = "status $status"
        return when {
            code.is4xxClientError -> HttpClientErrorException.create(code, text, HttpHeaders(), ByteArray(0), null)
            else -> HttpServerErrorException.create(code, text, HttpHeaders(), ByteArray(0), null)
        }
    }

    /** A real rejection instance - the type is only constructible through the bulkhead's factory. */
    private fun bulkheadRejection(): BulkheadFullException = BulkheadFullException.createBulkheadFullException(Bulkhead.of("probe", BulkheadConfig.ofDefaults()))

    /** A decorator over the supplied breaker registry; the other two primitives stay out of the way. */
    private fun decoratorWith(registry: CircuitBreakerRegistry): BlockingResilienceDecorator =
        BlockingResilienceDecorator(
            registry,
            TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(Duration.ofMinutes(5)).build()),
            BulkheadRegistry.ofDefaults(),
        ).also { decorators += it }

    /** Props for the single accounts endpoint, mesh defaults otherwise. */
    private fun propsFor(): MeshProperties =
        MeshProperties(
            defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
            endpoints = listOf(MeshProperties.EndpointConfig(triplet)),
        )
}
