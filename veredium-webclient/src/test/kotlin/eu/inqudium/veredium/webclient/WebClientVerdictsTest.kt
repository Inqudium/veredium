package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadFullException
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.core.io.buffer.DataBufferLimitException
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.time.Duration
import java.util.concurrent.TimeoutException

/**
 * Tests for [WebClientVerdicts] - the reactive stack's reading of the shared breaker verdicts - and for the
 * invariant they protect in [ReactiveResilienceDecorator]: a failure this pipeline raises ITSELF must never be
 * charged to the backend, and a benign 4xx must count as the backend answering.
 *
 * The RULES are pinned once, in veredium-core (`ClientSideFailurePolicyTest`, `HttpExchangeErrorPolicyTest`);
 * what is pinned here is what only this module can know: that the verdicts reach the instance the real
 * decorator uses, and that they match the exact exception SHAPES this stack produces - a
 * `WebClientResponseException` for an answer, and the codec's `DataBufferLimitException` arriving wrapped in a
 * `WebClientResponseException` that reports 200.
 *
 * The contention in the bulkhead test is produced deterministically (a permit held by an uncompleted source)
 * rather than by racing threads: counting a rejection is a heisenbug that never shows up in a single-threaded
 * happy-path test.
 */
class WebClientVerdictsTest {
    private val alias = "api-example-com-GET-accounts"
    private val triplet = "/endpoint/api.example.com/GET/accounts/{accountId}"

    @Nested
    inner class ClientSideFailure {
        @Test
        fun `should not count a bulkhead rejection as a circuit-breaker failure`() {
            // What is tested: end to end through the real decorator - when the bulkhead rejects a call because
            //   its only permit is taken, the circuit breaker must not record that rejection as a failure.
            // Success criteria: the second call fails with a BulkheadFullException (proving the rejection
            //   really happened, so the assertion below is not vacuous) while the breaker's failed-call counter
            //   stays at zero.
            // Why it matters: this is the escalation path. Counting the rejection means a transient concurrency
            //   peak opens the breaker and takes the backend down for waitDurationInOpenState - the load
            //   shedding meant to absorb the peak amplifies it into an outage instead.

            // Given: a single-permit bulkhead that rejects immediately, and the customizer has run
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(circuitBreakerRegistry, singlePermitBulkheads())
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())
            val circuitBreaker = circuitBreakerRegistry.circuitBreaker(alias)

            // And: the only permit is held by a call that has not completed
            val held = Sinks.one<String>()
            val holding = decorator.decorate(alias, held.asMono()).subscribe({ }, { })

            try {
                // When: a second call is decorated and subscribed
                val rejection = catchThrowable { decorator.decorate(alias, Mono.just("second")).block() }

                // Then: it was rejected by the bulkhead ...
                assertThat(rejection).isInstanceOf(BulkheadFullException::class.java)
                // ... and the breaker did not hold that against the backend
                assertThat(circuitBreaker.metrics.numberOfFailedCalls).isZero()
            } finally {
                holding.dispose()
                held.tryEmitEmpty()
            }
        }

        @Test
        fun `should still count a genuine backend error as a circuit-breaker failure`() {
            // What is tested: the counterpart that keeps the test above honest - the customized config must ignore
            //   ONLY the client-side failures, not disable failure recording as such.
            // Success criteria: a plain backend error through the same decorated pipeline advances the
            //   failed-call counter to exactly one.
            // Why it matters: without this, a breaker that recorded nothing at all - the far worse defect, since
            //   it disables the protection silently - would satisfy the zero-failure assertion above.

            // Given: the same wiring, with the customizer applied
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(circuitBreakerRegistry, singlePermitBulkheads())
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())
            val circuitBreaker = circuitBreakerRegistry.circuitBreaker(alias)

            // When: a call fails for a real reason
            val failure =
                catchThrowable {
                    decorator.decorate(alias, Mono.error<String>(IllegalStateException("backend exploded"))).block()
                }

            // Then: the error surfaces and the breaker counts it
            assertThat(failure).isInstanceOf(IllegalStateException::class.java)
            assertThat(circuitBreaker.metrics.numberOfFailedCalls).isEqualTo(1)
        }

        @Test
        fun `should ignore a codec buffer-limit failure even though it arrives wrapped as a 200 response`() {
            // What is tested: that the buffer-limit failure is discounted in the shape it ACTUALLY arrives in.
            // Success criteria: a WebClientResponseException carrying a DataBufferLimitException as its
            //   CAUSE is ignored - not only the bare exception, which is not what the pipeline ever sees.
            // Why it matters: Spring wraps a codec failure on an otherwise successful response into a
            //   WebClientResponseException reporting 200 OK, with the real cause underneath. Resilience4j's
            //   ignoreExceptions(Class...) matches the THROWN type only, so listing DataBufferLimitException
            //   there would have been a silent no-op - present in the code, visible in review, doing nothing.
            //   Only walking the cause chain reaches it.
            //
            // Given
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())

            // When: the failure in the exact shape the client produces it
            val wrapped =
                WebClientResponseException(200, "OK", null, null, null)
                    .initCause(DataBufferLimitException("Exceeded limit on max bytes to buffer : 8192")) as Throwable

            // Then
            assertThat(ignores(circuitBreakerRegistry, wrapped)).isTrue()
            assertThat(WebClientVerdicts.clientSideFailure.isBufferLimitFailure(wrapped)).isTrue()
            // ... and the bare form too, so the rule does not depend on the wrapping staying as it is
            assertThat(ignores(circuitBreakerRegistry, DataBufferLimitException("too big"))).isTrue()
            // ... and a 200-reporting response exception is never read as a benign 4xx
            assertThat(WebClientVerdicts.httpExchangeError.isExpectedClientError(wrapped)).isFalse()
        }

        @Test
        fun `should still record a genuine backend failure that carries a large error body`() {
            // What is tested: the boundary of the cause walk - that discounting buffer failures does not leak
            //   into discounting real ones.
            // Success criteria: a 5xx is recorded, including one whose error body is large. Measured
            //   beforehand: such a response yields a plain WebClientResponseException with NO
            //   DataBufferLimitException underneath, because Spring does not fail on an oversized error body.
            // Why it matters: this is the failure mode that would make the whole breaker useless - if a 5xx
            //   ever started carrying a buffer failure as its cause, the cause walk would silently discount
            //   genuine outages. Nothing in the code prevents that; this test is what would catch it.
            //
            // Given
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())

            // When / Then: a server error stays a server error
            val serverError = WebClientResponseException(500, "Internal Server Error", null, ByteArray(64 * 1024), null)
            assertThat(ignores(circuitBreakerRegistry, serverError)).isFalse()
            assertThat(ignores(circuitBreakerRegistry, TimeoutException("budget blown"))).isFalse()
        }
    }

    @Nested
    inner class ExpectedClientError {
        @Test
        fun `should read a WebClientResponseException as the client error it reports`() {
            // What is tested: the one thing this stack supplies to the shared rule - the status reader - against
            //   the exact type WebClient raises for a non-2xx answer.
            // Success criteria: a benign 4xx is expected, 408/429 and every 5xx are not, and a throwable
            //   that carries no status is not either.
            // Why it matters: a reader keyed on the wrong type would make every 4xx an operational fault again -
            //   the WARN-with-stack-trace noise and the breaker charging business cases to the backend that the
            //   policy exists to end.
            //
            // Given
            val policy = WebClientVerdicts.httpExchangeError

            // When / Then
            assertThat(policy.isExpectedClientError(response(404))).isTrue()
            assertThat(policy.isExpectedClientError(response(400))).isTrue()
            assertThat(policy.isExpectedClientError(response(408))).isFalse()
            assertThat(policy.isExpectedClientError(response(429))).isFalse()
            assertThat(policy.isExpectedClientError(response(500))).isFalse()
            assertThat(policy.isExpectedClientError(TimeoutException("budget blown"))).isFalse()
        }

        @Test
        fun `should not count a 404 answer as a circuit-breaker failure`() {
            // What is tested: the benign-4xx verdict end to end through the real decorator rather than against
            //   the predicate.
            // Success criteria: a 404 in WebClient's shape still fails the call for the caller, while the
            //   breaker's failed-call counter stays at zero.
            // Why it matters: the breaker's error hook records every error and defers the record/ignore decision
            //   to the config, so this is what proves the verdict reaches the instance the decorator actually
            //   uses - not merely a predicate that would be right if it were installed.
            //

            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(registry, BulkheadRegistry.ofDefaults())
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // When: the backend answers "not found"
            val failure = catchThrowable { decorator.decorate(alias, Mono.error<String>(response(404))).block() }

            // Then: the caller still sees the error - the exchange did not succeed ...
            assertThat(failure).isInstanceOf(WebClientResponseException::class.java)
            // ... but the breaker did not hold it against the backend
            assertThat(registry.circuitBreaker(alias).metrics.numberOfFailedCalls).isZero()
        }

        @Test
        fun `should still count a 500 answer as a circuit-breaker failure`() {
            // What is tested: the counterpart of the 404 case through the same decorated pipeline.
            // Success criteria: a 500 fails the call and advances the failed-call counter to exactly one.
            // Why it matters: without this, a breaker that recorded nothing at all - the far worse defect, since
            //   it disables the protection silently - would satisfy the zero-failure assertion above.
            //

            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(registry, BulkheadRegistry.ofDefaults())
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // When
            val failure = catchThrowable { decorator.decorate(alias, Mono.error<String>(response(500))).block() }

            // Then
            assertThat(failure).isInstanceOf(WebClientResponseException::class.java)
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
    ): Boolean =
        registry
            .circuitBreaker(alias)
            .circuitBreakerConfig.ignoreExceptionPredicate
            .test(error)

    /** A response exception in the shape the pipeline sees it; status only, no body is needed here. */
    private fun response(status: Int): WebClientResponseException = WebClientResponseException(status, "status $status", null, null, null)

    /** A registry whose bulkheads hold exactly one permit and reject the next caller without waiting. */
    private fun singlePermitBulkheads(): BulkheadRegistry =
        BulkheadRegistry.of(
            BulkheadConfig
                .custom()
                .maxConcurrentCalls(1)
                .maxWaitDuration(Duration.ZERO)
                .build(),
        )

    /**
     * A decorator over the supplied registries. The time limiter is deliberately generous: these tests hold a
     * permit open on purpose, and a short budget would cancel that call and free the permit mid-test, turning a
     * deterministic rejection into a race.
     */
    private fun decoratorWith(
        circuitBreakerRegistry: CircuitBreakerRegistry,
        bulkheadRegistry: BulkheadRegistry,
    ): ReactiveResilienceDecorator =
        ReactiveResilienceDecorator(
            circuitBreakerRegistry,
            TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(Duration.ofMinutes(5)).build()),
            bulkheadRegistry,
        )

    /** Props for the single accounts endpoint, mesh defaults otherwise. */
    private fun propsFor(): MeshProperties =
        MeshProperties(
            defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
            endpoints = listOf(MeshProperties.EndpointConfig(triplet)),
        )
}
