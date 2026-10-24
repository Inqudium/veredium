package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import eu.inqudium.veredium.testsupport.CustomizedRegistries
import eu.inqudium.veredium.testsupport.ignores
import eu.inqudium.veredium.testsupport.singlePermitBulkheads
import io.github.resilience4j.bulkhead.BulkheadFullException
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.client.RestClientResponseException
import java.io.IOException
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference

/**
 * Tests for [RestClientVerdicts] - the blocking stack's reading of the shared breaker verdicts - and for the
 * invariant they protect in [BlockingResilienceDecorator]: a failure this pipeline raises ITSELF must never be
 * charged to the backend, and a benign 4xx must count as the backend answering.
 *
 * The RULES are pinned once, in veredium-core (`ClientSideFailurePolicyTest`, `HttpExchangeErrorPolicyTest`);
 * what is pinned here is what only this module can know: that the verdicts reach the instance the real
 * decorator uses, and that they match the exact exception SHAPES this stack produces - Spring's status-specific
 * subclasses for an answer, the `ResourceAccessException` wrapper around an I/O failure, the four client-side
 * failures of the thread hand-off.
 *
 * The contention in the bulkhead test is produced deterministically (a permit held by a call parked on a latch)
 * rather than by racing threads: counting a rejection is a heisenbug that never shows up in a single-threaded
 * happy-path test.
 */
class RestClientVerdictsTest {
    private val alias = "api-example-com-GET-accounts"
    private val triplet = "/endpoint/api.example.com/GET/accounts/{accountId}"

    private val decorators = mutableListOf<BlockingResilienceDecorator>()
    private val callers: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()

    @AfterEach
    fun tearDown() {
        callers.shutdownNow()
        decorators.forEach { it.shutdown() }
    }

    @Nested
    inner class `A client-side failure` {
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
            //
            // Given: a single-permit bulkhead that rejects immediately, the customizer has run, and the only
            // permit is held by a call that has not completed
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(circuitBreakerRegistry, singlePermitBulkheads())
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor(), RestClientVerdicts.breakerVerdicts)
            val circuitBreaker = circuitBreakerRegistry.circuitBreaker(alias)
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            val holding =
                callers.submit<String> {
                    decorator.decorate(alias) {
                        entered.countDown()
                        release.await()
                        "held"
                    }
                }
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()

            try {
                // When: a second call is made
                val rejection = catchThrowable { decorator.decorate<String>(alias) { "second" } }

                // Then: it was rejected by the bulkhead ...
                assertThat(rejection).isInstanceOf(BulkheadFullException::class.java)
                // ... and the breaker did not hold that against the backend
                assertThat(circuitBreaker.metrics.numberOfFailedCalls).isZero()
            } finally {
                release.countDown()
                holding.get(5, TimeUnit.SECONDS)
            }
        }

        @Test
        fun `should not count a call refused by a shut-down decorator as a circuit-breaker failure`() {
            // What is tested: the fourth client-side failure, through the real decorator - a call made after the
            //   decorator's executor was shut down (context teardown) is refused with a RejectedExecutionException,
            //   and the breaker must not record that refusal as a failure of the backend.
            // Success criteria: the call ends in a RejectedExecutionException (so the assertion below is not
            //   vacuous) while the breaker's failed-call counter stays at zero.
            // Why it matters: the submission happens INSIDE the callable the breaker wraps (so a short-circuited
            //   call submits nothing), which puts the refusal on the breaker's path like a transport fault;
            //   without this entry the breaker would charge the backend for the process shutting down.
            //
            // Given: a decorator whose executor is already shut down, and the customizer applied
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(circuitBreakerRegistry, BulkheadRegistry.ofDefaults())
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor(), RestClientVerdicts.breakerVerdicts)
            val circuitBreaker = circuitBreakerRegistry.circuitBreaker(alias)
            decorator.shutdown()

            // When
            val refusal = catchThrowable { decorator.decorate<String>(alias) { "too late" } }

            // Then: the executor refused the call ...
            assertThat(refusal).isInstanceOf(RejectedExecutionException::class.java)
            // ... and the breaker did not hold that against the backend
            assertThat(circuitBreaker.metrics.numberOfFailedCalls).isZero()
        }

        @Test
        fun `should not count an interrupted caller as a circuit-breaker failure`() {
            // What is tested: the third client-side failure, through the real decorator - a caller that stops
            //   waiting (its own deadline, a cancelled scope) gets an InterruptedException, and the breaker must
            //   not record that as a failure of the backend.
            // Success criteria: the interrupted caller's call ends in an InterruptedException (so the
            //   assertion below is not vacuous) while the breaker's failed-call counter stays at zero; the
            //   worker it abandoned is cancelled, as the decorator's own tests pin.
            // Why it matters: the reactive twin's breaker never sees a cancellation - a cancelled subscription
            //   carries no error. On the blocking stack the cancellation IS an exception out of the decorated
            //   call, and `CircuitBreaker.decorateCallable` records every Exception, so a host that cancels
            //   callers routinely - a short deadline upstream, a shutting-down executor - would open the circuit
            //   against a backend that was never given the time to answer.
            //
            // Given: a call parked on the worker, a caller waiting on it, and the customizer applied
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(circuitBreakerRegistry, BulkheadRegistry.ofDefaults())
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor(), RestClientVerdicts.breakerVerdicts)
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            val callerThread = AtomicReference<Thread>()
            val outcome =
                callers.submit<Throwable?> {
                    callerThread.set(Thread.currentThread())
                    catchThrowable {
                        decorator.decorate<String>(alias) {
                            entered.countDown()
                            release.await()
                            "never"
                        }
                    }
                }
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()

            try {
                // When: the caller is interrupted while waiting
                callerThread.get().interrupt()

                // Then: the caller sees the interruption ...
                assertThat(outcome.get(5, TimeUnit.SECONDS)).isInstanceOf(InterruptedException::class.java)
                // ... and the breaker did not hold it against the backend
                assertThat(circuitBreakerRegistry.circuitBreaker(alias).metrics.numberOfFailedCalls).isZero()
            } finally {
                release.countDown()
            }
        }

        @Test
        fun `should still count a genuine backend error as a circuit-breaker failure`() {
            // What is tested: the counterpart that keeps the tests above honest - the customized config must ignore
            //   ONLY the client-side failures, not disable failure recording as such.
            // Success criteria: a plain backend error through the same decorated pipeline advances the
            //   failed-call counter to exactly one.
            // Why it matters: without this, a breaker that recorded nothing at all - the far worse defect, since
            //   it disables the protection silently - would satisfy the zero-failure assertion above.
            //
            // Given: the same wiring, with the customizer applied
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(circuitBreakerRegistry, singlePermitBulkheads())
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor(), RestClientVerdicts.breakerVerdicts)
            val circuitBreaker = circuitBreakerRegistry.circuitBreaker(alias)

            // When: a call fails for a real reason
            val failure = catchThrowable { decorator.decorate<String>(alias) { throw IllegalStateException("backend exploded") } }

            // Then: the error surfaces and the breaker counts it
            assertThat(failure).isInstanceOf(IllegalStateException::class.java)
            assertThat(circuitBreaker.metrics.numberOfFailedCalls).isEqualTo(1)
        }

        @Test
        fun `should ignore a buffer-limit failure bare and when it arrives wrapped`() {
            // What is tested: that the buffer-limit failure is discounted in the shape the exchange function
            //   throws it (bare) AND in the shape a wrapper gives it.
            // Success criteria: the customized breaker ignores both the bare and the wrapped exception, and the
            //   policy recognises the wrapped one as a buffer-limit failure.
            // Why it matters: Spring's blocking client wraps every IOException of an exchange into a
            //   ResourceAccessException, and such a wrapping turns a class-based ignore into a silent no-op -
            //   present in the code, visible in review, doing nothing: Resilience4j's ignoreExceptions(Class...)
            //   matches the THROWN type only. Only walking the cause chain survives a wrapper, so the walk is
            //   pinned here against the wrapper the client uses; an ignore that matched the bare type only would
            //   charge the backend for this client's own refusal the moment a wrapper appeared - and nothing
            //   would show it.
            //
            // Given
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor(), RestClientVerdicts.breakerVerdicts)

            // When: the failure bare, and behind the client's own wrapper
            val bare = ResponseBodyLimitExceededException("accounts", 8192, 20_000)
            val wrapped = ResourceAccessException("I/O error on GET request", IOException("body too large", bare))

            // Then
            assertThat(ignores(circuitBreakerRegistry, bare, alias)).isTrue()
            assertThat(ignores(circuitBreakerRegistry, wrapped, alias)).isTrue()
            assertThat(RestClientVerdicts.clientSideFailure.isBufferLimitFailure(wrapped)).isTrue()
        }

        @Test
        fun `should still record a genuine backend failure that carries a large error body`() {
            // What is tested: the boundary of the cause walk - that discounting buffer failures does not leak
            //   into discounting real ones.
            // Success criteria: a 5xx is recorded, including one whose error body is large. The client's
            //   reader TRUNCATES an error body rather than rejecting it (BoundedBodyReaderTest), so such a
            //   response yields a plain HttpServerErrorException with NO limit exception underneath.
            // Why it matters: this is the failure mode that would make the whole breaker useless - if a 5xx
            //   ever started carrying a buffer failure as its cause, the cause walk would silently discount
            //   genuine outages. Nothing in the code prevents that; this test is what would catch it.
            //
            // Given
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor(), RestClientVerdicts.breakerVerdicts)

            // When / Then: a server error stays a server error
            val serverError =
                HttpServerErrorException.create(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "Internal Server Error",
                    HttpHeaders(),
                    ByteArray(64 * 1024),
                    null,
                )
            assertThat(ignores(circuitBreakerRegistry, serverError, alias)).isFalse()
            assertThat(ignores(circuitBreakerRegistry, TimeoutException("budget blown"), alias)).isFalse()
        }
    }

    @Nested
    inner class `An expected client error` {
        @Test
        fun `should read Spring's status-specific subclasses as the client errors they are`() {
            // What is tested: the one thing this stack supplies to the shared rule - the status reader - against
            //   the exact types the client throws: HttpClientErrorException.<Status> for a 4xx,
            //   HttpServerErrorException.<Status> for a 5xx, both subclasses of RestClientResponseException.
            // Success criteria: a benign 4xx is expected, 408/429 and every 5xx are not, and a throwable
            //   that carries no status is not either.
            // Why it matters: a reader keyed on the wrong type would make every 4xx an operational fault again -
            //   the WARN-with-stack-trace noise and the breaker charging business cases to the backend that the
            //   policy exists to end.
            //
            // Given / When / Then: a truth table over the shapes the client throws
            val policy = RestClientVerdicts.httpExchangeError
            assertThat(policy.isExpectedClientError(response(404))).isTrue()
            assertThat(policy.isExpectedClientError(response(400))).isTrue()
            assertThat(policy.isExpectedClientError(response(408))).isFalse()
            assertThat(policy.isExpectedClientError(response(429))).isFalse()
            assertThat(policy.isExpectedClientError(response(500))).isFalse()
            assertThat(policy.isExpectedClientError(ResourceAccessException("connect timed out"))).isFalse()
            assertThat(policy.isExpectedClientError(TimeoutException("budget blown"))).isFalse()
        }

        @Test
        fun `should not count a 404 answer as a circuit-breaker failure`() {
            // What is tested: a 404 through the real decorator and the customized breaker, rather than against
            //   the predicate alone.
            // Success criteria: the caller still gets `HttpClientErrorException.NotFound`, and the breaker's
            //   failed-call counter stays at zero.
            // Why it matters: the breaker's error hook records every error and defers the record/ignore decision
            //   to the config, so this is what proves the verdict reaches the instance the decorator actually
            //   uses - a 404-heavy lookup must not open the circuit.
            //
            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(registry, BulkheadRegistry.ofDefaults())
            CustomizedRegistries.circuitBreakers(registry, propsFor(), RestClientVerdicts.breakerVerdicts)

            // When: the backend answers "not found"
            val failure = catchThrowable { decorator.decorate<String>(alias) { throw response(404) } }

            // Then: the caller still sees the error - the exchange did not succeed ...
            assertThat(failure).isInstanceOf(HttpClientErrorException.NotFound::class.java)
            // ... but the breaker did not hold it against the backend
            assertThat(registry.circuitBreaker(alias).metrics.numberOfFailedCalls).isZero()
        }

        @Test
        fun `should still count a 500 answer as a circuit-breaker failure`() {
            // What is tested: a 500 through the same decorator and customized breaker.
            // Success criteria: the caller gets the server-error exception and the failed-call counter is one.
            // Why it matters: without this, a breaker that recorded nothing at all - the far worse defect, since
            //   it disables the protection silently - would satisfy the zero-failure assertion above.
            //
            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(registry, BulkheadRegistry.ofDefaults())
            CustomizedRegistries.circuitBreakers(registry, propsFor(), RestClientVerdicts.breakerVerdicts)

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

    /**
     * A decorator over the supplied registries. The time limiter is deliberately generous: these tests hold a
     * permit open on purpose, and a short budget would cancel that call and free the permit mid-test, turning a
     * deterministic rejection into a race.
     */
    private fun decoratorWith(
        circuitBreakerRegistry: CircuitBreakerRegistry,
        bulkheadRegistry: BulkheadRegistry,
    ): BlockingResilienceDecorator =
        BlockingResilienceDecorator(
            circuitBreakerRegistry,
            TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(Duration.ofMinutes(5)).build()),
            bulkheadRegistry,
        ).also { decorators += it }

    /** Props for the single accounts endpoint, mesh defaults otherwise. */
    private fun propsFor(): MeshProperties =
        MeshProperties(
            defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
            endpoints = listOf(MeshProperties.EndpointConfig(triplet)),
        )
}
