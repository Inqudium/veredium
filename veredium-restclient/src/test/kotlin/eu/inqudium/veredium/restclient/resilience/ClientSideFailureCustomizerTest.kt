package eu.inqudium.veredium.restclient.resilience

import eu.inqudium.veredium.restclient.BlockingResilienceDecorator
import eu.inqudium.veredium.restclient.ClientSideFailurePolicy
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
import org.junit.jupiter.api.Timeout
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.client.ResourceAccessException
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
 * Tests for the client-side-failure verdict the circuit-breaker customizer installs, and the invariant it
 * protects in [BlockingResilienceDecorator]: a failure this pipeline raises ITSELF must never be charged to the
 * backend.
 *
 * `BulkheadFullException` is the currently registered case and the one exercised here. The decorator composes
 * `CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )`, so a rejection travels outwards through the breaker's
 * error hook, which records every error and defers the record/ignore decision to the config. Resilience4j's
 * default ignore list is empty, so without the customizer a concurrency peak - the backend perfectly healthy,
 * indeed never even called - drives the failure rate up and eventually opens the breaker, turning load
 * shedding into an outage. That is a heisenbug: it needs real contention and never shows up in a
 * single-threaded happy-path test, which is why the contention here is produced deterministically (a permit
 * held by a call parked on a latch) rather than by racing threads.
 *
 * The tests deliberately assert the customizer's BEHAVIOUR (which exceptions the breaker ends up ignoring),
 * not the contents of `CLIENT_SIDE_FAILURES`, so extending that list stays a one-line change and does not
 * drag a test rewrite behind it. The customizer is applied through [CustomizedRegistries], the way the Boot
 * configuration applies it while building each instance.
 */
class ClientSideFailureCustomizerTest {
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
    inner class DecoratedPipeline {
        @Test
        fun `should not count a bulkhead rejection as a circuit-breaker failure`() {
            // What is tested: end to end through the real decorator - when the bulkhead rejects a call because
            //   its only permit is taken, the circuit breaker must not record that rejection as a failure.
            // How success is judged: the second call fails with a BulkheadFullException (proving the rejection
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
                val rejection = catchThrowable { decorator.decorate(alias) { "second" } }

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
            // How success is judged: the call ends in a RejectedExecutionException (so the assertion below is not
            //   vacuous) while the breaker's failed-call counter stays at zero.
            // Why it matters: the submission happens INSIDE the callable the breaker wraps (so a short-circuited
            //   call submits nothing), which puts the refusal on the breaker's path like a transport fault;
            //   without this entry the breaker would charge the backend for the process shutting down.

            // Given: a decorator whose executor is already shut down, and the customizer applied
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(circuitBreakerRegistry, BulkheadRegistry.ofDefaults())
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())
            val circuitBreaker = circuitBreakerRegistry.circuitBreaker(alias)
            decorator.shutdown()

            // When
            val refusal = catchThrowable { decorator.decorate(alias) { "too late" } }

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
            // How success is judged: the interrupted caller's call ends in an InterruptedException (so the
            //   assertion below is not vacuous) while the breaker's failed-call counter stays at zero; the
            //   worker it abandoned is cancelled, as the decorator's own tests pin.
            // Why it matters: the reactive twin's breaker never sees a cancellation - a cancelled subscription
            //   carries no error. On the blocking stack the cancellation IS an exception out of the decorated
            //   call, and `CircuitBreaker.decorateCallable` records every Exception, so a host that cancels
            //   callers routinely - a short deadline upstream, a shutting-down executor - would open the circuit
            //   against a backend that was never given the time to answer.

            // Given: a call parked on the worker, a caller waiting on it, and the customizer applied
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(circuitBreakerRegistry, BulkheadRegistry.ofDefaults())
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            val callerThread = AtomicReference<Thread>()
            val outcome =
                callers.submit<Throwable?> {
                    callerThread.set(Thread.currentThread())
                    catchThrowable {
                        decorator.decorate(alias) {
                            entered.countDown()
                            release.await()
                            "never"
                        }
                    }
                }
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()

            // When: the caller is interrupted while waiting
            callerThread.get().interrupt()

            // Then: the caller sees the interruption ...
            assertThat(outcome.get(5, TimeUnit.SECONDS)).isInstanceOf(InterruptedException::class.java)
            // ... and the breaker did not hold it against the backend
            assertThat(circuitBreakerRegistry.circuitBreaker(alias).metrics.numberOfFailedCalls).isZero()
            release.countDown()
        }

        @Test
        fun `should still count a genuine backend error as a circuit-breaker failure`() {
            // What is tested: the counterpart that keeps the test above honest - the customized config must ignore
            //   ONLY the client-side failures, not disable failure recording as such.
            // How success is judged: a plain backend error through the same decorated pipeline advances the
            //   failed-call counter to exactly one.
            // Why it matters: without this, a breaker that recorded nothing at all - the far worse defect, since
            //   it disables the protection silently - would satisfy the zero-failure assertion above.

            // Given: the same wiring, with the customizer applied
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(circuitBreakerRegistry, singlePermitBulkheads())
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())
            val circuitBreaker = circuitBreakerRegistry.circuitBreaker(alias)

            // When: a call fails for a real reason
            val failure = catchThrowable { decorator.decorate<String>(alias) { throw IllegalStateException("backend exploded") } }

            // Then: the error surfaces and the breaker counts it
            assertThat(failure).isInstanceOf(IllegalStateException::class.java)
            assertThat(circuitBreaker.metrics.numberOfFailedCalls).isEqualTo(1)
        }
    }

    @Nested
    inner class Customization {
        @Test
        fun `should install the ignore even when the mesh circuit-breaker toggle is off`() {
            // What is tested: that the verdict is NOT gated on ResolvedTimeouts.circuitBreakerEnabled.
            // How success is judged: with the toggle at its default (off), the resulting config still ignores a
            //   BulkheadFullException.
            // Why it matters: the toggle governs only whether mesh DERIVES the slow-call knobs, while the breaker
            //   is in the call path unconditionally. Gating this on the toggle would leave the invariant unmet in
            //   exactly the default configuration - the one that actually ships.

            // Given: mesh defaults, i.e. the circuit-breaker toggle off
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val props = propsFor()
            assertThat(props.resolveAll().getValue(triplet).circuitBreakerEnabled).isFalse()

            // When
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, props)

            // Then
            assertThat(ignores(circuitBreakerRegistry, alias, bulkheadRejection())).isTrue()
        }

        @Test
        fun `should preserve an operator's own ignore list instead of replacing it`() {
            // What is tested: that the verdict is ADDITIVE. `ignoreException(predicate)` REPLACES the predicate
            //   the builder holds - which is what the operator's `ignore-exceptions` became - so a naive
            //   customizer would silently drop it; the customizer reads the configured predicate first and ORs
            //   it back in.
            // How success is judged: after the customizer the config ignores BOTH the operator's exception and
            //   the bulkhead rejection, and still records an unrelated one.
            // Why it matters: the customizer shapes a config it does not own. Clobbering an operator's ignore
            //   list would silently re-arm the breaker against errors they had deliberately excluded - a
            //   regression they would only notice as unexplained open circuits.

            // Given: what YAML produced already ignores an exception of the operator's choosing
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            circuitBreakerRegistry.circuitBreaker(
                alias,
                CircuitBreakerConfig.custom().ignoreExceptions(IllegalStateException::class.java).build(),
            )

            // When
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())

            // Then: both are ignored - as is the caller's interruption - and an unrelated exception is still recorded
            assertThat(ignores(circuitBreakerRegistry, alias, IllegalStateException("operator's choice"))).isTrue()
            assertThat(ignores(circuitBreakerRegistry, alias, bulkheadRejection())).isTrue()
            assertThat(ignores(circuitBreakerRegistry, alias, InterruptedException("caller gave up"))).isTrue()
            assertThat(ignores(circuitBreakerRegistry, alias, TimeoutException("budget blown"))).isFalse()
        }

        @Test
        fun `should ignore a buffer-limit failure bare and when it arrives wrapped`() {
            // What is tested: that the buffer-limit failure is discounted in the shape it arrives in today (thrown
            //   bare out of the exchange function) AND in the shape a wrapper would give it.
            // Why the second shape: Spring's blocking client wraps every IOException of an exchange into a
            //   ResourceAccessException, and the reactive twin saw exactly such a wrapping turn a class-based
            //   ignore into a silent no-op - present in the code, visible in review, doing nothing. Resilience4j's
            //   ignoreExceptions(Class...) matches the THROWN type only; only walking the cause chain survives a
            //   wrapper, so the walk is pinned here against the wrapper the client would use.
            //
            // Given
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())

            // When: the failure bare, and behind the client's own wrapper
            val bare = ResponseBodyLimitExceededException("accounts", 8192, 20_000)
            val wrapped = ResourceAccessException("I/O error on GET request", IOException("body too large", bare))

            // Then
            assertThat(ignores(circuitBreakerRegistry, alias, bare)).isTrue()
            assertThat(ignores(circuitBreakerRegistry, alias, wrapped)).isTrue()
        }

        @Test
        fun `should still record a genuine backend failure that carries a large error body`() {
            // What is tested: the boundary of the cause walk - that discounting buffer failures does not leak
            //   into discounting real ones.
            // How success is judged: a 5xx is recorded, including one whose error body is large. The client's
            //   reader TRUNCATES an error body rather than rejecting it (BoundedBodyReaderTest), so such a
            //   response yields a plain HttpServerErrorException with NO limit exception underneath.
            // Why it matters: this is the failure mode that would make the whole breaker useless - if a 5xx
            //   ever started carrying a buffer failure as its cause, the cause walk would silently discount
            //   genuine outages. Nothing in the code prevents that; this test is what would catch it.
            //
            // Given
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())

            // When / Then: a server error stays a server error
            val serverError =
                HttpServerErrorException.create(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "Internal Server Error",
                    HttpHeaders(),
                    ByteArray(64 * 1024),
                    null,
                )
            assertThat(ignores(circuitBreakerRegistry, alias, serverError)).isFalse()
            assertThat(ignores(circuitBreakerRegistry, alias, TimeoutException("budget blown"))).isFalse()
        }

        @Test
        fun `should customize every configured mesh backend`() {
            // Given: two endpoints, so there is one customizer per alias rather than a single one
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val secondTriplet = "/endpoint/api.example.com/POST/payments"
            val props =
                MeshProperties(
                    defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(triplet),
                            MeshProperties.EndpointConfig(secondTriplet),
                        ),
                )

            // When
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, props)

            // Then: every derived alias carries the ignore
            assertThat(ignores(circuitBreakerRegistry, alias, bulkheadRejection())).isTrue()
            assertThat(ignores(circuitBreakerRegistry, "api-example-com-POST-payments", bulkheadRejection())).isTrue()
        }

        @Test
        fun `should carry the derived slow-call knobs and the ignore in the one customizer`() {
            // What is tested: that the verdict and the mesh derivation do not compete - they are ONE customizer
            //   per alias (Resilience4j admits exactly one per instance name), so there is no order in which one
            //   could undo the other.
            // How success is judged: with the circuit-breaker derivation enabled, the config carries the derived
            //   duration (800ms at individual 1000, maxRetries 0), the 100% pin of the inversion case, AND the
            //   ignore.
            // Why it matters: the former two-registrar design needed order-independence tests for exactly this;
            //   with one customizer the property holds by construction, and this test is what says so.

            // Given: circuit-breaker derivation enabled
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()

            // When
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor(circuitBreakerEnabled = true))

            // Then
            val config = circuitBreakerRegistry.circuitBreaker(alias).circuitBreakerConfig
            assertThat(config.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(800))
            assertThat(config.slowCallRateThreshold).isEqualTo(100f)
            assertThat(config.ignoreExceptionPredicate.test(bulkheadRejection())).isTrue()
        }
    }

    @Nested
    inner class CauseChainBoundary {
        // The cause walk in ClientSideFailurePolicy is bounded by MAX_CAUSE_DEPTH "so a pathological cause
        // chain cannot spin". Both ends of that bound are asserted here, through the customized instance's
        // predicate rather than the policy function, so these stay statements about the shipped behaviour.

        @Test
        fun `should still discount a client-side failure at the deepest position the walk reaches`() {
            // The walk inspects MAX_CAUSE_DEPTH candidates, the thrown exception being the first. A root cause
            // at exactly that last position must still be found - otherwise the effective depth is one less
            // than the constant says, and nobody would notice.
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())

            val atLimit = nested(ClientSideFailurePolicy.MAX_CAUSE_DEPTH - 1, ResponseBodyLimitExceededException("accounts", 8192, -1))

            assertThat(ignores(circuitBreakerRegistry, alias, atLimit)).isTrue()
        }

        @Test
        fun `should stop looking one layer beyond the bound`() {
            // What is tested: that the bound is real - a client-side failure buried DEEPER than the walk goes is
            //   not found, and the failure is therefore recorded.
            // How success is judged: the same chain as above with one more wrapper is NOT ignored.
            // Why it matters: this documents the accepted limitation rather than leaving it to be discovered.
            //   Discounting is the exception to normal breaker behaviour, so falling back to "record it" past
            //   the bound is the safe direction - a breaker that counts a client-side failure trips slightly
            //   early, whereas an unbounded walk on a cyclic chain would hang the predicate inside the
            //   breaker's error hook, on a request-serving thread.
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())

            val beyondLimit = nested(ClientSideFailurePolicy.MAX_CAUSE_DEPTH, ResponseBodyLimitExceededException("accounts", 8192, -1))

            assertThat(ignores(circuitBreakerRegistry, alias, beyondLimit)).isFalse()
        }

        @Test
        @Timeout(value = 10, unit = TimeUnit.SECONDS)
        fun `should terminate on a cyclic cause chain`() {
            // 1. What is tested: that a cause chain which loops back on itself does not spin the walk.
            // 2. How success is judged: the predicate returns a verdict at all (false - no client-side failure
            //    is in the cycle). A regression here would not fail the assertion but spin forever; the @Timeout
            //    turns that spin into a test failure instead of a hung build.
            // 3. Why it matters: the walk's own guard (`takeIf { it !== candidate }`) only catches a throwable
            //    that is its OWN cause; a two-element A -> B -> A cycle passes straight through it and is
            //    stopped by the depth bound alone. Since this predicate runs inside the circuit breaker's error
            //    hook on a request-serving thread, a spin there would not be a slow test - it would wedge the
            //    caller.
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, propsFor())

            // Given: A -> B -> A, built the only way the JDK allows (initCause on a cause-less throwable)
            val first = IllegalStateException("first")
            val second = IllegalStateException("second")
            first.initCause(second)
            second.initCause(first)

            assertThat(ignores(circuitBreakerRegistry, alias, first)).isFalse()
        }

        /** A chain of [layers] wrappers around [root]; the returned throwable is the outermost one. */
        private fun nested(
            layers: Int,
            root: Throwable,
        ): Throwable {
            var current = root
            repeat(layers) { current = IllegalStateException("layer", current) }
            return current
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** Whether the registered instance's config classifies [error] as ignorable (not a backend failure). */
    private fun ignores(
        registry: CircuitBreakerRegistry,
        alias: String,
        error: Throwable,
    ): Boolean =
        registry
            .circuitBreaker(alias)
            .circuitBreakerConfig.ignoreExceptionPredicate
            .test(error)

    /** A real rejection instance - the exception type is only constructible through the bulkhead's factory. */
    private fun bulkheadRejection(): BulkheadFullException = BulkheadFullException.createBulkheadFullException(Bulkhead.of("probe", BulkheadConfig.ofDefaults()))

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
    ): BlockingResilienceDecorator =
        BlockingResilienceDecorator(
            circuitBreakerRegistry,
            TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(Duration.ofMinutes(5)).build()),
            bulkheadRegistry,
        ).also { decorators += it }

    /** Props for the single accounts endpoint; individual=1000 at maxRetries=0 derives slowCall=800ms. */
    private fun propsFor(circuitBreakerEnabled: Boolean = false): MeshProperties =
        MeshProperties(
            defaults =
                MeshProperties.Defaults(
                    individualRequestTimeoutMs = 1000,
                    circuitBreaker = MeshProperties.BackendToggle(enabled = circuitBreakerEnabled),
                ),
            endpoints = listOf(MeshProperties.EndpointConfig(triplet)),
        )
}
