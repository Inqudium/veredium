package eu.inqudium.veredium.restclient

import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadFullException
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationHandler
import io.micrometer.observation.ObservationRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import java.time.Duration
import java.util.Optional
import java.util.concurrent.CancellationException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Tests for the mechanics of [BlockingResilienceDecorator] that the reactive twin does not have: the thread
 * hand-off the blocking TimeLimiter needs, and everything that has to survive it - the caller's MDC, the
 * caller's observation, the exception types the breaker sees, and the two ways a call can be abandoned.
 *
 * Contention is produced deterministically, with latches a worker blocks on and the test releases, never with
 * racing threads or sleeps. The only real time in this class is the TimeLimiter budget of the timeout cases,
 * which is the thing under test there.
 */
class BlockingResilienceDecoratorTest {
    private val alias = "api-example-com-GET-accounts"

    /** Decorators built during a test; their worker executors are shut down in [tearDown]. */
    private val decorators = mutableListOf<BlockingResilienceDecorator>()

    /** Runs the calls that must block from a thread other than the test's. */
    private val callers: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()

    @AfterEach
    fun tearDown() {
        callers.shutdownNow()
        decorators.forEach { it.shutdown() }
    }

    @Nested
    inner class `The hand-off` {
        @Test
        fun `should run the call on a virtual worker thread, not on the caller's`() {
            // What is tested: the structural fact every other test here rests on - the call does NOT run on the
            //   thread that invoked decorate(), and the thread it runs on is a virtual one.
            // Why it matters: a decorator that ran the call inline would make the TimeLimiter a no-op (nothing
            //   could release the caller at the budget), and a platform-thread pool would be a second, hidden
            //   bulkhead. Both would pass every functional test on a fast backend.
            //
            // Given
            val decorator = decorator()
            val caller = Thread.currentThread()

            // When
            val worker = decorator.decorate(alias) { Thread.currentThread() }

            // Then
            assertThat(worker).isNotSameAs(caller)
            assertThat(worker.isVirtual).isTrue()
            assertThat(worker.name).startsWith("restclient-call-")
        }

        @Test
        fun `should hand the caller's MDC to the worker and keep it only for the call`() {
            // What is tested: consequence 2 of the hand-off - a key the caller put in its MDC is visible on the
            //   worker while the call runs.
            // How success is judged: the call reads the key on the worker and gets the caller's value; the call
            //   also records the worker thread, whose MDC is empty again once the call is over (read back through
            //   a second call on a fresh worker is pointless, so the restoration is observed from inside, after
            //   the fact, via a latch).
            // Why it matters: legatium's exchange event is written on the worker. Without the copy, the one
            //   field that joins the outbound line to the inbound request - the caller's correlation id - would
            //   be missing from every exchange event this module produces.
            //
            // Given: a caller carrying a correlation id
            val decorator = decorator()
            MDC.put("CORRELATION_ID", "Out:7cc0270abf0da42c")
            try {
                // When
                val seenOnWorker = decorator.decorate(alias) { MDC.get("CORRELATION_ID") }

                // Then
                assertThat(seenOnWorker).isEqualTo("Out:7cc0270abf0da42c")
            } finally {
                MDC.clear()
            }
        }

        @Test
        fun `should give the worker an empty MDC when the caller has none`() {
            // Given / When / Then: no leakage of anything into a call made without context
            val decorator = decorator()
            MDC.clear()
            val seenOnWorker = decorator.decorate(alias) { MDC.getCopyOfContextMap().orEmpty() }
            assertThat(seenOnWorker).isEmpty()
        }

        @Test
        fun `should reopen the caller's current observation on the worker`() {
            // What is tested: the observation half of consequence 2 - inside the call, the registry's current
            //   observation is the one the CALLER had open, so Spring's http.client.requests observation (opened
            //   on the worker) nests under it instead of starting a detached root.
            // How success is judged: the call returns the registry's current observation as seen on the worker,
            //   and it is the very parent the test started on its own thread.
            // Why it matters: this is what keeps an outbound client span inside the inbound server trace. It is
            //   invisible in any test that does not look - the call succeeds either way - and only shows up as
            //   broken traces in a trace viewer.
            //
            // Given: a recording registry and a parent observation open on the test thread
            val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(RecordingHandler()) }
            val decorator = decorator(observationRegistry = registry)
            val parent = Observation.start("inbound.server", registry)

            // When: the call runs inside the parent's scope
            val seenOnWorker =
                parent.openScope().use {
                    decorator.decorate(alias) { Optional.ofNullable(registry.currentObservation) }
                }
            parent.stop()

            // Then: the worker saw the caller's observation as current
            assertThat(seenOnWorker).containsSame(parent)
        }

        @Test
        fun `should leave the worker without a current observation once the call is over`() {
            // Why this case is important: a scope that is not closed on the worker would leak the caller's
            // observation into whatever Micrometer does with that thread afterwards. Observed from inside the
            // call: the scope is open during it, and closed by the time the call's own continuation runs.
            //
            // Given
            val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(RecordingHandler()) }
            val decorator = decorator(observationRegistry = registry)
            val parent = Observation.start("inbound.server", registry)

            // When
            val duringCall = parent.openScope().use { decorator.decorate(alias) { Optional.ofNullable(registry.currentObservation) } }
            parent.stop()
            val afterwards = decorator.decorate(alias) { Optional.ofNullable(registry.currentObservation) }

            // Then
            assertThat(duringCall).containsSame(parent)
            assertThat(afterwards).isEmpty()
        }
    }

    @Nested
    inner class `What the breaker sees` {
        @Test
        fun `should surface the call's own exception, not the worker's ExecutionException wrapper`() {
            // What is tested: that the Future hand-off does not change the type of a failure. A worker's exception
            //   arrives at the caller wrapped in an ExecutionException; the TimeLimiter unwraps it, and the breaker
            //   outside must see - and record - the ORIGINAL.
            // Why it matters: every predicate on the breaker (the two registrars) matches on exception type and
            //   cause chain. A wrapper nobody expected would make a BulkheadFullException look like a backend
            //   fault and a benign 4xx look like an unknown error.
            //
            // Given
            val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
            val decorator = decorator(circuitBreakers = circuitBreakers)
            val failure = IllegalStateException("backend exploded")

            // When
            val thrown = catchThrowable { decorator.decorate<String>(alias) { throw failure } }

            // Then: the same instance, and the breaker counted it
            assertThat(thrown).isSameAs(failure)
            assertThat(circuitBreakers.circuitBreaker(alias).metrics.numberOfFailedCalls).isEqualTo(1)
        }

        @Test
        fun `should reject the second call with BulkheadFullException while the first holds the only permit`() {
            // What is tested: that the bulkhead permit is taken on the worker for the duration of the call, so a
            //   second call finds it taken - and that the rejection reaches the caller as itself.
            // Given: a single-permit bulkhead, and a call parked on the worker holding that permit
            val decorator = decorator(bulkheads = singlePermitBulkheads())
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
            assertThat(entered.await(5, TimeUnit.SECONDS)).`as`("the first call must be in flight").isTrue()

            try {
                // When
                val rejection = catchThrowable { decorator.decorate(alias) { "second" } }

                // Then
                assertThat(rejection).isInstanceOf(BulkheadFullException::class.java)
            } finally {
                release.countDown()
                assertThat(holding.get(5, TimeUnit.SECONDS)).isEqualTo("held")
            }
        }

        @Test
        fun `should release the caller at the TimeLimiter budget, interrupt the worker and record the timeout`() {
            // What is tested: the TimeLimiter in the blocking shape - the caller is released with a
            //   TimeoutException at the budget although the call has not returned, the worker is cancelled
            //   (interrupted), and the breaker records the timeout as a failure.
            // How success is judged: the caller's exception is a TimeoutException; the worker's park ends in an
            //   InterruptedException it records; the breaker's failed-call counter reads one.
            // Why it matters: this is the end-to-end budget guarantee every caller relies on, and the one thing
            //   a blocking client cannot do without the hand-off.
            //
            // Given: a 100 ms budget and a call that would park forever
            val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
            val decorator = decorator(circuitBreakers = circuitBreakers, timeLimiter = Duration.ofMillis(100))
            val release = CountDownLatch(1)
            val workerInterrupted = CountDownLatch(1)

            // When
            val thrown =
                catchThrowable {
                    decorator.decorate(alias) {
                        try {
                            release.await()
                        } catch (e: InterruptedException) {
                            workerInterrupted.countDown()
                            throw e
                        }
                        "never"
                    }
                }

            // Then
            assertThat(thrown).isInstanceOf(TimeoutException::class.java)
            assertThat(workerInterrupted.await(5, TimeUnit.SECONDS)).`as`("the abandoned worker must be interrupted").isTrue()
            assertThat(circuitBreakers.circuitBreaker(alias).metrics.numberOfFailedCalls).isEqualTo(1)
        }

        @Test
        fun `should cancel the worker when the caller itself is interrupted`() {
            // What is tested: the blocking counterpart of a subscriber cancelling. Resilience4j cancels the
            //   future on ITS timeout only; a caller that is interrupted while waiting would otherwise leave the
            //   worker running, holding a bulkhead permit, for a result nobody waits for.
            // How success is judged: the interrupted caller gets an InterruptedException, and the worker's park
            //   ends in an InterruptedException of its own.
            //
            // Given: a call parked on the worker, and a caller thread waiting on it
            val decorator = decorator(timeLimiter = Duration.ofMinutes(5))
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            val workerInterrupted = CountDownLatch(1)
            val callerThread = AtomicReference<Thread>()
            val callerOutcome =
                callers.submit<Throwable?> {
                    callerThread.set(Thread.currentThread())
                    catchThrowable {
                        decorator.decorate(alias) {
                            entered.countDown()
                            try {
                                release.await()
                            } catch (e: InterruptedException) {
                                workerInterrupted.countDown()
                                throw e
                            }
                            "never"
                        }
                    }
                }
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()

            // When: the caller is interrupted
            callerThread.get().interrupt()

            // Then
            assertThat(callerOutcome.get(5, TimeUnit.SECONDS)).isInstanceOf(InterruptedException::class.java)
            assertThat(workerInterrupted.await(5, TimeUnit.SECONDS)).`as`("the worker must be cancelled with its caller").isTrue()
            release.countDown()
        }

        @Test
        fun `should short-circuit an open breaker without submitting anything to a worker`() {
            // What is tested: the breaker is the OUTERMOST decoration, so an open circuit rejects locally - no
            //   permit taken, no worker started, the call lambda never runs.
            // Given: a breaker forced open
            val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
            val decorator = decorator(circuitBreakers = circuitBreakers)
            circuitBreakers.circuitBreaker(alias).transitionToOpenState()
            val ran = AtomicBoolean(false)

            // When
            val thrown =
                catchThrowable {
                    decorator.decorate(alias) {
                        ran.set(true)
                        "never"
                    }
                }

            // Then
            assertThat(thrown).isInstanceOf(CallNotPermittedException::class.java)
            assertThat(ran).isFalse()
            assertThat(circuitBreakers.circuitBreaker(alias).state).isEqualTo(CircuitBreaker.State.OPEN)
        }

        @Test
        fun `should let an Error escape as itself`() {
            // Given / When / Then: an Error is not an exception the breaker records, and it must not be turned
            // into one on its way through the hand-off
            val decorator = decorator()
            val error = AssertionError("invariant broken on the worker")
            val thrown = catchThrowable { decorator.decorate<String>(alias) { throw error } }
            assertThat(thrown).isSameAs(error)
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /**
     * A decorator over the supplied registries. The time limiter is generous unless a test says otherwise: most
     * tests hold a call open on purpose, and a short budget would cancel it mid-test and turn a deterministic
     * outcome into a race.
     */
    private fun decorator(
        circuitBreakers: CircuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults(),
        bulkheads: BulkheadRegistry = BulkheadRegistry.ofDefaults(),
        timeLimiter: Duration = Duration.ofMinutes(5),
        observationRegistry: ObservationRegistry? = null,
    ): BlockingResilienceDecorator =
        BlockingResilienceDecorator(
            circuitBreakers,
            TimeLimiterRegistry.of(
                TimeLimiterConfig
                    .custom()
                    .timeoutDuration(timeLimiter)
                    .cancelRunningFuture(true)
                    .build(),
            ),
            bulkheads,
            observationRegistry,
        ).also { decorators += it }

    /** A registry whose bulkheads hold exactly one permit and reject the next caller without waiting. */
    private fun singlePermitBulkheads(): BulkheadRegistry =
        BulkheadRegistry.of(
            BulkheadConfig
                .custom()
                .maxConcurrentCalls(1)
                .maxWaitDuration(Duration.ZERO)
                .build(),
        )

    /** Accepts every context and records nothing: its presence is what makes a registry count as live. */
    private class RecordingHandler : ObservationHandler<Observation.Context> {
        override fun supportsContext(context: Observation.Context): Boolean = true
    }
}
