package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.testsupport.CustomizedRegistries
import eu.inqudium.veredium.testsupport.singlePermitBulkheads
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadFullException
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import reactor.core.Exceptions
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Pins the operator ORDER of [ReactiveResilienceDecorator.decorate] - `CircuitBreaker( TimeLimiter( Bulkhead(
 * call ) ) )` - by the behaviour each nesting produces, without HTTP: the sources are a never-emitting
 * `Sinks.one()` and counting `Mono.defer`s, the registries are real Resilience4j registries, and the circuit
 * breaker carries the mesh customizer with this stack's [WebClientVerdicts] the way the Boot configuration
 * installs it ([CustomizedRegistries]).
 *
 * The TimeLimiter, open-breaker and full-bulkhead tests pin consequences of the nesting: each of the five other
 * orders of the three `transformDeferred` calls in `decorate()` turns at least one of them red -
 * - TimeLimiter OUTSIDE the breaker: the timeout cancels the breaker's subscriber instead of erroring through
 *   it, and the breaker records nothing - the TimeLimiter test fails (and the time limiter now sees the open
 *   breaker's short-circuit, which fails the open-breaker test).
 * - Bulkhead OUTSIDE the breaker: an open breaker's rejection takes (and returns) a permit first, and a full
 *   bulkhead's rejection never reaches the breaker - the open-breaker and the full-bulkhead tests fail.
 * - Bulkhead OUTSIDE the TimeLimiter: a full bulkhead's rejection does not pass the time limiter on its way
 *   out - the full-bulkhead test fails. That is the only observable difference this swap makes on the reactive
 *   stack (see the bulkhead-wait test for why the budget is not one of them).
 *
 * The fourth test pins what NO order changes but the reference configuration relies on: where a caller waits
 * for a permit.
 *
 * The blocking twin of the first case is pinned in veredium-core (`BlockingResilienceDecoratorTest`). The
 * TimeLimiter budgets are real time but short and only on the tests that are about the timeout; a
 * never-emitting source makes the outcome independent of how fast the machine is.
 */
class ReactiveResilienceDecoratorTest {
    private val alias = "api-example-com-GET-accounts"
    private val mesh =
        MeshProperties(
            defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
            endpoints = listOf(MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/accounts/{accountId}")),
        )

    @Test
    fun `should let the circuit breaker record a TimeLimiter timeout as a failure`() {
        // What is tested: that a call the TimeLimiter aborts reaches the circuit breaker as an ERROR, and that the
        //   mesh verdicts count it - the whole reason the breaker sits outside the time limiter.
        // Success criteria: a never-emitting source under a 100 ms budget fails with exactly the TimeoutException
        //   the time limiter raises; the breaker's failed-call counter is exactly one; the source lost its
        //   subscriber (the timeout cancelled the call rather than leaving it running) and the bulkhead permit
        //   is back.
        // Why it matters: a hanging backend is exactly what the breaker exists for. Were the time limiter nested
        //   outside it, the timeout would CANCEL the breaker's subscription - which Resilience4j treats as "no
        //   result", releasing the permission without recording anything - and the breaker would never open on
        //   timeouts while every 5xx test stayed green.
        //
        // Given: a never-answering backend and a short budget
        val circuitBreakers = customizedCircuitBreakers()
        val bulkheads = BulkheadRegistry.ofDefaults()
        val decorator = decorator(circuitBreakers, budget = Duration.ofMillis(100), bulkheads = bulkheads)
        val backend = Sinks.one<String>()

        // When
        val failure = failureOf(decorator.decorate(alias, backend.asMono()))

        // Then
        assertThat(failure).isExactlyInstanceOf(TimeoutException::class.java)
        val metrics = circuitBreakers.circuitBreaker(alias).metrics
        assertThat(metrics.numberOfFailedCalls).`as`("the breaker must count the timeout").isEqualTo(1)
        assertThat(metrics.numberOfSuccessfulCalls).isZero()
        assertThat(backend.currentSubscriberCount()).`as`("the timed-out call must be cancelled").isZero()
        assertThat(bulkheads.bulkhead(alias).metrics.availableConcurrentCalls)
            .isEqualTo(BulkheadConfig.DEFAULT_MAX_CONCURRENT_CALLS)
    }

    @Test
    fun `should short-circuit an open breaker without subscribing to the call or taking a bulkhead permit`() {
        // What is tested: what an OPEN breaker does to the two layers inside it - nothing at all.
        // Success criteria: the call fails with CallNotPermittedException; the source was never subscribed; the
        //   bulkhead published no permitted call (so no permit was taken and handed back) and the time limiter
        //   recorded nothing.
        // Why it matters: the breaker is outermost so that a short-circuited call is the cheapest call there
        //   is. With the bulkhead outside it, every rejected call would briefly occupy a permit - the bulkhead
        //   would bound and count a population that never reaches the backend, and under a burst of rejections
        //   could shed calls the breaker would have let through in half-open state.
        //
        // Given: a breaker forced open, and a source and a bulkhead that count what reaches them
        val circuitBreakers = customizedCircuitBreakers()
        circuitBreakers.circuitBreaker(alias).transitionToOpenState()
        val bulkheads = BulkheadRegistry.ofDefaults()
        val permitted = bulkheadPermits(bulkheads.bulkhead(alias))
        val timeLimiters = TimeLimiterRegistry.of(generousBudget())
        val timeLimiterEvents = CopyOnWriteArrayList<Any>()
        timeLimiters.timeLimiter(alias).eventPublisher.onEvent { timeLimiterEvents += it }
        val subscriptions = AtomicInteger()
        val source = Mono.defer { Mono.just("answer").also { subscriptions.incrementAndGet() } }
        val decorator = ReactiveResilienceDecorator(circuitBreakers, timeLimiters, bulkheads)

        // When
        val failure = failureOf(decorator.decorate(alias, source))

        // Then
        assertThat(failure).isInstanceOf(CallNotPermittedException::class.java)
        assertThat(circuitBreakers.circuitBreaker(alias).state).isEqualTo(CircuitBreaker.State.OPEN)
        assertThat(subscriptions.get()).`as`("an open breaker must not call the backend").isZero()
        assertThat(permitted.get()).`as`("an open breaker must not take a bulkhead permit").isZero()
        assertThat(timeLimiterEvents).`as`("the time limiter must not see the short-circuit").isEmpty()
    }

    @Test
    fun `should hand a full bulkhead's rejection to the breaker, which ignores it under the mesh verdicts`() {
        // What is tested: the rejection path of a full bulkhead through the composition - it travels OUTWARD
        //   through the time limiter and the breaker, and with the mesh customizer's verdicts the breaker
        //   ignores it rather than counting it.
        // Success criteria: the second call fails with BulkheadFullException; the breaker published exactly one
        //   ignored-error event carrying that exception and its failed-call counter stays at zero; and the time
        //   limiter, which the rejection passes on its way out, recorded it as one failed call. That last part is
        //   what the composition does rather than a choice anyone made: Resilience4j's TimeLimiterOperator
        //   reports every error it relays, so the TimeLimiter's own `failed` metric includes bulkhead
        //   rejections - pinned here so a dashboard reading it is not surprised.
        // Why it matters: the breaker seeing the rejection is what the nesting gives (with the bulkhead
        //   outermost it never would, and both events would be missing); the breaker not COUNTING it is what the
        //   verdicts give - without them Resilience4j's default records every error, and a concurrency peak would
        //   open the breaker on a healthy backend. `WebClientVerdictsTest` pins the second half; this pins that
        //   the two compose.
        //
        // Given: a single-permit bulkhead whose permit is held by a call that has not completed
        val circuitBreakers = customizedCircuitBreakers()
        val ignored = CopyOnWriteArrayList<Throwable>()
        circuitBreakers.circuitBreaker(alias).eventPublisher.onIgnoredError { ignored += it.throwable }
        val timeLimiters = TimeLimiterRegistry.of(generousBudget())
        val timeLimiterErrors = CopyOnWriteArrayList<Throwable>()
        timeLimiters.timeLimiter(alias).eventPublisher.onError { timeLimiterErrors += it.throwable }
        val decorator = ReactiveResilienceDecorator(circuitBreakers, timeLimiters, singlePermitBulkheads(Duration.ZERO))
        val held = Sinks.one<String>()
        val holding = decorator.decorate(alias, held.asMono()).subscribe({ }, { })
        try {
            // When: a second call arrives
            val rejection = failureOf(decorator.decorate(alias, Mono.just("second")))

            // Then
            assertThat(rejection).isInstanceOf(BulkheadFullException::class.java)
            assertThat(ignored).singleElement().isInstanceOf(BulkheadFullException::class.java)
            assertThat(circuitBreakers.circuitBreaker(alias).metrics.numberOfFailedCalls).isZero()
            assertThat(timeLimiterErrors).singleElement().isInstanceOf(BulkheadFullException::class.java)
        } finally {
            holding.dispose()
        }
    }

    @Test
    fun `should let a caller wait for a bulkhead permit outside the TimeLimiter budget, on the subscribing thread`() {
        // What is tested: where the wait for a permit happens when the bulkhead is configured to wait. Pinned as
        //   the composition actually behaves, which is NOT what one would guess from the nesting: Resilience4j's
        //   reactive bulkhead acquires its permit inside `subscribe()`, before it hands its subscription
        //   downstream, and Reactor's `timeout` only starts its timer once it receives that subscription - so the
        //   budget starts AFTER the wait, and the wait blocks the subscribing thread.
        // Success criteria: with the only permit held, a 500 ms max-wait and a 100 ms budget, the call fails
        //   with BulkheadFullException (the wait ran its course; the budget never fired) on the thread that
        //   subscribed - and the breaker ignores it.
        // Why it matters: it is the real reason behind `max-wait-duration: 0` in the reference configuration. A
        //   waiting caller does not burn its budget here; it parks the thread that subscribed - an event loop,
        //   whenever the call is composed into another reactive pipeline - for up to max-wait-duration, which no
        //   timeout in the ladder bounds. Should a Resilience4j or Reactor upgrade change either half, this test
        //   says so before a configuration that relies on it does.
        //
        // Given: the only permit taken directly, a bulkhead that waits for one, and a budget far shorter than
        // the wait
        val circuitBreakers = customizedCircuitBreakers()
        val bulkheads = singlePermitBulkheads(Duration.ofMillis(500))
        val bulkhead = bulkheads.bulkhead(alias)
        assertThat(bulkhead.tryAcquirePermission()).isTrue()
        val decorator = decorator(circuitBreakers, budget = Duration.ofMillis(100), bulkheads = bulkheads)
        val errorThread = AtomicReference<Thread>()
        try {
            // When
            val failure =
                failureOf(decorator.decorate(alias, Mono.just("second")).doOnError { errorThread.set(Thread.currentThread()) })

            // Then
            assertThat(failure).isInstanceOf(BulkheadFullException::class.java)
            assertThat(errorThread.get()).`as`("the wait and its rejection happen on the subscribing thread").isSameAs(Thread.currentThread())
            assertThat(circuitBreakers.circuitBreaker(alias).metrics.numberOfFailedCalls).isZero()
        } finally {
            bulkhead.releasePermission()
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** A circuit-breaker registry whose instance for [alias] carries the mesh customizer with this stack's verdicts. */
    private fun customizedCircuitBreakers(): CircuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults().also { CustomizedRegistries.circuitBreakers(it, mesh, WebClientVerdicts.breakerVerdicts) }

    private fun decorator(
        circuitBreakers: CircuitBreakerRegistry,
        budget: Duration,
        bulkheads: BulkheadRegistry,
    ): ReactiveResilienceDecorator =
        ReactiveResilienceDecorator(
            circuitBreakers,
            TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(budget).build()),
            bulkheads,
        )

    /**
     * What [mono] fails with, as the caller would see it: `block()` wraps a checked exception (the
     * TimeoutException) in Reactor's ReactiveException, which `Exceptions.unwrap` takes off again.
     */
    private fun failureOf(mono: Mono<*>): Throwable? = catchThrowable { mono.block() }?.let(Exceptions::unwrap)

    /** For the tests NOT about the timeout: a budget no call here can run into. */
    private fun generousBudget(): TimeLimiterConfig = TimeLimiterConfig.custom().timeoutDuration(Duration.ofMinutes(5)).build()

    /** Counts the permits [bulkhead] hands out - its CallPermitted events. */
    private fun bulkheadPermits(bulkhead: Bulkhead): AtomicInteger = AtomicInteger().also { count -> bulkhead.eventPublisher.onCallPermitted { count.incrementAndGet() } }
}
