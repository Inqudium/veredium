package eu.inqudium.veredium.restclient

import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.observation.ObservationRegistry
import jakarta.annotation.PreDestroy
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future

/**
 * Applies the three Resilience4j primitives as `CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )` around a
 * BLOCKING call, using the registries auto-configured from `application.yml`. This order keeps the bulkhead
 * measuring genuine in-flight calls, lets the time limiter abandon slow calls, and lets the breaker record the
 * resulting `TimeoutException` as a failure.
 *
 * ## Why THIS order - it is Resilience4j's own
 *
 * The nesting is not a local preference: it is exactly the composition Resilience4j's Spring integration
 * applies to its annotations, minus the two primitives this pipeline does not use. The aspects declare a
 * default order (Spring `Ordered`, so a LOWER value binds FURTHER OUT), which yields
 * `Retry( CircuitBreaker( RateLimiter( TimeLimiter( Bulkhead( call ) ) ) ) )`:
 *
 * | Primitive      | Default aspect order      | Position | Used here |
 * |----------------|---------------------------|----------|-----------|
 * | Retry          | `LOWEST_PRECEDENCE - 5`   | outermost| no        |
 * | CircuitBreaker | `LOWEST_PRECEDENCE - 4`   |          | yes       |
 * | RateLimiter    | `LOWEST_PRECEDENCE - 3`   |          | no        |
 * | TimeLimiter    | `LOWEST_PRECEDENCE - 2`   |          | yes       |
 * | Bulkhead       | `LOWEST_PRECEDENCE - 1`   | innermost| yes       |
 *
 * (Read off the `*ConfigurationProperties.get*AspectOrder()` defaults in `resilience4j-spring6`, the version
 * this module resolves. Should a future upgrade change them, this table - not the reasoning above - is what
 * needs rechecking.)
 *
 * ## The thread hand-off the TimeLimiter needs
 *
 * A blocking call cannot time itself out: the thread that would notice the deadline is the one stuck in the
 * socket read. Resilience4j's blocking TimeLimiter therefore works on a `Future` - the call runs on ANOTHER
 * thread, the caller waits on the future for at most the budget, and on expiry the caller is released with a
 * `TimeoutException` while the future is cancelled (`cancelRunningFuture`). That hand-off is the one structural
 * difference to the reactive twin, where the TimeLimiter operator simply cancels the subscription, and it has
 * three consequences this class owns:
 *
 * 1. **The worker.** Every call runs on a fresh VIRTUAL thread from [executor]: a parked carrier costs nothing,
 *    so there is no pool to size and no pool that could become a second, undeclared bulkhead. The bulkhead
 *    permit is taken ON the worker, so it still counts what is actually in flight at the backend.
 *
 *    This is why the executor is deliberately UNBOUNDED and why Resilience4j's `ThreadPoolBulkhead` - which
 *    bundles a bounded platform-thread pool with the hand-off - is not used, although it looks made for the
 *    job. Its `CompletableFuture` cannot be interrupted (`cancel(true)` ignores its argument by specification),
 *    so a TimeLimiter expiry would leave the pool thread blocked in the socket until the transport timeout,
 *    holding its slot and its connection for calls nobody waits for; its queue (default 100) admits calls that
 *    then wait with their budget already running, a third waiting room the ladder's margin does not cover; its
 *    meters count pool and queue depth rather than calls at the backend, which is what the pool is sized
 *    against; and it would be a second hand-off next to the one the TimeLimiter already needs. The semaphore
 *    with a zero wait rejects at once, the virtual worker is interruptible, and the only concurrency limits in
 *    the call path stay the configured, named and metered ones - the bulkheads and the pool. The long form is
 *    `docs/timeouts-bulkheads-threads.md`, section 4.1.
 * 2. **The caller's context.** The worker starts with an empty MDC and no current observation, so everything
 *    logged there - legatium's exchange event above all - would lose the caller's correlation keys, and the
 *    `http.client.requests` observation Spring opens on the worker would start a detached root instead of
 *    nesting under the caller's span. [propagate] carries both across: the caller's MDC map is installed on the
 *    worker for the duration of the call, and the caller's current observation (when the host has a registry)
 *    is reopened as a scope there. Both are undone before the worker is released. The reactive twin solves the
 *    same problem with Reactor's context-propagation hook; here the thread IS the context, and a copy is enough.
 * 3. **What cancellation does.** Cancelling the future interrupts the worker, and since JDK 13 a blocking socket
 *    read DOES react to that: the JDK's socket implementation closes the socket and the read fails at once with
 *    `SocketException: Closed by interrupt` (the connect and the pool lease are interruptible too). The worker is
 *    therefore released promptly in every phase, its bulkhead permit with it, and the half-read connection is
 *    discarded rather than returned to the pool. The CALLER is released at the budget regardless, which is the
 *    property its end-to-end budget depends on. What the abandoned worker logs is the transport's exception:
 *    legatium's exchange event reports the cancelled exchange as a `failure` (there is no `cancelled` on the
 *    blocking stack), pinned in `LegatiumExchangeLoggingIntegrationTest`. The mesh ladder still places the
 *    per-endpoint response timeout just below the TimeLimiter budget, so that under normal conditions the
 *    transport trips first - as a `timeout` - and the TimeLimiter is only the outer backstop over the phases the
 *    response timeout does not cover (connect, pending-acquire).
 *
 * The caller being interrupted - its own budget expired, or its scope cancelled - is the blocking counterpart of
 * a subscriber cancelling: the TimeLimiter does not cancel the future in that case, so [decorate] does, and
 * rethrows the `InterruptedException` for [ResilientRestClient] to report as a cancellation.
 *
 * ## The consequence that is NOT self-enforcing
 *
 * The breaker sits OUTSIDE the bulkhead, so a `BulkheadFullException` - this pipeline's own load shedding,
 * not a backend fault - reaches the breaker's error hook like any other error and would be counted as a
 * backend failure. Nothing in Resilience4j's defaults prevents that; it takes an explicit ignore predicate,
 * which the per-alias `CircuitBreakerConfigCustomizer` of `resilience/MeshResilienceCustomizers.kt` installs
 * while Resilience4j builds the instance from `resilience4j.circuitbreaker.instances.<alias>` - for every MESH
 * ALIAS, and only those. Without that customizer the invariant does not hold, and `MeshResilienceStartupGuard`
 * fails the start when it did not land. (The TimeLimiter unwraps the worker's `ExecutionException`, so the
 * breaker sees the rejection as itself, not as a wrapper - pinned by `BlockingResilienceDecoratorTest`.)
 *
 * ## Three verdicts, not two
 *
 * That customizer sorts every error into the three answers Resilience4j actually has, the same split the
 * resolver breaker of the reactive stack's consumer uses (`answered-means-healthy`, `docs/decisions.md` D2):
 *
 * | The call | Verdict | Source |
 * |---|---|---|
 * | never reached the backend, or the caller stopped waiting (`BulkheadFullException`, the buffer limit, `InterruptedException`) | **ignored** - evidence in neither direction | [ClientSideFailurePolicy] |
 * | reached it and got a benign 4xx | **success** - the backend answered, promptly and correctly | [HttpExchangeErrorPolicy] |
 * | anything else - transport faults, 5xx, 408/429, the TimeLimiter abort | **failure** | the config defaults |
 *
 * The middle row is the one that is easy to get wrong in the safe-looking direction. Ignoring a benign 4xx
 * also keeps it out of the sliding window, so on a 4xx-dominated backend the window fills with whatever was
 * left over and a few genuine faults become its entire content - the failure mode that opened the resolver
 * breaker at a nominal 100 % on 2026-07-31. See `circuitBreakerCustomizer`.
 *
 * The scope limit is load-bearing, not incidental: a customizer exists per `meshProperties.resolveAll()` alias,
 * so a breaker constructed OUTSIDE this module never receives the invariant and has to carry its own ignore
 * predicate.
 */
@Component
class BlockingResilienceDecorator(
    private val circuitBreakerRegistry: CircuitBreakerRegistry,
    private val timeLimiterRegistry: TimeLimiterRegistry,
    private val bulkheadRegistry: BulkheadRegistry,
    /**
     * The host's observation registry, read for ONE purpose: the caller's current observation, so the worker can
     * reopen it (consequence 2 in the class KDoc). Nullable on purpose - an optional dependency, so a direct
     * (test) construction and a host without Micrometer observation both work; absent, nothing is propagated
     * and the exchange's observation, if any, starts as its own root.
     */
    private val observationRegistry: ObservationRegistry? = null,
) {
    private val logger: Logger = LoggerFactory.getLogger(BlockingResilienceDecorator::class.java)

    /**
     * One virtual thread per call, owned by this decorator and shut down with it. Not injectable: a host's own
     * `ExecutorService` bean would otherwise be picked up by constructor injection and shut down by a component
     * that does not own it, and a bounded pool would silently cap concurrency below what the bulkheads declare.
     */
    private val executor: ExecutorService =
        Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("restclient-call-", 0).factory())

    fun <T : Any> decorate(
        backendName: String,
        call: () -> T,
    ): T {
        val circuitBreaker = circuitBreakerRegistry.circuitBreaker(backendName)
        val timeLimiter = timeLimiterRegistry.timeLimiter(backendName)
        // Only the semaphore Bulkhead applies here; a ThreadPoolBulkhead would be a second hand-off on top of
        // the TimeLimiter's and would hide the one this class declares.
        val bulkhead = bulkheadRegistry.bulkhead(backendName)

        // Innermost: the permit is acquired on the WORKER, right before the call, so the bulkhead counts calls
        // that are genuinely in flight and a rejection costs no thread hand-off beyond the one already made.
        val guarded: Callable<T> = Bulkhead.decorateCallable(bulkhead, propagate(call))

        // Middle: the hand-off and the wait. The future is created per invocation INSIDE the callable the breaker
        // wraps, so a short-circuited call (breaker open) submits nothing.
        val limited =
            Callable<T> {
                val future: Future<T> = executor.submit(guarded)
                try {
                    timeLimiter.decorateFutureSupplier<T, Future<T>> { future }.call()
                } catch (e: InterruptedException) {
                    // The CALLER went away. Resilience4j cancels the future on ITS timeout only, so the worker
                    // would otherwise run on, holding a bulkhead permit, for a result nobody waits for. The
                    // interrupt closes the worker's socket (consequence 3 in the class KDoc), so this is prompt.
                    future.cancel(true)
                    throw e
                }
            }

        // Outermost: the breaker records whatever comes out - the timeout, the transport fault, the status
        // exception - subject to the record/ignore predicates the registrars below contribute.
        return CircuitBreaker.decorateCallable(circuitBreaker, limited).call()
    }

    /**
     * The configured TimeLimiter timeout for [backendName] - the end-to-end request budget. Exposed so the
     * transport can derive a *tighter* response-phase guard from it, rather than a second independent timeout
     * that could drift past the budget.
     */
    fun requestBudget(backendName: String): Duration = timeLimiterRegistry.timeLimiter(backendName).timeLimiterConfig.timeoutDuration

    /**
     * Wraps [call] so that it runs on the worker with the CALLER's context: the MDC as the caller has it now,
     * and the caller's current observation reopened as a scope. Captured on the calling thread at decoration
     * time, which is the moment `decorate` runs - the same moment a reactive `contextCapture()` would read the
     * subscriber's thread-locals.
     *
     * Restoration is unconditional and in `finally`: a virtual thread is never reused, but an executor handed in
     * by a test might reuse platform threads, and a worker must not leak one caller's keys into the next call.
     */
    private fun <T : Any> propagate(call: () -> T): Callable<T> {
        val callerMdc: Map<String, String>? = MDC.getCopyOfContextMap()
        val callerObservation = observationRegistry?.currentObservation
        return Callable {
            val workerMdc: Map<String, String>? = MDC.getCopyOfContextMap()
            if (callerMdc != null) MDC.setContextMap(callerMdc) else MDC.clear()
            val scope = callerObservation?.openScope()
            try {
                call()
            } finally {
                scope?.close()
                if (workerMdc != null) MDC.setContextMap(workerMdc) else MDC.clear()
            }
        }
    }

    /**
     * Releases the worker executor. A thread-per-task executor holds no idle threads, so there is nothing to
     * drain: `shutdownNow` only interrupts the workers of calls still in flight, whose callers are by then
     * already gone or going. A call decorated AFTER this point is refused by the executor with a
     * `RejectedExecutionException`, which the breaker ignores as a client-side failure.
     */
    @PreDestroy
    fun shutdown() {
        executor.shutdownNow()
        logger.debug("RestClient call executor shut down")
    }
}
