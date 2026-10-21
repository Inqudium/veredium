package eu.inqudium.veredium.core.resilience

import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadFullException
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiter
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.observation.ObservationRegistry
import jakarta.annotation.PreDestroy
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference

/**
 * Applies the three Resilience4j primitives as `CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )` around a
 * BLOCKING call. This order keeps the bulkhead measuring genuine in-flight calls, lets the time limiter abandon
 * slow calls, and lets the breaker record the resulting `TimeoutException` as a failure.
 *
 * A component of its own, usable without any client of this library: hand it the three registries (or, per
 * call, the three instances) and a `() -> T`, and the call runs guarded. `veredium-restclient` is its one
 * consumer in this repository; a host that has a blocking call of its own to bound - a JDBC lookup, a legacy
 * SDK, a file system - decorates it the same way. There is no Spring annotation on the class: a host that wants
 * it as a bean declares one (the blocking client does, in `RestClientConfiguration`), which is also where
 * the worker-thread name of that host is chosen.
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
 * difference to a reactive pipeline, where the TimeLimiter operator simply cancels the subscription, and it has
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
 *    then wait with their budget already running, a third waiting room no timeout margin covers; its meters
 *    count pool and queue depth rather than calls at the backend, which is what a pool is sized against; and it
 *    would be a second hand-off next to the one the TimeLimiter already needs. The semaphore with a zero wait
 *    rejects at once, the virtual worker is interruptible, and the only concurrency limits in the call path stay
 *    the configured, named and metered ones. The long form is `veredium-restclient/docs/timeouts-bulkheads-threads.md`,
 *    section 4.1.
 * 2. **The caller's context.** The worker starts with an empty MDC and no current observation, so everything
 *    logged there would lose the caller's correlation keys, and an observation opened on the worker would start
 *    a detached root instead of nesting under the caller's span. [propagate] carries both across: the caller's
 *    MDC map is installed on the worker for the duration of the call, and the caller's current observation
 *    (when the host hands in a registry) is reopened as a scope there. Both are undone before the worker is
 *    released. A reactive pipeline solves the same problem with Reactor's context-propagation hook; here the
 *    thread IS the context, and a copy is enough.
 * 3. **What cancellation does.** Cancelling the future interrupts the worker, and since JDK 13 a blocking socket
 *    read DOES react to that: the JDK's socket implementation closes the socket and the read fails at once with
 *    `SocketException: Closed by interrupt` (the connect and a pool lease are interruptible too). The worker is
 *    therefore released promptly in every phase, its bulkhead permit with it. The CALLER is released at the
 *    budget regardless, which is the property its end-to-end budget depends on. What the abandoned worker sees
 *    is whatever its blocking operation throws on interrupt; a call that ignores interruption runs on until it
 *    returns, which is the one thing this class cannot change - decorate only calls that honour it.
 *
 * The caller being interrupted - its own budget expired, or its scope cancelled - is the blocking counterpart of
 * a subscriber cancelling: the TimeLimiter does not cancel the future in that case, so [decorate] does, and
 * rethrows the `InterruptedException` for the caller to report as a cancellation.
 *
 * ## The asynchronous form
 *
 * [decorateAsync] runs the SAME pipeline but returns at once with a [CompletableFuture]: the wait on the worker's
 * future - the TimeLimiter's `future.get(budget)` - happens on a second virtual thread, the *waiter*, instead of
 * on the caller's. Everything above holds unchanged, because the waiter simply IS the caller of [decorate]:
 * the breaker, the budget, the bulkhead permit on the worker, the context (captured on the real caller's thread
 * and carried to the waiter, from there to the worker). Cancelling the returned future interrupts the waiter,
 * which cancels the worker exactly as an interrupted blocking caller does - so a cancelled asynchronous call
 * frees its permit and its connection promptly, which a plain `CompletableFuture` chain over a thread pool
 * never does (its `cancel` does not interrupt by specification). Two virtual threads per call cost nothing
 * worth measuring; what they buy is one set of semantics for both forms.
 *
 * ## The consequence that is NOT self-enforcing
 *
 * The breaker sits OUTSIDE the bulkhead, so a `BulkheadFullException` - this pipeline's own load shedding,
 * not a backend fault - reaches the breaker's error hook like any other error and would be counted as a
 * backend failure. The same goes for the two faults the hand-off itself can raise: the caller's
 * `InterruptedException` (rethrown out of the decorated call, and `CircuitBreaker.decorateCallable` records
 * every `Exception`) and the `RejectedExecutionException` of a worker submitted after [shutdown]. Nothing in
 * Resilience4j's defaults prevents any of it; it takes an explicit ignore predicate on every breaker this class
 * is used with. [SELF_INFLICTED_FAILURES] names the three so a host can configure them, and the mesh customizer
 * of this module installs them - through the stack's [ClientSideFailurePolicy] - on every mesh alias while
 * Resilience4j builds the instance. A breaker constructed outside that wiring never receives the invariant and
 * has to carry its own predicate; a standalone user passes [SELF_INFLICTED_FAILURES] to
 * `CircuitBreakerConfig.ignoreExceptions`. (The TimeLimiter unwraps the worker's `ExecutionException`, so the
 * breaker sees every failure as itself, not as a wrapper - pinned by `BlockingResilienceDecoratorTest`.)
 *
 * @property observationRegistry The host's observation registry, read for ONE purpose: the caller's current
 *   observation, so the worker can reopen it (consequence 2 above). Nullable on purpose - an optional
 *   dependency, so a direct construction and a host without Micrometer observation both work; absent, nothing
 *   is propagated and an observation opened on the worker, if any, starts as its own root.
 * @property workerNamePrefix The name prefix of the virtual worker threads (`<prefix><n>`), which is what a
 *   thread dump and a log line's thread name show. A host names its own calls; the blocking client uses
 *   `restclient-call-`.
 */
class BlockingResilienceDecorator(
    private val circuitBreakerRegistry: CircuitBreakerRegistry,
    private val timeLimiterRegistry: TimeLimiterRegistry,
    private val bulkheadRegistry: BulkheadRegistry,
    private val observationRegistry: ObservationRegistry? = null,
    private val workerNamePrefix: String = DEFAULT_WORKER_NAME_PREFIX,
) {
    private val logger: Logger = LoggerFactory.getLogger(BlockingResilienceDecorator::class.java)

    /**
     * One virtual thread per call, owned by this decorator and shut down with it. Not injectable: a host's own
     * `ExecutorService` bean would otherwise be picked up by constructor injection and shut down by a component
     * that does not own it, and a bounded pool would silently cap concurrency below what the bulkheads declare.
     */
    private val executor: ExecutorService =
        Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(workerNamePrefix, 0).factory())

    /**
     * Runs [call] guarded by the three instances registered under [name] in the registries this decorator was
     * built with - the shape a Spring host uses, where `resilience4j.<layer>.instances.<name>` configures them.
     * An instance the registry does not hold is created on its default config, as the registries do.
     *
     * @throws TimeoutException when the TimeLimiter budget expired before the call returned.
     * @throws InterruptedException when the CALLER was interrupted while waiting; the worker is cancelled.
     */
    @Throws(TimeoutException::class, InterruptedException::class)
    fun <T : Any> decorate(
        name: String,
        call: () -> T,
    ): T =
        decorate(
            circuitBreakerRegistry.circuitBreaker(name),
            timeLimiterRegistry.timeLimiter(name),
            // Only the semaphore Bulkhead applies here; a ThreadPoolBulkhead would be a second hand-off on top of
            // the TimeLimiter's and would hide the one this class declares.
            bulkheadRegistry.bulkhead(name),
            call,
        )

    /**
     * Runs [call] guarded by exactly these three instances - the shape a standalone user takes, with instances
     * built by hand (`CircuitBreaker.of(...)`, `TimeLimiter.of(...)`, `Bulkhead.of(...)`) or taken from any
     * registry. The registries this decorator holds are not consulted.
     *
     * Declared with `@Throws` for a Java host (ADR-0002 offers this class as a component of its own): the two
     * checked exceptions the pipeline itself raises would otherwise be uncatchable by type in Java. Everything
     * else arrives as itself, unwrapped - `CallNotPermittedException`, `BulkheadFullException`, and whatever the
     * call throws (a Kotlin [call] may throw a checked exception of its own, which is not declared here).
     *
     * @throws TimeoutException when the TimeLimiter budget expired before the call returned.
     * @throws InterruptedException when the CALLER was interrupted while waiting; the worker is cancelled.
     */
    @Throws(TimeoutException::class, InterruptedException::class)
    fun <T : Any> decorate(
        circuitBreaker: CircuitBreaker,
        timeLimiter: TimeLimiter,
        bulkhead: Bulkhead,
        call: () -> T,
    ): T {
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
                    // interrupt reaches the worker's blocking operation (consequence 3 in the class KDoc).
                    future.cancel(true)
                    throw e
                }
            }

        // Outermost: the breaker records whatever comes out - the timeout, the transport fault, the caller's
        // own exception - subject to the record/ignore predicates its config carries.
        return CircuitBreaker.decorateCallable(circuitBreaker, limited).call()
    }

    /**
     * The asynchronous form of [decorate] by name: returns at once; the call runs guarded by the three instances
     * registered under [name], and the future completes with its result or exceptionally with exactly the
     * throwable the blocking form would have thrown (`TimeoutException`, `CallNotPermittedException`,
     * `BulkheadFullException`, the call's own exception - never wrapped; `get()` and `join()` add their own
     * `ExecutionException` / `CompletionException` as every `CompletableFuture` does).
     */
    fun <T : Any> decorateAsync(
        name: String,
        call: () -> T,
    ): CompletableFuture<T> =
        decorateAsync(
            circuitBreakerRegistry.circuitBreaker(name),
            timeLimiterRegistry.timeLimiter(name),
            bulkheadRegistry.bulkhead(name),
            call,
        )

    /**
     * The asynchronous form of [decorate] with the three instances handed in directly (see the class KDoc,
     * "The asynchronous form").
     *
     * Cancelling the returned future (`cancel(...)`, with either argument - the flag is meaningless on a
     * `CompletableFuture` and ignored here as well) interrupts the waiter and through it the worker: the
     * blocking operation is abandoned, the bulkhead permit released, and the breaker sees the caller's
     * `InterruptedException` - one of [SELF_INFLICTED_FAILURES], which it must be told to ignore. A call
     * submitted after [shutdown] is refused with a `RejectedExecutionException` - delivered on the future, not
     * thrown, because an asynchronous caller expects every outcome there; the breaker never saw that call.
     *
     * The caller's MDC and observation are captured on the calling thread, as in the blocking form, and reach the
     * worker through the waiter.
     */
    fun <T : Any> decorateAsync(
        circuitBreaker: CircuitBreaker,
        timeLimiter: TimeLimiter,
        bulkhead: Bulkhead,
        call: () -> T,
    ): CompletableFuture<T> {
        val result = OffloadedFuture<T>()
        // Captured HERE, on the real caller's thread; the waiter then carries it, and decorate() captures it
        // again from the waiter for the worker.
        val guarded = propagate { decorate(circuitBreaker, timeLimiter, bulkhead, call) }
        val waiter =
            try {
                executor.submit {
                    try {
                        result.complete(guarded.call())
                    } catch (t: Throwable) {
                        result.completeExceptionally(t)
                    }
                }
            } catch (e: RejectedExecutionException) {
                result.completeExceptionally(e)
                return result
            }
        result.waiter.set(waiter)
        // Safety: a cancel() that arrived before the line above found no waiter to interrupt; do it now. Nothing
        // can reach `result` before it is returned below today, so this is unreachable as written - kept so that
        // a change publishing the future earlier (a callback, a registry of in-flight calls) cannot reopen the
        // window in which a cancelled call would keep its worker and its permit.
        if (result.isCancelled) waiter.cancel(true)
        return result
    }

    /**
     * The configured TimeLimiter timeout for [name] - the end-to-end budget of a call decorated under that name.
     * Exposed so a transport can derive a *tighter* response-phase guard from it, rather than a second
     * independent timeout that could drift past the budget.
     */
    fun requestBudget(name: String): Duration = timeLimiterRegistry.timeLimiter(name).timeLimiterConfig.timeoutDuration

    /**
     * Wraps [call] so that it runs on the worker with the CALLER's context: the MDC as the caller has it now,
     * and the caller's current observation reopened as a scope. Captured on the calling thread at decoration
     * time, which is the moment `decorate` runs - the same moment a reactive `contextCapture()` would read the
     * subscriber's thread-locals.
     *
     * Restoration is unconditional and in `finally`: the virtual workers of this class are never reused, but the
     * wrapper must not depend on that - a thread that runs it twice must not carry one caller's keys or scope
     * into the next call. `internal` so `BlockingResilienceDecoratorTest` can prove exactly that on a REUSED
     * thread of its own, which the decorator's fresh-thread executor never offers.
     */
    internal fun <T : Any> propagate(call: () -> T): Callable<T> {
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
     * `RejectedExecutionException`, which a breaker configured with [SELF_INFLICTED_FAILURES] ignores.
     *
     * Annotated for a host that declares this class as a bean; a standalone user calls it when done.
     */
    @PreDestroy
    fun shutdown() {
        executor.shutdownNow()
        logger.debug("Blocking call executor '{}' shut down", workerNamePrefix)
    }

    /**
     * The future [decorateAsync] hands out: a `CompletableFuture` whose cancellation reaches the waiter thread.
     * `newIncompleteFuture` returns a plain future so that derived stages (`thenApply`, ...) do not inherit the
     * override - cancelling a derived stage must not cancel the call, as it does not for any other
     * `CompletableFuture`.
     */
    private class OffloadedFuture<T> : CompletableFuture<T>() {
        val waiter = AtomicReference<Future<*>>()

        override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
            val cancelled = super.cancel(mayInterruptIfRunning)
            waiter.get()?.cancel(true)
            return cancelled
        }

        override fun <U> newIncompleteFuture(): CompletableFuture<U> = CompletableFuture()
    }

    companion object {
        /** The worker-thread name prefix when a host names none. */
        const val DEFAULT_WORKER_NAME_PREFIX: String = "veredium-call-"

        /**
         * The failures this pipeline raises ITSELF - the backend was never called, or the caller stopped waiting
         * - which a circuit breaker used with this decorator must therefore not count. The breaker config has
         * to be told (see the class KDoc): a stack's [ClientSideFailurePolicy] lists them next to its own buffer
         * limit, a standalone user hands them to `CircuitBreakerConfig.ignoreExceptions`.
         *
         * - [BulkheadFullException]: the bulkhead rejected the call; the breaker, outermost, sees it like any
         *   other error.
         * - [InterruptedException]: the CALLER went away while waiting for the worker - its own deadline expired,
         *   its scope was cancelled, its executor is shutting down. Rethrown out of the decorated call, so the
         *   breaker records it unless told otherwise.
         * - [RejectedExecutionException]: the worker executor refused the submission because the decorator is
         *   already shut down - a call made during context teardown. Submitted INSIDE the callable the breaker
         *   wraps (so that a short-circuited call submits nothing), which puts the refusal on the breaker's path.
         */
        val SELF_INFLICTED_FAILURES: List<Class<out Throwable>> =
            listOf(
                BulkheadFullException::class.java,
                InterruptedException::class.java,
                RejectedExecutionException::class.java,
            )
    }
}
