package eu.inqudium.veredium.webclient

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * [AbortableExchange], the link between a cancelled subscription and the `Future` Apache returns for the
 * exchange: either side may come first, and the exchange must be aborted either way.
 */
class AbortableExchangeTest {
    @Test
    fun `should cancel the future when the abort comes after it was attached`() {
        // What is tested: attach, then abort - the usual order, the exchange already running.
        // Success criteria: the attached future is cancelled.
        // Why it matters: this is how a TimeLimiter abort reaches an exchange Spring's connector does not keep.
        //
        // Given
        val exchange = AbortableExchange()
        val started = CompletableFuture<Unit>()
        exchange.attach(started)

        // When
        exchange.abort()

        // Then
        assertThat(started).isCancelled()
    }

    @Test
    fun `should cancel the future on attachment when the abort came first`() {
        // What is tested: abort, then attach - the subscription cancelled before Apache handed out its future.
        // Success criteria: the future is cancelled the moment it is attached.
        // Why it matters: without it, an exchange aborted that early would run on with its pool slot until the
        //   response timeout, though nobody waits for it.
        //
        // Given
        val exchange = AbortableExchange()
        exchange.abort()
        val started = CompletableFuture<Unit>()

        // When
        exchange.attach(started)

        // Then
        assertThat(started).isCancelled()
    }

    @Test
    fun `should leave the future alone when nothing aborts`() {
        // What is tested: attach without an abort.
        // Success criteria: the future is not cancelled.
        // Why it matters: an exchange that completes normally must not be cut by its own bookkeeping.
        //
        // Given / When
        val started = CompletableFuture<Unit>()
        AbortableExchange().attach(started)

        // Then
        assertThat(started).isNotCancelled()
    }

    @Test
    fun `should cancel the future whichever side wins a race between abort and attach`() {
        // What is tested: abort and attach started together from two threads behind a shared barrier, many times.
        // Success criteria: in every round the future ends up cancelled.
        // Why it matters: the two calls come from different threads (the cancelling subscriber, Apache's
        //   execute); a window in which each misses the other would leave an aborted exchange running.
        //
        // Given
        val pool = Executors.newFixedThreadPool(2)
        try {
            repeat(ROUNDS) { round ->
                val exchange = AbortableExchange()
                val started = CompletableFuture<Unit>()
                val barrier = CyclicBarrier(2)

                // When
                val aborting = pool.submit { barrier.await(5, TimeUnit.SECONDS).also { exchange.abort() } }
                val attaching = pool.submit { barrier.await(5, TimeUnit.SECONDS).also { exchange.attach(started) } }
                aborting.get(5, TimeUnit.SECONDS)
                attaching.get(5, TimeUnit.SECONDS)

                // Then
                assertThat(started).`as`("round $round").isCancelled()
            }
        } finally {
            pool.shutdownNow()
        }
    }

    private companion object {
        const val ROUNDS = 1_000
    }
}
