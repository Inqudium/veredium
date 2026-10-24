package eu.inqudium.veredium.restclient

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.io.InterruptedIOException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit

/**
 * The hand-off of the Reactor Netty engine's two blocking waits ([ReactorNettyEngineTransport.handOff]), over a
 * wait that nothing but an interrupt ends - the case no real exchange produces on demand: an exchange whose end
 * the disposed event loops never signal.
 *
 * That the engine runs its head wait through this hand-off, and shuts its offload executor down, is pinned at the
 * engine in `RestClientEngineIntegrationTest` (`The shutdown`), with the event loops held so that nothing else can
 * end the call.
 */
class ReactorNettyHandOffTest {
    private val offload: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()
    private val workers: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()

    @AfterEach
    fun shutDown() {
        offload.shutdownNow()
        workers.shutdownNow()
    }

    @Test
    fun `should fail a wait nothing else ends at once when the engine's offload executor is shut down`() {
        // What is tested: a worker waiting through handOff on a task that only an interrupt ends, when the
        //   offload executor is shut down with shutdownNow - what the engine's shutdown does last.
        // Success criteria: the worker's wait fails within 2 s with an IOException whose cause is the task's
        //   InterruptedException - not an InterruptedIOException, since the worker itself was not interrupted.
        // Why it matters: Spring's wait for the head is a block() without a time bound, and the response timeout
        //   is a timer on the event loops. Once a shutdown past its deadline has disposed the loops, nothing
        //   signals the exchange's end, and it would hold its caller until the TimeLimiter budget; the shutdown
        //   of the offload executor is what ends it.
        //
        // Given: a worker waiting on a task that never ends by itself
        val started = CountDownLatch(1)
        val wait =
            CompletableFuture.supplyAsync(
                {
                    catchThrowable {
                        ReactorNettyEngineTransport.handOff(offload, "Request") {
                            started.countDown()
                            CountDownLatch(1).await()
                        }
                    }
                },
                workers,
            )
        assertThat(started.await(2, TimeUnit.SECONDS)).`as`("the task must have started").isTrue()

        // When
        offload.shutdownNow()

        // Then
        val failure = wait.get(2, TimeUnit.SECONDS)
        assertThat(failure).isExactlyInstanceOf(IOException::class.java).hasCauseInstanceOf(InterruptedException::class.java)
    }

    @Test
    fun `should interrupt the head's task and fail with an InterruptedIOException when the worker is interrupted`() {
        // What is tested: handOff with its default onInterrupt (the head's), when the WORKER is interrupted - the
        //   TimeLimiter's abort, a caller interrupt.
        // Success criteria: the worker fails with an InterruptedIOException and keeps its interrupt flag; the
        //   task, which honours an interrupt as Reactor's block() does, is interrupted too and ends.
        // Why it matters: an interrupt that reaches Spring's block() directly cancels the subscription and with
        //   it the exchange. With the head handed off, the interrupt reaches the worker instead, which must pass
        //   it on, or the exchange would keep its connection until the response timeout.
        //
        // Given
        val started = CountDownLatch(1)
        val taskInterrupted = CountDownLatch(1)
        val outcome = CompletableFuture<Pair<Throwable?, Boolean>>()
        val worker =
            Thread.ofVirtual().start {
                val failure =
                    catchThrowable {
                        ReactorNettyEngineTransport.handOff(offload, "Request") {
                            started.countDown()
                            try {
                                CountDownLatch(1).await()
                            } catch (e: InterruptedException) {
                                taskInterrupted.countDown()
                                throw e
                            }
                        }
                    }
                outcome.complete(failure to Thread.currentThread().isInterrupted)
            }
        assertThat(started.await(2, TimeUnit.SECONDS)).`as`("the task must have started").isTrue()

        // When
        worker.interrupt()

        // Then
        val (failure, flagKept) = outcome.get(2, TimeUnit.SECONDS)
        assertThat(failure).isExactlyInstanceOf(InterruptedIOException::class.java).hasMessage("Request interrupted")
        assertThat(flagKept).`as`("the worker keeps its interrupt flag").isTrue()
        assertThat(taskInterrupted.await(2, TimeUnit.SECONDS)).`as`("the task must be interrupted").isTrue()
    }

    @Test
    fun `should refuse a wait started after the engine's shutdown with an IOException naming it`() {
        // What is tested: handOff once the offload executor is shut down - a client built before the factory's
        //   shutdown and called after it.
        // Success criteria: an IOException naming the engine and that it is shut down, the executor's
        //   RejectedExecutionException as its cause.
        // Why it matters: the caller must see the transport failure an exchange on a closed engine is, not a bare
        //   RejectedExecutionException that Spring would not wrap and the breaker would classify differently.
        //
        // Given
        offload.shutdownNow()

        // When
        val failure = catchThrowable { ReactorNettyEngineTransport.handOff<Unit>(offload, "Request") { } }

        // Then
        assertThat(failure)
            .isExactlyInstanceOf(IOException::class.java)
            .hasMessage("Request refused: the HTTP engine 'reactor-netty' is shut down")
            .hasCauseInstanceOf(RejectedExecutionException::class.java)
    }
}
