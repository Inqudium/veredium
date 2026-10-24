package eu.inqudium.veredium.core.engine

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport

/**
 * The lifecycle [EngineSet] gives the engines of a client factory on both stacks, over engines that record what is done
 * to them - the failure paths no real engine produces on demand: a release that throws, a creation that fails half way,
 * a client built after the shutdown.
 */
class EngineSetTest {
    private val logger = LoggerFactory.getLogger(EngineSetTest::class.java) as Logger
    private val lines = ListAppender<ILoggingEvent>().apply { start() }.also { logger.addAppender(it) }

    @AfterEach
    fun detach() {
        logger.detachAppender(lines)
    }

    @Nested
    inner class `Creating the engines with the set` {
        @Test
        fun `should create every engine in use with the set, in the order given`() {
            // What is tested: the construction of an EngineSet over two of three engines.
            // Success criteria: exactly the two engines are created, in the order given, before any use.
            // Why it matters: an engine is created with its factory, so a misconfigured engine fails the context start,
            //   not the first call - and an engine no backend selected is never created, so it needs no library.
            //
            // Given
            val created = mutableListOf<TestEngine>()

            // When
            val engines = EngineSet(listOf(TestEngine.ONE, TestEngine.THREE), OWNER, logger) { engine -> RecordingEngine(engine).also { created += engine } }

            // Then
            assertThat(created).containsExactly(TestEngine.ONE, TestEngine.THREE)
            assertThat(engines.all.keys).containsExactly(TestEngine.ONE, TestEngine.THREE)
        }

        @Test
        fun `should release the engines already created when a later one fails to construct, then rethrow`() {
            // What is tested: the construction of an EngineSet when the third of three engines fails in its
            //   constructor.
            // Success criteria: the failure propagates unchanged; the two engines created before it were released, with
            //   a deadline already reached.
            // Why it matters: the failure fails the factory's construction, so its destroy callback never runs - an
            //   Apache evictor or a started Jetty client created before would leak, and Jetty's non-daemon threads
            //   would keep the JVM of a failed start alive.
            //
            // Given
            val created = mutableListOf<RecordingEngine>()
            val failure = IllegalStateException("cannot start")

            // When / Then
            assertThatThrownBy {
                EngineSet(TestEngine.entries, OWNER, logger) { engine ->
                    if (engine == TestEngine.THREE) throw failure
                    RecordingEngine(engine).also { created += it }
                }
            }.isSameAs(failure)
            assertThat(created.map { it.engine }).containsExactly(TestEngine.ONE, TestEngine.TWO)
            assertThat(created.map { it.deadlineReachedAtShutdown }).containsExactly(true, true)
        }
    }

    @Nested
    inner class `Handing out an engine` {
        @Test
        fun `should refuse an engine after the shutdown`() {
            // What is tested: use() after shutdown().
            // Success criteria: it fails with an IllegalStateException naming the owner and the shutdown, and the block
            //   does not run.
            // Why it matters: a client built after the release would run on an engine whose pool and threads are gone -
            //   its first call would fail far from the cause.
            //
            // Given
            val engines = EngineSet(listOf(TestEngine.ONE), OWNER, logger) { RecordingEngine(it) }
            engines.shutdown(DEADLINE) { 0L }
            var ran = false

            // When / Then
            assertThatThrownBy { engines.use(TestEngine.ONE, "late") { ran = true } }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessage("$OWNER was shut down; the client of backend 'late' would run on a released HTTP engine")
            assertThat(ran).isFalse()
        }

        @Test
        fun `should release an engine only after a client build that holds it has ended, and refuse the next one`() {
            // What is tested: shutdown() on another thread while a use() block holds the engine open on a latch, then
            //   use() once the shutdown has returned.
            // Success criteria: the shutdown thread waits; the engine is not released while the block runs, and is
            //   released after the block ended; the later use() fails with the refusal and its block does not run.
            // Why it matters: the lock is what keeps a client built during the context's shutdown from receiving an
            //   engine the shutdown is releasing under it - its first call would then fail on a closed pool, far from
            //   the cause. A single-threaded test cannot tell the lock from its absence.
            //
            // Given: a use() block that holds the engine until released
            val inBlock = AtomicBoolean(false)
            val releasedDuringBlock = AtomicReference<Boolean>()
            val engines = EngineSet(listOf(TestEngine.ONE), OWNER, logger) { RecordingEngine(it, onShutdown = { releasedDuringBlock.set(inBlock.get()) }) }
            val entered = CountDownLatch(1)
            val leave = CountDownLatch(1)
            val builder =
                Thread.ofPlatform().start {
                    engines.use(TestEngine.ONE, "early") {
                        inBlock.set(true)
                        entered.countDown()
                        leave.await(5, TimeUnit.SECONDS)
                        inBlock.set(false)
                    }
                }
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()

            // When: the shutdown runs while the block holds the engine
            val shutdown = Thread.ofPlatform().start { engines.shutdown(DEADLINE) { 0L } }
            val shutdownWaited = eventually { shutdown.state == Thread.State.WAITING || !shutdown.isAlive }
            val releasedWhileHeld = engines.all.getValue(TestEngine.ONE).isClosed
            leave.countDown()
            builder.join(5_000)
            shutdown.join(5_000)
            var ranLate = false
            val late = catchThrowable { engines.use(TestEngine.ONE, "late") { ranLate = true } }

            // Then
            assertThat(shutdownWaited).`as`("the shutdown thread waited for the lock").isTrue()
            assertThat(releasedWhileHeld).`as`("released while the block held the engine").isFalse()
            assertThat(releasedDuringBlock.get()).`as`("the release ran inside the block").isFalse()
            assertThat(shutdown.isAlive).isFalse()
            assertThat(late).isInstanceOf(IllegalStateException::class.java).hasMessageContaining("was shut down")
            assertThat(ranLate).isFalse()
        }

        @Test
        fun `should refuse an engine no backend selected`() {
            // What is tested: use() for an engine the set was not created with.
            // Success criteria: an IllegalStateException naming the engine and the owner.
            // Why it matters: the factory resolves every backend's engine at construction; asking for another one is a
            //   programming error that must not create resources nothing would release.
            //
            // Given
            val engines = EngineSet(listOf(TestEngine.ONE), OWNER, logger) { RecordingEngine(it) }

            // When / Then
            assertThatThrownBy { engines.use(TestEngine.TWO, "other") { } }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessage("HTTP engine 'two' was not selected for any backend of $OWNER")
        }
    }

    @Nested
    inner class `Releasing the engines against one deadline` {
        @Test
        fun `should hand every engine the same deadline, so the release takes the bound once`() {
            // What is tested: shutdown() over three engines.
            // Success criteria: all three receive the one deadline passed to shutdown().
            // Why it matters: the engines are released one after another; a bound per engine would let a shutdown take
            //   that bound once per engine - the destroy callback of a factory on three engines three times as long.
            //
            // Given
            val engines = EngineSet(TestEngine.entries, OWNER, logger) { RecordingEngine(it) }

            // When
            engines.shutdown(DEADLINE) { 0L }

            // Then
            assertThat(engines.all.values.map { it.shutdownDeadline }).containsExactly(DEADLINE, DEADLINE, DEADLINE)
        }

        @Test
        fun `should release every engine although one of them throws, and report the one that did`() {
            // What is tested: shutdown() over three engines, the middle one's release throwing.
            // Success criteria: all three were released; no exception leaves shutdown(); one WARN names the failing
            //   engine and carries its exception.
            // Why it matters: shutdown() runs from the factory's destroy callback. An exception there would end the
            //   shutdown at the failing engine - the ones after it keep their pools, loops and threads, and Spring logs
            //   a stack trace where the outcome line should be.
            //
            // Given
            val engines =
                EngineSet(TestEngine.entries, OWNER, logger) { engine ->
                    RecordingEngine(engine, failShutdown = IllegalStateException("stop failed").takeIf { engine == TestEngine.TWO })
                }

            // When
            engines.shutdown(DEADLINE) { 0L }

            // Then
            assertThat(engines.all.values.map { it.shutdownDeadline }).containsExactly(DEADLINE, DEADLINE, DEADLINE)
            val warning = lines.list.single { it.level == Level.WARN }
            assertThat(warning.formattedMessage).isEqualTo("HTTP engine 'two' failed to release its resources")
            assertThat(warning.throwableProxy.message).isEqualTo("stop failed")
        }

        @Test
        fun `should take a second shutdown without a failure and stay closed`() {
            // What is tested: shutdown() called twice, as Spring's destroy callback and a test's own call may.
            // Success criteria: neither call throws or logs a WARN; every engine is closed after the first and is
            //   handed the second call too - closing again is each engine's own idempotency, which the transports
            //   state; use() is refused afterwards.
            // Why it matters: the class KDoc promises an idempotent release. A second call that threw would fail the
            //   destroy callback, and one that reopened the set would hand out a released engine.
            //
            // Given
            val engines = EngineSet(TestEngine.entries, OWNER, logger) { RecordingEngine(it) }
            engines.shutdown(DEADLINE) { 0L }
            val closedAfterFirst = engines.all.values.map { it.isClosed }

            // When
            engines.shutdown(DEADLINE) { 0L }

            // Then
            assertThat(closedAfterFirst).containsOnly(true)
            assertThat(engines.all.values.map { it.shutdowns }).containsExactly(2, 2, 2)
            assertThat(lines.list.filter { it.level == Level.WARN }).isEmpty()
            assertThatThrownBy { engines.use(TestEngine.ONE, "late") { } }.isInstanceOf(IllegalStateException::class.java)
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private enum class TestEngine(
        override val key: String,
    ) : HttpEngine {
        ONE("one"),
        TWO("two"),
        THREE("three"),
        ;

        override val requiredClasses: List<String> = emptyList()
        override val dependency: String = "none"
    }

    /** An engine that records its release, runs [onShutdown] in it, and throws [failShutdown] from it when given. */
    private class RecordingEngine(
        val engine: TestEngine,
        private val failShutdown: RuntimeException? = null,
        private val onShutdown: () -> Unit = {},
    ) : EngineResources {
        @Volatile
        var shutdownDeadline: Long? = null
        var deadlineReachedAtShutdown: Boolean? = null
        var shutdowns = 0

        override val key: String = engine.key

        override val isClosed: Boolean get() = shutdownDeadline != null

        override fun shutdown(
            deadlineNanos: Long,
            nanoTime: () -> Long,
        ) {
            onShutdown()
            shutdowns++
            shutdownDeadline = deadlineNanos
            deadlineReachedAtShutdown = nanoTime() - deadlineNanos >= 0
            failShutdown?.let { throw it }
        }
    }

    private companion object {
        const val OWNER = "TestFactory"
        const val DEADLINE = 42_000L

        /** Whether [condition] holds within 5 s, polled every 10 ms - the core's tests cannot use the test-support one. */
        fun eventually(condition: () -> Boolean): Boolean {
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
            while (!condition()) {
                if (System.nanoTime() - deadline >= 0) return false
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10))
            }
            return true
        }
    }
}
