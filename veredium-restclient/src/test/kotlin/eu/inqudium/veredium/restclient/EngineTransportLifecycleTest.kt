package eu.inqudium.veredium.restclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.http.client.ClientHttpRequestFactory
import java.time.Duration

/**
 * The lifecycle helpers every engine's creation and shutdown run through ([EngineTransport.createAll],
 * [EngineTransport.shutdownAll]), over transports that record what is done to them - the failure paths no real
 * engine produces on demand: a shutdown that throws, a construction that fails half way.
 */
class EngineTransportLifecycleTest {
    private val logger = LoggerFactory.getLogger(EngineTransportLifecycleTest::class.java) as Logger
    private val lines = ListAppender<ILoggingEvent>().apply { start() }.also { logger.addAppender(it) }

    @AfterEach
    fun detach() {
        logger.detachAppender(lines)
    }

    @Test
    fun `should shut down every engine although one of them throws, and report the one that did`() {
        // What is tested: shutdownAll over three transports, the middle one's shutdown throwing.
        // Success criteria: all three were asked to shut down against the same deadline; no exception leaves
        //   shutdownAll; one WARN names the failing engine and carries its exception.
        // Why it matters: shutdownAll runs from the factory's destroy callback. An exception there would end the
        //   shutdown at the failing engine - the ones after it keep their pools, loops and threads, and Spring
        //   logs a stack trace where the outcome line should be.
        //
        // Given
        val first = RecordingTransport(RestClientEngine.APACHE)
        val failing = RecordingTransport(RestClientEngine.JETTY, failShutdown = IllegalStateException("stop failed"))
        val last = RecordingTransport(RestClientEngine.REACTOR_NETTY)

        // When
        EngineTransport.shutdownAll(
            listOf(first, failing, last),
            deadlineNanos = 42L,
            nanoTime = { 0L },
            window = Duration.ofSeconds(1),
            logger = logger,
        )

        // Then
        assertThat(listOf(first, failing, last).map { it.shutdownDeadline }).containsExactly(42L, 42L, 42L)
        val warning = lines.list.single { it.level == Level.WARN }
        assertThat(warning.formattedMessage).isEqualTo("HTTP engine 'jetty' failed to release its resources")
        assertThat(warning.throwableProxy.message).isEqualTo("stop failed")
    }

    @Test
    fun `should shut down the engines already created when a later one fails to construct, then rethrow`() {
        // What is tested: createAll when the third of three engines fails in its constructor.
        // Success criteria: the failure propagates unchanged; the two engines created before it were shut down,
        //   with a deadline already reached; no transport is returned.
        // Why it matters: the failure fails the factory's construction, so its destroy callback never runs - an
        //   Apache evictor or a started Jetty client created before would leak, and Jetty's non-daemon threads
        //   would keep the JVM of a failed start alive.
        //
        // Given
        val created = mutableListOf<RecordingTransport>()
        val failure = IllegalStateException("cannot start")

        // When / Then
        assertThatThrownBy {
            EngineTransport.createAll(RestClientEngine.entries, logger) { engine ->
                if (engine == RestClientEngine.JETTY) throw failure
                RecordingTransport(engine).also { created += it }
            }
        }.isSameAs(failure)
        assertThat(created.map { it.engine }).containsExactly(RestClientEngine.APACHE, RestClientEngine.JDK)
        assertThat(created).allSatisfy { assertThat(it.shutdownDeadline).isNotNull() }
        assertThat(created.map { it.deadlineReachedAtShutdown }).containsExactly(true, true)
    }

    /** A transport that records its shutdown, and throws [failShutdown] from it when given. */
    private class RecordingTransport(
        engine: RestClientEngine,
        private val failShutdown: RuntimeException? = null,
    ) : EngineTransport(engine) {
        var shutdownDeadline: Long? = null
        var deadlineReachedAtShutdown: Boolean? = null

        override fun requestFactory(endpoint: ResolvedTimeouts): ClientHttpRequestFactory = error("not used")

        override val applied: List<String> = emptyList()
        override val notApplied: List<String> = emptyList()
        override val poolMeters: String = "none"
        override val cancellation: String = "none"
        override val isClosed: Boolean get() = shutdownDeadline != null

        override fun connectionsInUse(): Int = 0

        override fun shutdown(
            deadlineNanos: Long,
            nanoTime: () -> Long,
            window: Duration,
            logger: org.slf4j.Logger,
        ) {
            shutdownDeadline = deadlineNanos
            deadlineReachedAtShutdown = nanoTime() >= deadlineNanos
            failShutdown?.let { throw it }
        }
    }
}
