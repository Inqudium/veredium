package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.slf4j.helpers.NOPLogger
import org.springframework.http.client.reactive.ClientHttpConnector
import org.springframework.http.client.reactive.JdkClientHttpConnector
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The lifecycle [EngineTransports] gives the engines of one factory - the part that does not depend on any
 * engine library, driven here with recording stand-ins: one deadline for all releases, a failing release that
 * does not stop the others, and no engine created after the release.
 */
class EngineTransportsTest {
    private val endpoint: ResolvedTimeouts =
        MeshProperties(endpoints = listOf(MeshProperties.EndpointConfig(triplet = "/endpoint/localhost/GET/a", alias = "a")))
            .resolveAll()
            .values
            .single()

    @Test
    fun `should hand every engine the same deadline, so the release takes the bound once`() {
        // What is tested: release() over two started engines.
        // Success criteria: both engines receive the one deadline passed to release(), in the order created.
        // Why it matters: the engines are released one after another; a bound per engine would let a shutdown
        //   take that bound once per engine - the @PreDestroy of a factory on three engines three times as long.
        //
        // Given
        val released = CopyOnWriteArrayList<Pair<WebClientEngine, Long>>()
        val transports = EngineTransports(setOf(WebClientEngine.JDK, WebClientEngine.APACHE), NOPLogger.NOP_LOGGER) { RecordingTransport(it, released) }
        transports.connectorFor(WebClientEngine.JDK, endpoint)
        transports.connectorFor(WebClientEngine.APACHE, endpoint)

        // When
        transports.release(DEADLINE) { 0L }

        // Then
        assertThat(released).containsExactlyInAnyOrder(WebClientEngine.JDK to DEADLINE, WebClientEngine.APACHE to DEADLINE)
    }

    @Test
    fun `should release the other engines when one release fails`() {
        // What is tested: a release that throws.
        // Success criteria: release() does not throw, and the other engine is released all the same.
        // Why it matters: release() runs from a destroy callback; one engine's failure must neither leak the
        //   others nor surface as a stack trace where the outcome line belongs.
        //
        // Given
        val released = CopyOnWriteArrayList<Pair<WebClientEngine, Long>>()
        val transports =
            EngineTransports(setOf(WebClientEngine.JETTY, WebClientEngine.JDK), NOPLogger.NOP_LOGGER) { engine ->
                RecordingTransport(engine, released, fails = engine == WebClientEngine.JETTY)
            }
        transports.connectorFor(WebClientEngine.JETTY, endpoint)
        transports.connectorFor(WebClientEngine.JDK, endpoint)

        // When
        transports.release(DEADLINE) { 0L }

        // Then
        assertThat(released).contains(WebClientEngine.JDK to DEADLINE)
    }

    @Test
    fun `should create no engine after the release`() {
        // What is tested: connectorFor() for an engine in use that no client had created before release().
        // Success criteria: it fails with an IllegalStateException naming the shutdown, and the engine is not
        //   created.
        // Why it matters: an engine created after the release owns threads and sockets nothing would release.
        //
        // Given
        val created = CopyOnWriteArrayList<WebClientEngine>()
        val transports =
            EngineTransports(setOf(WebClientEngine.JDK), NOPLogger.NOP_LOGGER) { engine ->
                created += engine
                RecordingTransport(engine, CopyOnWriteArrayList())
            }
        transports.release(DEADLINE) { 0L }

        // When/Then
        assertThatThrownBy { transports.connectorFor(WebClientEngine.JDK, endpoint) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("shut down")
        assertThat(created).isEmpty()
        assertThat(transports.started()).isEmpty()
    }

    /** An engine that records the deadline it was released against, and optionally fails doing so. */
    private class RecordingTransport(
        override val engine: WebClientEngine,
        private val released: MutableList<Pair<WebClientEngine, Long>>,
        private val fails: Boolean = false,
    ) : EngineTransport {
        override fun connectorFor(endpoint: ResolvedTimeouts): ClientHttpConnector = JdkClientHttpConnector()

        override fun release(
            deadlineNanos: Long,
            nanoTime: () -> Long,
        ) {
            check(!fails) { "release failed" }
            released += engine to deadlineNanos
        }

        override fun isReleased(): Boolean = released.any { it.first == engine }
    }

    private companion object {
        const val DEADLINE = 42_000L
    }
}
