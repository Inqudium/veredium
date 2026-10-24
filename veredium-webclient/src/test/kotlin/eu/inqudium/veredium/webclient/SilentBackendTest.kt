package eu.inqudium.veredium.webclient

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.TimeUnit

/**
 * [SilentBackend], driven by a plain socket instead of an HTTP client: the moment it reports the connection closed is
 * what the abort cases time an engine by, so it has to be the client's close and nothing else.
 */
class SilentBackendTest {
    @Test
    fun `should report a request that arrived and the client's close, and answer nothing`() {
        // What is tested: SilentBackend against a client that sends a request and then closes its socket.
        // Success criteria: requestArrived opens; the client reads no byte within 300 ms; after the close
        //   connectionClosed opens and closedAtNanos lies between the close and the check.
        // Why it matters: the suites time an engine's abort by the moment the backend saw the connection closed;
        //   a timestamp taken elsewhere would measure the test, not the engine.
        //
        // Given
        SilentBackend().use { backend ->
            val client =
                Socket().apply {
                    soTimeout = 300
                    connect(InetSocketAddress(InetAddress.getLoopbackAddress(), backend.authority.substringAfterLast(':').toInt()))
                }
            client.getOutputStream().write("GET /silent HTTP/1.1\r\nHost: backend\r\n\r\n".toByteArray(Charsets.ISO_8859_1))
            assertThat(backend.requestArrived.await(5, TimeUnit.SECONDS)).isTrue()
            val answered = runCatching { client.getInputStream().read() }

            // When
            val closed = System.nanoTime()
            client.close()

            // Then
            assertThat(answered.exceptionOrNull()).`as`("no byte within the read timeout").isInstanceOf(SocketTimeoutException::class.java)
            assertThat(backend.connectionClosed.await(5, TimeUnit.SECONDS)).isTrue()
            assertThat(backend.closedAtNanos).isBetween(closed, System.nanoTime())
        }
    }
}
