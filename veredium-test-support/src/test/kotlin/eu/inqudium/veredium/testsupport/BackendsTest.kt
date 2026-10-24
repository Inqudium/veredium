package eu.inqudium.veredium.testsupport

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.InputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.TimeUnit

/**
 * The raw-socket backends, driven by a plain socket instead of an HTTP client: the signals the client suites prove
 * their abort and release cases with have to fire for the client's doing and only for it.
 */
class BackendsTest {
    @Test
    fun `should report a client that leaves while the body is still dripping`() {
        // What is tested: DrippingBackend against a client that sends a request, waits for the head and the first
        //   chunk and closes its socket.
        // Success criteria: headSent opens; connectionDropped opens within 5 s of the close; the connection ends.
        // Why it matters: the suites prove an engine's body-phase abort with connectionDropped. A latch that did
        //   not see the client leave would turn a working abort red; that is the signal the abort cases rest on.
        //
        // Given
        DrippingBackend().use { backend ->
            val client = connect(backend)
            client.getOutputStream().write(REQUEST)
            assertThat(backend.headSent.await(5, TimeUnit.SECONDS)).`as`("the head was sent").isTrue()

            // When
            client.close()

            // Then
            assertThat(backend.connectionDropped.await(5, TimeUnit.SECONDS)).isTrue()
            assertThat(eventually { backend.connectionsEnded.get() == 1 }).isTrue()
        }
    }

    @Test
    fun `should report a client that leaves before its request head is complete`() {
        // What is tested: DrippingBackend against a client that sends half a request head and closes its socket.
        // Success criteria: connectionDropped opens within 5 s; headSent stays closed.
        // Why it matters: a client that aborts before its request is on the wire also gave up the exchange; a latch
        //   that only watched the body phase would leave such an abort unseen, and a case waiting on it would fail
        //   for the backend's sake rather than the client's.
        //
        // Given
        DrippingBackend().use { backend ->
            val client = connect(backend)
            client.getOutputStream().write("GET /drip HTTP/1.1\r\n".toByteArray(Charsets.ISO_8859_1))

            // When
            client.close()

            // Then
            assertThat(backend.connectionDropped.await(5, TimeUnit.SECONDS)).isTrue()
            assertThat(backend.headSent.count).isEqualTo(1)
        }
    }

    @Test
    fun `should not report a drop when the backend closes its own connections`() {
        // What is tested: DrippingBackend closed while a client that has received the head keeps its socket open.
        // Success criteria: the backend ends the connection - the client reads its end within 5 s - and
        //   connectionDropped is still closed once the connection's thread has finished.
        // Why it matters: every case closes its backend at the end. A latch that the backend's own close counted
        //   down would report a drop the client never made - in a case that awaits it after a failed abort, the
        //   false signal would turn the failure green.
        //
        // Given
        DrippingBackend().use { backend ->
            connect(backend).use { client ->
                client.getOutputStream().write(REQUEST)
                assertThat(backend.headSent.await(5, TimeUnit.SECONDS)).`as`("the head was sent").isTrue()

                // When
                backend.close()

                // Then
                assertThat(drain(client.getInputStream())).`as`("the client saw the connection end").isTrue()
                assertThat(eventually { backend.connectionsEnded.get() == 1 }).`as`("the connection's thread finished").isTrue()
                assertThat(backend.connectionDropped.count).isEqualTo(1)
            }
        }
    }

    @Test
    fun `should answer every request of a kept-alive connection and count them`() {
        // What is tested: AnsweringBackend against two requests sent one after the other on one socket.
        // Success criteria: both are answered with a 200 carrying the body; requests is 2 and connections is 1.
        // Why it matters: the suites count connections against it to prove pooling and isolation; a backend that
        //   closed after one answer would make every client open a second connection.
        //
        // Given
        AnsweringBackend("""{"message":"hi"}""").use { backend ->
            connect(backend).use { client ->
                // When
                val answers =
                    List(2) {
                        client.getOutputStream().write(REQUEST)
                        readAnswer(client.getInputStream())
                    }

                // Then
                assertThat(answers).allSatisfy { assertThat(it).startsWith("HTTP/1.1 200 OK").endsWith("""{"message":"hi"}""") }
                assertThat(backend.requests.get()).isEqualTo(2)
                assertThat(backend.connections.get()).isEqualTo(1)
            }
        }
    }

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
            val client = connect(backend)
            client.soTimeout = 300
            client.getOutputStream().write(REQUEST)
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

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** A plain client socket to [backend], whose reads give up after 5 s. */
    private fun connect(backend: SocketBackend) =
        Socket().apply {
            soTimeout = 5_000
            connect(InetSocketAddress(InetAddress.getLoopbackAddress(), backend.authority.substringAfterLast(':').toInt()))
        }

    /** Reads [input] to its end; `true` when it ended, `false` when a read failed instead. */
    private fun drain(input: InputStream): Boolean =
        runCatching {
            val buffer = ByteArray(8192)
            var read = 0
            while (read >= 0) read = input.read(buffer)
        }.isSuccess

    /** One answer of [AnsweringBackend]: the head up to its empty line, then as many body bytes as it declares. */
    private fun readAnswer(input: InputStream): String {
        val head = StringBuilder()
        while (!head.endsWith("\r\n\r\n")) head.append(input.read().toChar())
        val length =
            head
                .lines()
                .first { it.startsWith("Content-Length:") }
                .substringAfter(':')
                .trim()
                .toInt()
        return head.toString() + String(input.readNBytes(length), Charsets.UTF_8)
    }

    private companion object {
        val REQUEST: ByteArray = "GET /drip HTTP/1.1\r\nHost: backend\r\n\r\n".toByteArray(Charsets.ISO_8859_1)
    }
}
