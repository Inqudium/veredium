package eu.inqudium.veredium.testsupport

import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import kotlin.concurrent.thread

/**
 * A raw-socket HTTP peer on the loopback ADDRESS - never `localhost`, which may resolve to `::1` first, where nothing
 * listens. WireMock cannot stand in for these: it reports requests, not connections, and it hands a delayed or
 * dribbled body to its output buffer, so the head reaches the wire only once that buffer is full.
 *
 * Every accepted connection is served on a daemon thread of its own and closed with the backend, so an exchange the
 * client kept open ends with the test.
 */
abstract class SocketBackend(
    backlog: Int = 50,
) : AutoCloseable {
    private val server = ServerSocket(0, backlog, InetAddress.getLoopbackAddress())

    /** `<loopback address>:<port>`, for a base URL or a mesh triplet. */
    val authority: String = "${server.inetAddress.hostAddress}:${server.localPort}"

    /** `http://` + [authority]. */
    val baseUrl: String = "http://$authority"

    /** The connections accepted so far. */
    val connections = AtomicInteger()

    /**
     * The connections whose [serve] has returned - the backend is done with them, whoever ended them. For this
     * module's own tests: a latch a test asserts is still closed proves nothing until the connection's thread, the one
     * that would open it, has finished.
     */
    internal val connectionsEnded = AtomicInteger()

    /**
     * Whether [close] was called. Set before any connection is closed, so a subclass that sees its connection fail can
     * tell its own shutdown from the client going away.
     */
    @Volatile
    protected var closing = false
        private set

    private val accepted = CopyOnWriteArrayList<Socket>()

    /** Invariant: under this lock, a socket is either registered in [accepted] before [close] runs or closed by the acceptor. */
    private val lifecycle = Any()

    /** Starts accepting; called by a subclass once its own state is initialised. */
    protected fun start(name: String) {
        thread(isDaemon = true, name = "$name-acceptor") {
            try {
                while (true) {
                    val socket = server.accept()
                    connections.incrementAndGet()
                    // Safety: a socket accepted while close() runs would be missed by its sweep and served on - for 30 s
                    // on a dripping backend - so registering and the closed check are one step.
                    val registered =
                        synchronized(lifecycle) {
                            if (!closing) accepted += socket
                            !closing
                        }
                    if (!registered) {
                        socket.close()
                        return@thread
                    }
                    thread(isDaemon = true, name = "$name-connection") {
                        try {
                            socket.use { serve(it) }
                        } finally {
                            connectionsEnded.incrementAndGet()
                        }
                    }
                }
            } catch (_: IOException) {
                // The server socket was closed - the test is over.
            }
        }
    }

    /** Serves one accepted connection; it is closed when this returns. */
    protected abstract fun serve(connection: Socket)

    override fun close() {
        synchronized(lifecycle) { closing = true }
        server.close()
        accepted.forEach { it.close() }
    }

    protected companion object {
        /** Reads one request head, up to its empty line; `false` at the end of the stream. The GETs here carry no body. */
        fun readHead(connection: Socket): Boolean {
            val input = connection.getInputStream()
            var matched = 0
            while (matched < HEAD_END.length) {
                val next = input.read()
                if (next < 0) return false
                matched =
                    when (next.toChar()) {
                        HEAD_END[matched] -> matched + 1
                        '\r' -> 1
                        else -> 0
                    }
            }
            return true
        }

        private const val HEAD_END = "\r\n\r\n"
    }
}

/** Answers every request with a 200 carrying [body] (JSON), keeping the connection alive; counts the [requests]. */
class AnsweringBackend(
    private val body: String = """{"message":"hi"}""",
) : SocketBackend() {
    /** The requests answered so far. */
    val requests = AtomicInteger()

    init {
        start("answering-backend")
    }

    override fun serve(connection: Socket) {
        val bytes = body.toByteArray(Charsets.UTF_8)
        val head = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${bytes.size}\r\n\r\n"
        try {
            while (readHead(connection)) {
                requests.incrementAndGet()
                connection.getOutputStream().apply {
                    write(head.toByteArray(Charsets.ISO_8859_1))
                    write(bytes)
                    flush()
                }
            }
        } catch (_: IOException) {
            // The client closed the connection.
        }
    }
}

/**
 * Answers every request with a 200 declaring a 1 MiB body, sends the head and the first 4 KiB at once ([headSent]) and
 * then one byte every 200 ms - a body that is still arriving long after any test is over, unless the client drops the
 * connection, which [connectionDropped] reports. With [cutAfterFirstChunk] it closes the connection after the first
 * 4 KiB instead: a peer that cuts the body off.
 */
class DrippingBackend(
    private val cutAfterFirstChunk: Boolean = false,
) : SocketBackend() {
    /** Counted down when the response head and the first 4 KiB were flushed to the client. */
    val headSent = CountDownLatch(1)

    /**
     * Counted down when the client went away before the response was complete: while the body was still arriving, or
     * before its request head was complete. Not by the backend's own [close], and not by [cutAfterFirstChunk]'s cut.
     */
    val connectionDropped = CountDownLatch(1)

    init {
        start("dripping-backend")
    }

    override fun serve(connection: Socket) {
        try {
            if (!readHead(connection)) {
                dropped()
                return
            }
            val out = connection.getOutputStream()
            out.write(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${1024 * 1024}\r\n\r\n"
                    .toByteArray(Charsets.ISO_8859_1),
            )
            out.write(ByteArray(4096) { 'z'.code.toByte() })
            out.flush()
            headSent.countDown()
            if (cutAfterFirstChunk) return
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
            while (System.nanoTime() - deadline < 0) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200))
                out.write('z'.code)
                out.flush()
            }
        } catch (_: IOException) {
            dropped()
        }
    }

    /** The client ended the exchange - unless the failure is this backend closing its own connections. */
    private fun dropped() {
        if (!closing) connectionDropped.countDown()
    }
}

/**
 * A listening socket whose accept queue is full, so the kernel leaves a further connect unanswered and it hangs until
 * the client's own connect timeout. Filled by plain connects until one of them times out itself; [saturated] is
 * `false` on a platform whose kernel refuses instead of dropping, where no connect can be held open.
 */
class UnansweredConnectBackend : AutoCloseable {
    private val server = ServerSocket(0, 1, InetAddress.getLoopbackAddress())

    /** `<loopback address>:<port>`. */
    val authority: String = "${server.inetAddress.hostAddress}:${server.localPort}"

    /** `http://` + [authority]. */
    val baseUrl: String = "http://$authority"

    private val probes = mutableListOf<Socket>()

    /** Whether the accept queue could be filled, i.e. whether a connect to this backend now hangs. */
    val saturated: Boolean = fill()

    private fun fill(): Boolean {
        repeat(MAX_PROBES) {
            val probe = Socket()
            try {
                probe.connect(InetSocketAddress(server.inetAddress, server.localPort), PROBE_TIMEOUT_MS)
                probes += probe
            } catch (_: SocketTimeoutException) {
                probe.close()
                return true
            } catch (_: IOException) {
                probe.close()
                return false
            }
        }
        return false
    }

    override fun close() {
        probes.forEach { it.close() }
        server.close()
    }

    private companion object {
        const val MAX_PROBES = 64
        const val PROBE_TIMEOUT_MS = 300
    }
}
