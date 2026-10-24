package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.testsupport.SocketBackend
import java.io.IOException
import java.net.Socket
import java.util.concurrent.CountDownLatch

/**
 * Accepts a connection, reads the request and never answers - so the only thing that can end the exchange is the
 * client. Records when the client closed the connection ([connectionClosed], [closedAtNanos]), which is what an abort
 * looks like on the wire for HTTP/1.1.
 */
class SilentBackend : SocketBackend() {
    /** Counted down when the first request's bytes arrived. */
    val requestArrived = CountDownLatch(1)

    /** Counted down when the client closed a connection. */
    val connectionClosed = CountDownLatch(1)

    /** The `System.nanoTime()` at which the client last closed a connection; 0 until it did. */
    @Volatile
    var closedAtNanos: Long = 0L
        private set

    init {
        start("silent-backend")
    }

    override fun serve(connection: Socket) {
        val buffer = ByteArray(8192)
        try {
            while (connection.getInputStream().read(buffer) >= 0) requestArrived.countDown()
        } catch (_: IOException) {
            // A reset is a close as well.
        }
        closedAtNanos = System.nanoTime()
        connectionClosed.countDown()
    }
}
