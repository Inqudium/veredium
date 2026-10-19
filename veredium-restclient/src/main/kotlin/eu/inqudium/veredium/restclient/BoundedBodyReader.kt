package eu.inqudium.veredium.restclient

import java.io.ByteArrayOutputStream
import java.io.InputStream

/**
 * A response body that was larger than the client is willing to buffer - the blocking counterpart of the
 * reactive codec's `DataBufferLimitException`.
 *
 * The backend answered, and with a success status: this is a CLIENT-side refusal, which is why the circuit
 * breaker is told to discount it ([RestClientVerdicts.clientSideFailure]) and why the one line that reports it names the
 * configured limit ([ResilientRestClient]). [contentLength] is the `Content-Length` the response declared, or
 * `-1` when it declared none (chunked encoding) - a LOWER bound on the body, since the header is the wire size
 * and the body may have been compressed.
 */
class ResponseBodyLimitExceededException(
    val backendName: String,
    val limitBytes: Int,
    val contentLength: Long,
) : RuntimeException(
        "Response body from backend '$backendName' exceeded the buffer limit of $limitBytes bytes" +
            if (contentLength >= 0) " (Content-Length: $contentLength)" else " (Content-Length: unknown)",
    )

/**
 * Reads a response body into memory under a hard byte limit.
 *
 * Spring's blocking `RestClient` has no `maxInMemorySize`: its message converters read whatever the peer sends,
 * so a single degenerate response could take the pod down. The WebClient twin gets that bound for free from the
 * reactive codec; this module enforces it itself, at the one place every response passes through - the
 * exchange function in [ResilientRestClient] - so that the ceiling means the same thing on both stacks: the
 * DECODED bytes buffered per concurrently handled response. Apache HttpClient decompresses transparently, so the
 * count here is after decompression, exactly as the codec counts.
 *
 * Two reads, for the two kinds of body:
 *  - [readOrNull] for a SUCCESS body, which has to arrive whole or not at all: a body that does not fit is
 *    reported as `null`, so the caller can raise [ResponseBodyLimitExceededException] with what it knows.
 *  - [readTruncated] for an ERROR body, which is carried on the exception for diagnosis only: a body past the
 *    limit is cut, never rejected. A 5xx must stay a 5xx for the breaker whatever size its error page has, and
 *    truncating is what keeps the "oversized error body" case from ever carrying a client-side failure as its
 *    cause (see [RestClientVerdicts.clientSideFailure]).
 *
 * The remainder of an over-limit body is NOT read here. Closing the response afterwards hands the stream back
 * to Apache HttpClient, which drains or discards the connection as its own rules dictate - that is the
 * transport's business, and the limit is about memory, not about the wire.
 */
object BoundedBodyReader {
    private const val CHUNK_SIZE = 8 * 1024

    /** The whole body when it fits into [limitBytes], otherwise `null`. */
    fun readOrNull(
        input: InputStream,
        limitBytes: Int,
    ): ByteArray? {
        val buffer = ByteArrayOutputStream()
        val chunk = ByteArray(CHUNK_SIZE)
        while (true) {
            // Never ask for more than one byte past the limit: that one byte is what distinguishes "exactly at
            // the limit" (fits) from "past it" (rejected) without reading the rest of an oversized body.
            // Computed in Long: RestClientProperties admits a limit of Int.MAX_VALUE, for which `limitBytes + 1`
            // wraps to Int.MIN_VALUE in Int arithmetic and the read below throws on a negative length.
            val wanted = minOf(chunk.size.toLong(), limitBytes + 1L - buffer.size()).toInt()
            val read = input.read(chunk, 0, wanted)
            if (read < 0) return buffer.toByteArray()
            buffer.write(chunk, 0, read)
            if (buffer.size() > limitBytes) return null
        }
    }

    /** At most [limitBytes] of the body; whatever lies beyond is left unread. */
    fun readTruncated(
        input: InputStream,
        limitBytes: Int,
    ): ByteArray {
        val buffer = ByteArrayOutputStream()
        val chunk = ByteArray(CHUNK_SIZE)
        while (buffer.size() < limitBytes) {
            val read = input.read(chunk, 0, minOf(chunk.size, limitBytes - buffer.size()))
            if (read < 0) break
            buffer.write(chunk, 0, read)
        }
        return buffer.toByteArray()
    }
}
