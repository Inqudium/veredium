package eu.inqudium.veredium.restclient

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
 *
 * @property backendName The backend whose response was refused - its mesh alias.
 * @property limitBytes The buffer limit the body exceeded, in bytes.
 * @property contentLength The declared `Content-Length`, or `-1` when the response declared none.
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
 * DECODED bytes buffered per concurrently handled response. Every engine of this module decompresses
 * transparently, so the count here is after decompression, exactly as the codec counts. The reader sees only an
 * `InputStream`, which is what keeps it engine-agnostic.
 *
 * Two reads, for the two kinds of body:
 *  - [readOrNull] for a SUCCESS body, which has to arrive whole or not at all: a body that does not fit is
 *    reported as `null`, so the caller can raise [ResponseBodyLimitExceededException] with what it knows.
 *  - [readTruncated] for an ERROR body, which is carried on the exception for diagnosis only: a body past the
 *    limit is cut, never rejected. A 5xx must stay a 5xx for the breaker whatever size its error page has, and
 *    truncating is what keeps the "oversized error body" case from ever carrying a client-side failure as its
 *    cause (see [RestClientVerdicts.clientSideFailure]).
 *
 * ## Memory
 *
 * The limit bounds what one response may cost, so the reader is careful not to multiply it. The bytes are read
 * straight into the array that is returned - no intermediate chunk, no `ByteArrayOutputStream` whose final
 * `toByteArray()` would double the peak. When the response declares a `Content-Length` (the usual case for a
 * JSON answer; the engines drop the header when they decompress, so a declared length is the decoded one), the
 * array is allocated at that size up front, capped at the limit so a lying header cannot reserve more than the
 * limit allows; a body of exactly that length is then returned without a single copy. Without a declared length
 * the array starts at 8 KiB and doubles, capped at the limit, and is trimmed to size once at the end.
 *
 * The remainder of an over-limit body is NOT read here. Closing the response afterwards hands the stream back
 * to the engine, and every engine of this module discards the connection rather than read the rest of a body
 * nobody wants - Spring's Apache and JDK responses would drain it, which is why their engines decorate the
 * response ([AbortingResponse]). That is the transport's business; the limit here is about memory.
 */
object BoundedBodyReader {
    private const val CHUNK_SIZE = 8 * 1024

    /** The largest array the JVM reliably allocates - the bound `ArrayList` and `ByteArrayOutputStream` use too. */
    private const val MAX_ARRAY_SIZE = Int.MAX_VALUE - 8

    /**
     * The whole body when it fits into [limitBytes], otherwise `null`. [contentLength] is the length the
     * response declared, or `-1` when it declared none; it only sizes the buffer and is never trusted as the
     * body's actual length.
     */
    fun readOrNull(
        input: InputStream,
        limitBytes: Int,
        contentLength: Long = -1,
    ): ByteArray? {
        // Read at most one byte past the limit: that one byte is what distinguishes "exactly at the limit" (fits)
        // from "past it" (rejected) without reading the rest of an oversized body. Computed in Long:
        // RestClientProperties admits a limit of Int.MAX_VALUE, for which `limitBytes + 1` wraps to
        // Int.MIN_VALUE in Int arithmetic.
        val body = readAtMost(input, limitBytes + 1L, initialCapacity(contentLength, limitBytes))
        return body.takeIf { it.size <= limitBytes }
    }

    /**
     * At most [limitBytes] of the body; whatever lies beyond is left unread. [contentLength] as for [readOrNull].
     *
     * A body that declared EXACTLY [limitBytes] and delivered them is complete, and one probe read sees its end at
     * once: without it the stream would count as not read to its end, and the engines' close would discard a
     * connection that has nothing left on it ([AbortingResponse]). Only then - for any other body at the limit, the
     * probe could wait for a peer that keeps sending.
     */
    fun readTruncated(
        input: InputStream,
        limitBytes: Int,
        contentLength: Long = -1,
    ): ByteArray =
        readAtMost(input, limitBytes.toLong(), initialCapacity(contentLength, limitBytes)).also { body ->
            if (body.size == limitBytes && contentLength == limitBytes.toLong()) input.read()
        }

    /**
     * The declared length when there is one, the first chunk otherwise - each capped at the limit, so neither a
     * lying header nor a tiny limit allocates more than the limit admits.
     */
    private fun initialCapacity(
        contentLength: Long,
        limitBytes: Int,
    ): Int = (if (contentLength >= 0) contentLength else CHUNK_SIZE.toLong()).coerceAtMost(limitBytes.toLong()).toInt()

    /** Reads until end of stream or until [maxBytes] have been read, into an array of [initialCapacity] that grows. */
    private fun readAtMost(
        input: InputStream,
        maxBytes: Long,
        initialCapacity: Int,
    ): ByteArray {
        var buffer = ByteArray(initialCapacity)
        var size = 0
        while (size < maxBytes) {
            if (size == buffer.size) {
                // Full. One probe byte decides between "done" - the normal end of a body presized from its
                // Content-Length, which then needs no copy at all - and "grow". Single-byte reads are cheap here:
                // the transport's stream is buffered underneath.
                val next = input.read()
                if (next < 0) break
                buffer = buffer.copyOf(grownCapacity(buffer.size, maxBytes))
                buffer[size++] = next.toByte()
                continue
            }
            val wanted = minOf((buffer.size - size).toLong(), maxBytes - size).toInt()
            val read = input.read(buffer, size, wanted)
            if (read < 0) break
            size += read
        }
        return if (size == buffer.size) buffer else buffer.copyOf(size)
    }

    /** Doubles [current] (at least one chunk), never past [maxBytes] or the largest array the JVM allocates. */
    private fun grownCapacity(
        current: Int,
        maxBytes: Long,
    ): Int {
        val grown = minOf(maxOf(current * 2L, CHUNK_SIZE.toLong()), maxBytes, MAX_ARRAY_SIZE.toLong()).toInt()
        // Only reachable with a limit at the very top of the admitted range and a body of two gigabytes - the
        // same OutOfMemoryError a ByteArrayOutputStream raises there.
        if (grown <= current) throw OutOfMemoryError("Response body does not fit into a byte array of $current bytes")
        return grown
    }
}
