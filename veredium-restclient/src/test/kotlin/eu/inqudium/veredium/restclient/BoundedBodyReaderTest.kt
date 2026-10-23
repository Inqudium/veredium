package eu.inqudium.veredium.restclient

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.ByteArrayInputStream
import java.io.InputStream

/**
 * Tests for [BoundedBodyReader], the one piece of this module that stands in for the reactive codec's buffer
 * limit. Everything about the limit's meaning downstream - which exception, which breaker verdict, which log
 * line - rests on this reader drawing the line at exactly the configured byte, so the boundary is pinned on both
 * sides here.
 *
 * A real response stream rarely hands over a body in one read: the transport delivers what the socket had, a
 * few bytes or a few kilobytes at a time. So beyond the single-read cases, every boundary is pinned again
 * through a [TricklingStream] - a fixed number of bytes per read, or a scripted sequence of read sizes
 * including the zero-length reads an `InputStream` may legally return - and with bodies larger than the
 * reader's 8 KiB growth step, so the accumulation across reads and across buffer growth is what is tested, not
 * one lucky `read`. The Jazzer target `BoundedBodyReaderFuzzTest` explores the same invariants with arbitrary
 * read patterns. The bytes are a non-repeating pattern ([pattern]), so a reader that kept the wrong chunk, or
 * the last chunk only, cannot produce an equal array by accident.
 */
class BoundedBodyReaderTest {
    @Nested
    inner class `The success bodies` {
        @Test
        fun `should return a body that fits, including one that is exactly at the limit`() {
            // What is tested: the inclusive end of the limit - a body of exactly `limit` bytes FITS.
            // Success criteria: the body is returned byte for byte under a limit equal to its size and under a
            //   larger one.
            // Why it matters: an off-by-one here rejects the largest legitimate response; the reader has to ask
            //   for one byte past the limit to tell "exactly at" from "past", and this is the case that proves
            //   it does not count that probe byte against the body.
            //
            // Given
            val body = pattern(1000)

            // When / Then
            assertThat(BoundedBodyReader.readOrNull(ByteArrayInputStream(body), 1000)).isEqualTo(body)
            assertThat(BoundedBodyReader.readOrNull(ByteArrayInputStream(body), 5000)).isEqualTo(body)
        }

        @Test
        fun `should report a body one byte past the limit as not fitting`() {
            // What is tested: the exclusive side of the same boundary - `limit + 1` bytes do not fit.
            // Success criteria: the result is null, not an array cut at the limit.
            // Why it matters: a success body arrives whole or not at all; a truncated array would be parsed as
            //   if it were the answer, and fail as malformed JSON instead of as the limit it is.
            //
            // Given
            val body = pattern(1001)

            // When
            val result = BoundedBodyReader.readOrNull(ByteArrayInputStream(body), 1000)

            // Then
            assertThat(result).isNull()
        }

        @Test
        fun `should stop reading once the limit is passed instead of draining the stream`() {
            // What is tested: that an oversized body is NOT read to the end.
            // Success criteria: a stream that reports how many bytes were taken shows at most limit + 1 (the
            //   probe byte), against a body far larger than that.
            // Why it matters: the limit is about memory, and a reader that drained a multi-megabyte body into a
            //   buffer before saying no would defeat it.
            //
            // Given: a large body behind a stream that counts what is read from it
            val counting = TricklingStream(ByteArray(1_000_000), readSizes = intArrayOf(Int.MAX_VALUE))

            // When
            val result = BoundedBodyReader.readOrNull(counting, 4096)

            // Then
            assertThat(result).isNull()
            assertThat(counting.bytesRead).isLessThanOrEqualTo(4097)
        }

        @Test
        fun `should read a body under the largest limit the properties admit`() {
            // What is tested: the top of the limit's range. RestClientProperties admits `max-in-memory-size`
            //   up to Int.MAX_VALUE bytes and pins that in its own test, so the reader has to work there.
            // Success criteria: a small body is returned whole under a limit of Int.MAX_VALUE.
            // Why it matters: the reader asks for one byte past the limit, and `Int.MAX_VALUE + 1` wraps to
            //   Int.MIN_VALUE in Int arithmetic - the first read would then be asked for a negative length and
            //   throw an IndexOutOfBoundsException, failing EVERY 2xx response of a deployment whose only
            //   mistake was to configure the maximum the guard explicitly allows.
            //
            // Given
            val body = pattern(1000)

            // When / Then
            assertThat(BoundedBodyReader.readOrNull(ByteArrayInputStream(body), Int.MAX_VALUE)).isEqualTo(body)
        }

        @Test
        fun `should return an empty array for an empty body`() {
            // What is tested: a body of zero bytes.
            // Success criteria: an empty array - not null.
            // Why it matters: null means "past the limit"; mistaking an empty 2xx (a 204) for an overrun would
            //   report a buffer-limit failure for a response that carried nothing. The empty body is the
            //   client's to reject, with its own exception, after the reader.
            //
            // Given / When / Then
            assertThat(BoundedBodyReader.readOrNull(ByteArrayInputStream(ByteArray(0)), 10)).isEmpty()
        }

        @ParameterizedTest(name = "{0} byte(s) per read")
        @ValueSource(ints = [1, 7, 8191, 8192, 8193, 100_000])
        fun `should accumulate a body larger than one growth step across many short reads`(bytesPerRead: Int) {
            // What is tested: a body of 20 000 bytes - more than two of the reader's 8 KiB growth steps -
            //   delivered a few bytes at a time (1, 7, just under/at/over the step size, all at once), under a
            //   limit it fits with room and a limit it fits exactly.
            // Success criteria: the result equals the body byte for byte in every case.
            // Why it matters: with single-read inputs a reader that kept only the last read, or overwrote the
            //   buffer when it grew, passes; only a body that arrives in pieces and outgrows the first buffer
            //   proves the accumulation.
            //
            // Given
            val body = pattern(20_000)

            // When
            val roomy = BoundedBodyReader.readOrNull(TricklingStream(body, intArrayOf(bytesPerRead)), 50_000)
            val exact = BoundedBodyReader.readOrNull(TricklingStream(body, intArrayOf(bytesPerRead)), 20_000)

            // Then
            assertThat(roomy).isEqualTo(body)
            assertThat(exact).isEqualTo(body)
        }

        @ParameterizedTest(name = "{0} byte(s) per read")
        @ValueSource(ints = [1, 7, 8191, 8192, 8193, 100_000])
        fun `should reject a body one byte past a limit that spans several growth steps`(bytesPerRead: Int) {
            // What is tested: the exclusive side of the boundary when it lies beyond the first buffer - a limit
            //   of two growth steps plus three bytes, a body one byte longer, delivered in pieces.
            // Success criteria: null, and no more than limit + 1 bytes were taken from the stream.
            // Why it matters: the probe byte that tells "at" from "past" is read at a buffer boundary here, the
            //   one place where the reader switches from filling to growing; an off-by-one there shows only for
            //   a body of exactly this shape.
            //
            // Given
            val limit = 2 * 8192 + 3
            val stream = TricklingStream(pattern(limit + 1), intArrayOf(bytesPerRead))

            // When
            val result = BoundedBodyReader.readOrNull(stream, limit)

            // Then
            assertThat(result).isNull()
            assertThat(stream.bytesRead).isLessThanOrEqualTo(limit + 1)
        }

        @Test
        fun `should keep reading through zero-byte and one-byte reads`() {
            // What is tested: a stream whose reads return 0, 1, 0, 3 bytes in turn - zero-length reads are legal
            //   for an InputStream - for a body at the limit and one past it.
            // Success criteria: the body at the limit is returned whole, the body past it is null.
            // Why it matters: a reader that took a 0 for the end of the stream would return a truncated body as
            //   if it were complete; one that took it for "no progress, give up" would hang or fail.
            //
            // Given
            val limit = 9000
            val sizes = intArrayOf(0, 1, 0, 3)

            // When
            val atLimit = BoundedBodyReader.readOrNull(TricklingStream(pattern(limit), sizes), limit)
            val pastLimit = BoundedBodyReader.readOrNull(TricklingStream(pattern(limit + 1), sizes), limit)

            // Then
            assertThat(atLimit).isEqualTo(pattern(limit))
            assertThat(pastLimit).isNull()
        }

        @Test
        fun `should read the same bytes whatever Content-Length the response declared`() {
            // What is tested: the buffer presizing from a declared Content-Length - correct, too small, too large
            //   and far past the limit, each against the same 20 000-byte body - on both sides of the limit.
            // Success criteria: the body is returned identically for every declared length under a limit it
            //   fits, and rejected identically under a limit one byte short of it.
            // Why it matters: the declared length is a hint from the peer; it sizes the first allocation and
            //   must never decide what is returned. A reader that trusted it would cut a body that lied low and
            //   wait for bytes that never come on one that lied high.
            //
            // Given
            val body = pattern(20_000)
            val declared = listOf(20_000L, 10L, 30_000L, Long.MAX_VALUE, 0L)

            // When
            val fitting = declared.map { BoundedBodyReader.readOrNull(TricklingStream(body, intArrayOf(4096)), 20_000, it) }
            val rejected = declared.map { BoundedBodyReader.readOrNull(TricklingStream(body, intArrayOf(4096)), 19_999, it) }

            // Then
            assertThat(fitting).allSatisfy { assertThat(it).isEqualTo(body) }
            assertThat(rejected).containsOnlyNulls()
        }
    }

    @Nested
    inner class `The error bodies` {
        @Test
        fun `should cut an error body at the limit rather than reject it`() {
            // What is tested: the other reading of the same limit - a diagnostic body is truncated, never
            //   refused.
            // Success criteria: exactly the first `limit` bytes of a body five times the limit.
            // Why it matters: a 5xx must stay a 5xx for the breaker whatever size its error page has; a refusal
            //   here would turn an outage into a client-side failure the breaker ignores.
            //
            // Given
            val body = pattern(5000)

            // When
            val truncated = BoundedBodyReader.readTruncated(ByteArrayInputStream(body), 1000)

            // Then
            assertThat(truncated).isEqualTo(body.copyOf(1000))
        }

        @Test
        fun `should read an error body of exactly the declared length at the limit to its end`() {
            // What is tested: an error body that declared exactly the limit and delivered it.
            // Success criteria: returned byte for byte, and the stream was read to its end (a read returned -1).
            // Why it matters: the engines' response close discards the connection of a body not read to its end
            //   (AbortingResponse). A complete body cut exactly at the limit, without that last read, would cost
            //   a connection that has nothing left on it.
            //
            // Given
            val body = pattern(1000)
            val stream = EndRecordingStream(body)

            // When
            val truncated = BoundedBodyReader.readTruncated(stream, 1000, contentLength = 1000)

            // Then
            assertThat(truncated).isEqualTo(body)
            assertThat(stream.sawEnd).`as`("the stream must have been read to its end").isTrue()
        }

        @ParameterizedTest(name = "Content-Length {0}")
        @ValueSource(longs = [-1, 5000])
        fun `should not read past the limit of an error body that declared no length or a longer one`(contentLength: Long) {
            // What is tested: an error body at the limit whose declared length does not say it is complete - none
            //   (chunked) or a larger one.
            // Success criteria: exactly the first `limit` bytes are returned, and no byte beyond them was read.
            // Why it matters: only a body whose declared end is at the limit can be probed for its end without
            //   waiting; for any other, one more read could wait for a peer that keeps sending.
            //
            // Given
            val body = pattern(5000)
            val stream = TricklingStream(body, intArrayOf(100_000))

            // When
            val truncated = BoundedBodyReader.readTruncated(stream, 1000, contentLength)

            // Then
            assertThat(truncated).isEqualTo(body.copyOf(1000))
            assertThat(stream.bytesRead).isEqualTo(1000)
        }

        @Test
        fun `should return a short error body whole`() {
            // What is tested: an error body below the limit.
            // Success criteria: returned byte for byte.
            // Why it matters: the error body is what the exception and the error line show an operator; cutting
            //   one that fits would lose the very detail the read exists to keep.
            //
            // Given
            val body = "{\"error\":\"boom\"}".toByteArray()

            // When / Then
            assertThat(BoundedBodyReader.readTruncated(ByteArrayInputStream(body), 1000)).isEqualTo(body)
        }

        @ParameterizedTest(name = "{0} byte(s) per read")
        @ValueSource(ints = [1, 7, 8191, 8192, 8193, 100_000])
        fun `should cut a trickling error body at a limit that spans several growth steps`(bytesPerRead: Int) {
            // What is tested: truncation across reads and buffer growth - a 30 000-byte error body delivered in
            //   pieces, cut at a limit of two growth steps plus three bytes; and the same body under a limit it
            //   fits.
            // Success criteria: exactly the first `limit` bytes, with no byte past the limit taken from the
            //   stream; the whole body under the roomy limit.
            // Why it matters: a truncation that miscounted across reads would carry a garbled prefix on the
            //   exception, or read past the limit - which for an error body is the only memory bound there is.
            //
            // Given
            val limit = 2 * 8192 + 3
            val body = pattern(30_000)
            val stream = TricklingStream(body, intArrayOf(bytesPerRead))

            // When
            val truncated = BoundedBodyReader.readTruncated(stream, limit)
            val whole = BoundedBodyReader.readTruncated(TricklingStream(body, intArrayOf(bytesPerRead)), 40_000)

            // Then
            assertThat(truncated).isEqualTo(body.copyOf(limit))
            assertThat(stream.bytesRead).isEqualTo(limit)
            assertThat(whole).isEqualTo(body)
        }

        @Test
        fun `should cut through zero-byte and one-byte reads and ignore the declared length`() {
            // What is tested: an error body read through 0-, 1- and 3-byte reads, with a declared Content-Length
            //   that is wrong in either direction.
            // Success criteria: the prefix of length `limit` every time, and an empty array for an empty body.
            // Why it matters: an error body's length header is as untrustworthy as a success body's, and a
            //   zero-length read is not the end of the stream.
            //
            // Given
            val limit = 9000
            val body = pattern(12_000)
            val sizes = intArrayOf(0, 1, 0, 3)

            // When
            val results = listOf(-1L, 5L, 12_000L, Long.MAX_VALUE).map { BoundedBodyReader.readTruncated(TricklingStream(body, sizes), limit, it) }
            val empty = BoundedBodyReader.readTruncated(TricklingStream(ByteArray(0), sizes), limit, 0)

            // Then
            assertThat(results).allSatisfy { assertThat(it).isEqualTo(body.copyOf(limit)) }
            assertThat(empty).isEmpty()
        }
    }

    /** A non-repeating byte pattern: position `i` holds `i % 251`, a prime, so no chunk size aligns with it. */
    private fun pattern(size: Int): ByteArray = ByteArray(size) { (it % 251).toByte() }

    /** A stream over [content] that records whether a read reported its end. */
    private class EndRecordingStream(
        content: ByteArray,
    ) : InputStream() {
        private val delegate = ByteArrayInputStream(content)
        var sawEnd: Boolean = false
            private set

        override fun read(): Int = delegate.read().also { if (it < 0) sawEnd = true }

        override fun read(
            b: ByteArray,
            off: Int,
            len: Int,
        ): Int = delegate.read(b, off, len).also { if (it < 0) sawEnd = true }
    }

    /**
     * A stream that hands out [content] in reads of the sizes [readSizes] names, cycling through them - never
     * more than the caller asked for, and a size of 0 is a legal zero-length read. Counts the bytes handed out,
     * so a test can see how far a reader went.
     */
    private class TricklingStream(
        content: ByteArray,
        private val readSizes: IntArray,
    ) : InputStream() {
        private val delegate = ByteArrayInputStream(content)
        private var nextSize = 0
        var bytesRead: Int = 0
            private set

        override fun read(): Int = delegate.read().also { if (it >= 0) bytesRead++ }

        override fun read(
            b: ByteArray,
            off: Int,
            len: Int,
        ): Int {
            if (len == 0) return 0
            if (delegate.available() == 0) return -1
            val size = readSizes[nextSize++ % readSizes.size]
            return delegate.read(b, off, minOf(len, size)).also { if (it > 0) bytesRead += it }
        }
    }
}
