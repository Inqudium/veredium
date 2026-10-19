package eu.inqudium.veredium.restclient

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.InputStream

/**
 * Tests for [BoundedBodyReader], the one piece of this module that stands in for the reactive codec's buffer
 * limit. Everything about the limit's meaning downstream - which exception, which breaker verdict, which log
 * line - rests on this reader drawing the line at exactly the configured byte, so the boundary is pinned on both
 * sides here.
 */
class BoundedBodyReaderTest {
    @Nested
    inner class SuccessBodies {
        @Test
        fun `should return a body that fits, including one that is exactly at the limit`() {
            // What is tested: the inclusive end of the limit - a body of exactly `limit` bytes FITS.
            // Why it matters: an off-by-one here rejects the largest legitimate response; the reader has to ask
            //   for one byte past the limit to tell "exactly at" from "past", and this is the case that proves
            //   it does not count that probe byte against the body.
            //
            // Given
            val body = ByteArray(1000) { 'x'.code.toByte() }

            // When / Then
            assertThat(BoundedBodyReader.readOrNull(ByteArrayInputStream(body), 1000)).isEqualTo(body)
            assertThat(BoundedBodyReader.readOrNull(ByteArrayInputStream(body), 5000)).isEqualTo(body)
        }

        @Test
        fun `should report a body one byte past the limit as not fitting`() {
            // Given
            val body = ByteArray(1001) { 'x'.code.toByte() }

            // When / Then: null, not a truncated array - a success body arrives whole or not at all
            assertThat(BoundedBodyReader.readOrNull(ByteArrayInputStream(body), 1000)).isNull()
        }

        @Test
        fun `should stop reading once the limit is passed instead of draining the stream`() {
            // What is tested: that an oversized body is NOT read to the end - the limit is about memory, and a
            //   reader that drained a multi-megabyte body into a buffer before saying no would defeat it.
            // How success is judged: a stream that reports how many bytes were taken shows at most limit + 1
            //   (the probe byte), against a body far larger than that.
            //
            // Given: a large body behind a stream that counts what is read from it
            val counting = CountingStream(ByteArray(1_000_000))

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
            // How success is judged: a small body is returned whole under a limit of Int.MAX_VALUE.
            // Why it matters: the reader asks for one byte past the limit, and `Int.MAX_VALUE + 1` wraps to
            //   Int.MIN_VALUE in Int arithmetic - the first read would then be asked for a negative length and
            //   throw an IndexOutOfBoundsException, failing EVERY 2xx response of a deployment whose only
            //   mistake was to configure the maximum the guard explicitly allows.
            //
            // Given
            val body = ByteArray(1000) { 'x'.code.toByte() }

            // When / Then
            assertThat(BoundedBodyReader.readOrNull(ByteArrayInputStream(body), Int.MAX_VALUE)).isEqualTo(body)
        }

        @Test
        fun `should return an empty array for an empty body`() {
            // Given / When / Then: an empty 2xx body is legal and must not be mistaken for an overrun
            assertThat(BoundedBodyReader.readOrNull(ByteArrayInputStream(ByteArray(0)), 10)).isEmpty()
        }
    }

    @Nested
    inner class ErrorBodies {
        @Test
        fun `should cut an error body at the limit rather than reject it`() {
            // What is tested: the other reading of the same limit - a diagnostic body is truncated, never
            //   refused, so a 5xx keeps being a 5xx whatever size its error page has.
            // Given
            val body = ByteArray(5000) { (it % 7).toByte() }

            // When
            val truncated = BoundedBodyReader.readTruncated(ByteArrayInputStream(body), 1000)

            // Then: exactly the first `limit` bytes
            assertThat(truncated).isEqualTo(body.copyOf(1000))
        }

        @Test
        fun `should return a short error body whole`() {
            // Given / When / Then
            val body = "{\"error\":\"boom\"}".toByteArray()
            assertThat(BoundedBodyReader.readTruncated(ByteArrayInputStream(body), 1000)).isEqualTo(body)
        }
    }

    /** A stream that counts the bytes handed out, so a test can see how far a reader went. */
    private class CountingStream(
        content: ByteArray,
    ) : InputStream() {
        private val delegate = ByteArrayInputStream(content)
        var bytesRead: Int = 0
            private set

        override fun read(): Int = delegate.read().also { if (it >= 0) bytesRead++ }

        override fun read(
            b: ByteArray,
            off: Int,
            len: Int,
        ): Int = delegate.read(b, off, len).also { if (it > 0) bytesRead += it }
    }
}
