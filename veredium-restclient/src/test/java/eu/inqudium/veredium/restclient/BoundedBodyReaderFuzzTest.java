package eu.inqudium.veredium.restclient;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Fuzzes {@code BoundedBodyReader}, the hard byte limit every response body of the blocking client passes
 * through - peer-controlled input, read in whatever pieces the transport hands over.
 *
 * Invariants under test, for an arbitrary body, limit, declared {@code Content-Length} and read pattern:
 * <ul>
 *   <li>{@code readOrNull} returns {@code null} exactly when the body is longer than the limit, and otherwise
 *       the body itself, byte for byte;</li>
 *   <li>{@code readTruncated} returns the prefix of length {@code min(length, limit)};</li>
 *   <li>neither takes more from the stream than it may: {@code limit + 1} bytes (the probe that tells "at the
 *       limit" from "past it") for {@code readOrNull}, {@code limit} for {@code readTruncated} - or
 *       {@code limit + 1} when the declared length IS the limit, where one probe read sees whether the body ended
 *       there, so the engines' close does not discard a connection that has nothing left on it;</li>
 *   <li>the declared length never changes a result - it only sizes the first buffer.</li>
 * </ul>
 * The stream returns short reads of fuzzer-chosen sizes, zero-length reads included (never two in a row, so the
 * stream still makes progress as a blocking stream must), and the body is the fuzzer's bytes repeated a
 * fuzzer-chosen number of times, so bodies well past the reader's 8 KiB growth step are reached from small
 * inputs.
 *
 * Runs as a regression test (checked-in inputs plus the empty input) in every build; the scheduled Fuzz
 * workflow explores for real (JAZZER_FUZZ=1).
 */
class BoundedBodyReaderFuzzTest {
    @FuzzTest(maxDuration = "10m")
    void readsExactlyTheBytesTheLimitAdmits(FuzzedDataProvider data) {
        // What is tested: BoundedBodyReader.readOrNull and readTruncated for arbitrary bodies, limits, declared
        //   lengths and read patterns.
        // Success criteria: for both reads of the same input, no read throws, and (1) readOrNull returns null
        //   exactly when the body is longer than the limit and otherwise the body byte for byte; (2)
        //   readTruncated returns the prefix of length min(length, limit); (3) readOrNull takes at most
        //   limit + 1 bytes from the stream and readTruncated at most limit (limit + 1 when the declared length
        //   equals the limit: the one probe read that sees a body complete at the limit end); (4) the declared
        //   length never changes a result - the expected values of (1) and (2) ignore it.
        // Why it matters: the limit is the only memory bound on a response the peer controls, and its meaning
        //   downstream - which exception, which breaker verdict - rests on the exact byte it is drawn at. A
        //   reader that loses or duplicates a piece across short reads, or miscounts at a buffer boundary,
        //   passes every single-read test; BoundedBodyReaderTest pins the cases someone thought of, this
        //   explores the rest.
        //
        // Given
        int limit = data.consumeInt(0, 64 * 1024);
        long declaredLength = data.consumeBoolean() ? -1L : data.consumeLong(0L, Long.MAX_VALUE);
        int[] readSizes = new int[data.consumeInt(1, 8)];
        for (int i = 0; i < readSizes.length; i++) {
            readSizes[i] = data.consumeInt(0, 20_000);
        }
        int repetitions = data.consumeInt(1, 64);
        byte[] body = repeat(data.consumeRemainingAsBytes(), repetitions);

        // When
        ShortReadStream whole = new ShortReadStream(body, readSizes);
        byte[] fitting = BoundedBodyReader.INSTANCE.readOrNull(whole, limit, declaredLength);
        ShortReadStream prefix = new ShortReadStream(body, readSizes);
        byte[] truncated = BoundedBodyReader.INSTANCE.readTruncated(prefix, limit, declaredLength);

        // Then
        if (body.length > limit) {
            require(
                    fitting == null,
                    "readOrNull returned a body of " + length(fitting) + " bytes past the limit",
                    body,
                    limit);
        } else {
            require(
                    Arrays.equals(fitting, body),
                    "readOrNull did not return the body unchanged",
                    body,
                    limit);
        }
        require(
                whole.taken <= limit + 1L,
                "readOrNull took " + whole.taken + " bytes from the stream",
                body,
                limit);
        require(
                Arrays.equals(truncated, Arrays.copyOf(body, Math.min(body.length, limit))),
                "readTruncated did not return the prefix of length min(length, limit)",
                body,
                limit);
        // The probe read past a body that declared exactly the limit: it finds the end of a complete body, and
        // takes one byte of a body longer than it declared (a lying peer) - never more.
        long truncatedMayTake = declaredLength == limit ? limit + 1L : limit;
        require(
                prefix.taken <= truncatedMayTake,
                "readTruncated took " + prefix.taken + " bytes from the stream",
                body,
                limit);
    }

    private static byte[] repeat(byte[] unit, int times) {
        byte[] result = new byte[unit.length * times];
        for (int i = 0; i < times; i++) {
            System.arraycopy(unit, 0, result, i * unit.length, unit.length);
        }
        return result;
    }

    private static String length(byte[] bytes) {
        return bytes == null ? "null" : String.valueOf(bytes.length);
    }

    private static void require(boolean holds, String violation, byte[] body, int limit) {
        if (!holds) {
            throw new IllegalStateException(
                    violation + " (body length " + body.length + ", limit " + limit + ")");
        }
    }

    /**
     * Hands out its content in reads of the given sizes, cycling - never more than asked for. A size of 0 is a
     * zero-length read, but never twice in a row: a blocking stream must eventually make progress, and a reader
     * that waits for it is right to.
     */
    private static final class ShortReadStream extends InputStream {
        private final ByteArrayInputStream delegate;
        private final int[] readSizes;
        private int next;
        private boolean lastWasEmpty;
        long taken;

        ShortReadStream(byte[] content, int[] readSizes) {
            this.delegate = new ByteArrayInputStream(content);
            this.readSizes = readSizes;
        }

        @Override
        public int read() {
            int value = delegate.read();
            if (value >= 0) {
                taken++;
            }
            return value;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (len == 0) {
                return 0;
            }
            if (delegate.available() == 0) {
                return -1;
            }
            int size = readSizes[next++ % readSizes.length];
            if (size == 0 && !lastWasEmpty) {
                lastWasEmpty = true;
                return 0;
            }
            lastWasEmpty = false;
            int read = delegate.read(b, off, Math.min(len, Math.max(size, 1)));
            taken += Math.max(read, 0);
            return read;
        }
    }
}
