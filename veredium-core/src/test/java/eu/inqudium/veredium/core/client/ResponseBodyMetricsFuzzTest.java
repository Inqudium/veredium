package eu.inqudium.veredium.core.client;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.nio.charset.StandardCharsets;

/**
 * Fuzzes {@code ResponseBodyMetrics.utf8Size}, the allocation-free UTF-8 counter both client stacks sample every
 * response body with - the number their {@code max-in-memory-size} ceiling is sized from.
 *
 * Invariant under test: for EVERY string, {@code utf8Size(s) == s.getBytes(UTF_8).length} - the JDK encoder is
 * the oracle, including its substitution of one {@code ?} byte for an unpaired surrogate. The input is split in
 * two and checked as the halves AND as their concatenation, so a surrogate pair that only forms across the cut
 * is exercised too: counted separately each half is an unpaired surrogate (1 byte each), together they are one
 * four-byte code point.
 *
 * Runs as a regression test (checked-in inputs plus the empty input) in every build; the scheduled Fuzz
 * workflow explores for real (JAZZER_FUZZ=1).
 */
class ResponseBodyMetricsFuzzTest {
    @FuzzTest(maxDuration = "10m")
    void utf8SizeAgreesWithTheJdkEncoder(FuzzedDataProvider data) {
        // What is tested: ResponseBodyMetrics.utf8Size for arbitrary strings - peer-controlled response bodies
        //   in production - including arbitrary UTF-16 (lone and reversed surrogates, every encoding width).
        // Success criteria: for the first part, the rest and their concatenation alike, the count equals the
        //   length of String.getBytes(UTF_8); utf8Size never throws.
        // Why it matters: the bounded reader and the codec measure the same body in bytes; a counter that drifts
        //   from the encoder - an off-by-one at a width boundary, a surrogate miscount - makes the meter
        //   disagree with the limit it exists to inform, and the hand-picked cases of ResponseBodyMetricsTest
        //   cover only the inputs someone thought of.
        //
        // Given
        String head = data.consumeString(64);
        String tail = data.consumeRemainingAsString();

        // When / Then
        requireAgreement(head);
        requireAgreement(tail);
        requireAgreement(head + tail);
    }

    private static void requireAgreement(String text) {
        int expected = text.getBytes(StandardCharsets.UTF_8).length;
        int actual = ResponseBodyMetrics.INSTANCE.utf8Size(text);
        if (actual != expected) {
            throw new IllegalStateException(
                    "utf8Size=" + actual + " but the JDK encoder produced " + expected + " bytes for "
                            + describe(text));
        }
    }

    /** The UTF-16 units, so a failing input with unprintable or lone surrogates is readable in the report. */
    private static String describe(String text) {
        StringBuilder units = new StringBuilder("[");
        for (int i = 0; i < text.length(); i++) {
            if (i > 0) {
                units.append(' ');
            }
            units.append(String.format("%04x", (int) text.charAt(i)));
        }
        return units.append(']').toString();
    }
}
