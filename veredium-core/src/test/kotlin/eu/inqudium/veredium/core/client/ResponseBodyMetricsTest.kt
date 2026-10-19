package eu.inqudium.veredium.core.client

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * Tests for [ResponseBodyMetrics]: the one computation both clients make on the hot path, the UTF-8 size of a
 * body, which is what their `max-in-memory-size` gets sized from.
 */
class ResponseBodyMetricsTest {
    @Nested
    inner class Utf8SizeMatchesTheJdkEncoder {
        @ParameterizedTest
        @ValueSource(
            strings = [
                "",
                "plain ascii json {\"a\":1}",
                "Umlaute: aeoeue vs äöüß",
                "cyrillic да, greek αβ",
                "cjk 漢字 mixed with ascii",
                "emoji 🙂 as a surrogate pair",
                "🙂🙂🙂",
                "boundary \u007f \u0080 ߿ ࠀ ￿",
            ],
        )
        fun `should count exactly what the JDK encoder produces`(text: String) {
            // What is tested: that the allocation-free counter and String.toByteArray(UTF_8) never disagree.
            // How success is judged: equality against the JDK encoder itself, not against numbers written out
            //   by hand - a hand-written expectation would only pin my arithmetic, which is the thing under
            //   test.
            // Why this case is important: this number is what the stack's `max-in-memory-size` gets sized
            //   from, and the bounded reader respectively the codec measures the same body in the same unit. A counter that drifts
            //   from the encoder would make the meter disagree with the limit it exists to inform - the values
            //   span every UTF-8 width (1, 2, 3 bytes) and both surrogate halves for that reason.
            //
            // Given / When / Then
            assertThat(ResponseBodyMetrics.utf8Size(text)).isEqualTo(text.toByteArray(Charsets.UTF_8).size)
        }

        @Test
        fun `should count an unpaired surrogate the way the encoder substitutes it`() {
            // What is tested: the one input where a naive width table is wrong - a lone surrogate, which is
            //   not encodable at all.
            // How success is judged: again equality with the JDK encoder, which substitutes a single '?'
            //   rather than failing. A table that classified it by code-point range would say three bytes.
            // Why this case is important: it cannot arise from a body decoded out of valid UTF-8, so it will
            //   never be caught in production - which is exactly why it belongs in a test rather than in a
            //   comment claiming it cannot happen.
            //
            // Given: a high surrogate with nothing after it, and a lone low surrogate
            val loneHigh = "before \ud83d"
            val loneLow = "\ude42 after"

            // When / Then
            assertThat(ResponseBodyMetrics.utf8Size(loneHigh)).isEqualTo(loneHigh.toByteArray(Charsets.UTF_8).size)
            assertThat(ResponseBodyMetrics.utf8Size(loneLow)).isEqualTo(loneLow.toByteArray(Charsets.UTF_8).size)
        }

        @Test
        fun `should exceed the UTF-16 length for multi-byte content, never fall below it`() {
            // Why this test exists: it states the DIRECTION that motivated the switch away from String.length.
            // Under-stating the buffered size is what silently sizes the limit too small; this pins that the
            // counter errs the other way, and by how much at the extreme (three bytes per BMP character).
            //
            // Given / When / Then
            assertThat(ResponseBodyMetrics.utf8Size("ascii")).isEqualTo("ascii".length)
            assertThat(ResponseBodyMetrics.utf8Size("äöü")).isEqualTo(2 * "äöü".length)
            assertThat(ResponseBodyMetrics.utf8Size("漢字")).isEqualTo(3 * "漢字".length)
        }
    }
}
