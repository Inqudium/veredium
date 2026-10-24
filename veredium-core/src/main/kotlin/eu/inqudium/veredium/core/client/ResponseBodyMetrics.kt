package eu.inqudium.veredium.core.client

/**
 * The response-size summary both client stacks record per call - the one input for sizing their
 * `max-in-memory-size` ceiling - in everything but its name: `restclient.response.body.size` and
 * `webclient.response.body.size` are each stack's, the unit, the tag and the measurement are shared.
 */
object ResponseBodyMetrics {
    /**
     * BYTES - the same unit as the stack's `max-in-memory-size`, which is the whole point of the meter.
     *
     * Sampled with [utf8Size], not `String.length`. The two agree on ASCII and diverge in exactly one
     * direction: a `length` of N encodes to between N and 3N UTF-8 bytes, so counting UTF-16 units would
     * UNDER-state the buffered size - the direction that quietly sizes a ceiling too small.
     */
    const val BASE_UNIT: String = "bytes"

    /**
     * Identifies which downstream this sample came from. Named after the `adapter_*` family of the log fields,
     * so a meter and a log line can be correlated on sight: the value is exactly what the exchange event and
     * the clients' transport lines carry as `adapter_name`.
     *
     * **The value is the mesh ENDPOINT alias, not an adapter class.** An adapter that owns two endpoints owns two
     * values, so a filter on this tag selects one CALL, not one adapter.
     */
    const val ADAPTER_TAG: String = "adapter"

    /**
     * The UTF-8 encoded size of [text], counted in one pass and without allocating.
     *
     * Equivalent to `text.toByteArray(Charsets.UTF_8).size` for every input, including unpaired surrogates - pinned
     * against the JDK encoder rather than against hand-written expectations. The point of not simply calling
     * `toByteArray` is the copy: on the hot path that is a second full buffer per response, up to
     * `max-in-memory-size` each, to produce a single number.
     *
     * The surrogate handling is the part worth reading. A valid high/low pair is ONE code point in four bytes
     * and consumes two UTF-16 units; an UNPAIRED surrogate is not encodable. Assumption: the JDK encoder
     * substitutes `?` - one byte - rather than failing. Both cases are counted the way the encoder counts them, so the
     * meter can never disagree with the reader or the codec about the same body.
     */
    fun utf8Size(text: String): Int {
        var bytes = 0
        var i = 0
        while (i < text.length) {
            val c = text[i]
            when {
                c.code < 0x80 -> {
                    bytes += 1
                }

                c.code < 0x800 -> {
                    bytes += 2
                }

                c.isHighSurrogate() && i + 1 < text.length && text[i + 1].isLowSurrogate() -> {
                    bytes += 4
                    i++
                }

                c.isSurrogate() -> {
                    bytes += 1
                }

                else -> {
                    bytes += 3
                }
            }
            i++
        }
        return bytes
    }
}
