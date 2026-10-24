package eu.inqudium.veredium.restclient

import java.util.concurrent.atomic.AtomicLong

/** A clock that advances [stepNanos] on every read - past any deadline at its next read. */
internal fun leapingClock(stepNanos: Long): () -> Long {
    val now = AtomicLong()
    return { now.addAndGet(stepNanos) }
}
