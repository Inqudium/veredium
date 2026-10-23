package eu.inqudium.veredium.testsupport

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport

/** Whether [condition] holds within [within], polled every 10 ms - a bounded wait for a state another thread reaches, never a sleep. */
fun eventually(
    within: Duration = Duration.ofSeconds(5),
    condition: () -> Boolean,
): Boolean {
    val deadline = System.nanoTime() + within.toNanos()
    while (!condition()) {
        if (System.nanoTime() - deadline >= 0) return false
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10))
    }
    return true
}

/** A clock that advances [stepNanos] on every read - past any deadline at its next read. */
fun leapingClock(stepNanos: Long): () -> Long {
    val now = AtomicLong()
    return { now.addAndGet(stepNanos) }
}
