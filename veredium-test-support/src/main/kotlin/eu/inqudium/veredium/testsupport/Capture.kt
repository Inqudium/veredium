package eu.inqudium.veredium.testsupport

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.github.tomakehurst.wiremock.http.Request
import com.github.tomakehurst.wiremock.http.RequestListener
import com.github.tomakehurst.wiremock.http.Response
import org.assertj.core.api.Assertions.assertThat
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport

/**
 * A Logback appender that captures the events [accept] lets through, from any thread - Logback's `ListAppender` is not
 * safe for that - and waits, bounded, for the ones a test expects.
 *
 * Each event is snapshotted when it is appended (`prepareForDeferredProcessing`): Logback captures the MDC property
 * map LAZILY, on first read, and a scoped MDC (legatium's emission scope) holds its keys only for the duration of the
 * log statement - read later on the test thread, the entries would be gone. A production async appender does the same
 * before it enqueues.
 */
class CapturingAppender(
    private val accept: (ILoggingEvent) -> Boolean = { true },
) : AppenderBase<ILoggingEvent>() {
    private val captured = CopyOnWriteArrayList<ILoggingEvent>()

    override fun append(eventObject: ILoggingEvent) {
        if (!accept(eventObject)) return
        eventObject.prepareForDeferredProcessing()
        captured += eventObject
    }

    /**
     * What has been captured so far - for asserting that something was NOT logged, where there is nothing to wait
     * for. The caller is responsible for having driven the call to completion first.
     */
    val events: List<ILoggingEvent> get() = captured.toList()

    /**
     * Everything captured, once at least [count] captured events match [until] - failing the test when they do not
     * within [within].
     */
    fun awaitEvents(
        count: Int = 1,
        within: Duration = Duration.ofSeconds(5),
        until: (ILoggingEvent) -> Boolean = { true },
    ): List<ILoggingEvent> {
        assertThat(eventually(within) { captured.count(until) >= count })
            .`as`("expected %d matching log event(s) within %s, captured %d", count, within, captured.size)
            .isTrue()
        return captured.toList()
    }
}

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

/** [failure] and its causes, outermost first, stopping at a cause that is its own. */
fun causeChain(failure: Throwable?): List<Throwable> = generateSequence(failure) { it.cause.takeIf { cause -> cause !== it } }.toList()

/** The simple class names along [failure]'s cause chain - for an engine's exception whose type is shaded or optional. */
fun causeNames(failure: Throwable?): List<String> = causeChain(failure).map { it.javaClass.simpleName }

/**
 * What a request to a URL triggers once it reached WireMock, per test - behind ONE listener per server: WireMock's
 * `resetAll()` does not remove a request listener, so one added per test would pile up and fire for a later test's
 * request to the same URL. Register it once with the server (`addMockServiceRequestListener`) and [clear] it before
 * every test.
 */
class WireMockArrivals : RequestListener {
    private val actions = ConcurrentHashMap<String, () -> Unit>()

    override fun requestReceived(
        request: Request,
        response: Response,
    ) {
        actions[request.url]?.invoke()
    }

    /** A latch that opens once a request to [url] reached WireMock in the current test. */
    fun arrivalOf(url: String): CountDownLatch = CountDownLatch(1).also { latch -> actions[url] = latch::countDown }

    /** Runs [action] whenever a request to [url] reaches WireMock in the current test. */
    fun onArrival(
        url: String,
        action: () -> Unit,
    ) {
        actions[url] = action
    }

    /** Forgets every action of the test before. */
    fun clear() = actions.clear()
}
