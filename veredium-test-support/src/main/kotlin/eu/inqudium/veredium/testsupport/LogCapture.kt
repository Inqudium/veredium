package eu.inqudium.veredium.testsupport

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import org.assertj.core.api.Assertions.assertThat
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList

/**
 * A Logback appender that captures the events [accept] lets through, from any thread - Logback's `ListAppender` is not
 * safe for that - and waits, bounded, for the ones a test expects. Attach it with [capturing], which also detaches
 * and stops it.
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

    /**
     * What has been captured so far - for asserting that something was NOT logged, where there is nothing to wait
     * for. The caller is responsible for having driven the call to completion first.
     */
    val events: List<ILoggingEvent> get() = captured.toList()

    override fun append(eventObject: ILoggingEvent) {
        if (!accept(eventObject)) return
        eventObject.prepareForDeferredProcessing()
        captured += eventObject
    }

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

/**
 * Runs [block] with a started [CapturingAppender] for the events [accept] lets through attached to [logger], and
 * returns what [block] returns. With a [level], the logger is opened to it for the duration. Afterwards - whatever
 * [block] did - the appender is detached and stopped and the logger's own level restored, so no test leaves a
 * recorder or a level behind on a process-wide logger.
 */
fun <T> capturing(
    logger: Logger,
    level: Level? = null,
    accept: (ILoggingEvent) -> Boolean = { true },
    block: (CapturingAppender) -> T,
): T {
    val appender = CapturingAppender(accept).apply { start() }
    val previousLevel = logger.level
    if (level != null) logger.level = level
    logger.addAppender(appender)
    try {
        return block(appender)
    } finally {
        logger.detachAppender(appender)
        appender.stop()
        logger.level = previousLevel
    }
}

/** The value of this event's key-value pair [name] - a field of legatium's exchange event; `null` when it carries none. */
fun ILoggingEvent.field(name: String): Any? = keyValuePairs?.singleOrNull { it.key == name }?.value
