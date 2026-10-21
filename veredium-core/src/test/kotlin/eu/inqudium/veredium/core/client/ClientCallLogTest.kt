package eu.inqudium.veredium.core.client

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.ThrowableProxy
import ch.qos.logback.core.read.ListAppender
import eu.inqudium.veredium.core.resilience.TestBufferLimitException
import eu.inqudium.veredium.core.resilience.TestResponseException
import eu.inqudium.veredium.core.resilience.TestVerdicts
import eu.inqudium.veredium.core.validation.ResponseValidation
import eu.inqudium.veredium.core.validation.ResponseValidationProperties
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import jakarta.validation.ConstraintViolationException
import jakarta.validation.Validation
import jakarta.validation.Validator
import jakarta.validation.ValidatorFactory
import jakarta.validation.constraints.Size
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.assertj.core.api.Assertions.entry
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

/**
 * Unit tests for [ClientCallLog], the call-side logging, size sampling and validation both client stacks share -
 * against the stand-in verdicts of [TestVerdicts] and a Logback [ListAppender] on a logger of this test's own,
 * so no HTTP stack is needed and every line can be read back with its level, cause and key-value pairs.
 *
 * Both client suites run this class end to end, but JaCoCo measures per module, so without this test the core
 * reported it at 0 % - and the level policy, the one rule here an alert depends on, was pinned only indirectly.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClientCallLogTest {
    private val logger = LoggerFactory.getLogger("eu.inqudium.veredium.core.client.ClientCallLogTest.client") as Logger
    private val appender = ListAppender<ILoggingEvent>()
    private lateinit var validatorFactory: ValidatorFactory

    @BeforeAll
    fun openFactory() {
        validatorFactory = Validation.buildDefaultValidatorFactory()
    }

    @AfterAll
    fun closeFactory() {
        validatorFactory.close()
    }

    @BeforeEach
    fun attachAppender() {
        appender.list.clear()
        appender.start()
        logger.addAppender(appender)
        logger.level = Level.TRACE
    }

    @AfterEach
    fun detachAppender() {
        logger.detachAppender(appender)
        appender.stop()
        logger.level = null
        MDC.clear()
    }

    @Nested
    inner class `The error line` {
        @Test
        fun `should log a benign 4xx at DEBUG without a stack trace`() {
            // What is tested: the expected-client-error branch of the level policy - a 404 in the stack's shape.
            // Success criteria: exactly one line, at DEBUG, with NO cause attached, naming the backend and the
            //   error itself in its text, and carrying the attempt's numbers.
            // Why it matters: the severity follows the breaker's own classification. A 404 that the adapter
            //   contract declares as a normal outcome at WARN with a stack trace is what made routine "not found"
            //   answers indistinguishable from outages - and the reactive stack once lost exactly the "no cause"
            //   half of this when it folded the two branches into one helper.
            //
            // Given
            val log = callLog()

            // When
            log.onError(String::class.java, TestResponseException(404)) { 17 }

            // Then
            val line = appender.list.single()
            assertThat(line.level).isEqualTo(Level.DEBUG)
            assertThat(line.throwableProxy).isNull()
            assertThat(line.formattedMessage)
                .contains("backend '$BACKEND'", "responseType=String", "responseTimeout: 1400ms", "timeLimiterTimeout: 1800ms")
                .contains("duration so far: 17ms", " - expected client error: ", "status 404")
        }

        @Test
        fun `should log every other failure at WARN with its cause attached`() {
            // What is tested: the unexpected branch of the level policy, for a 5xx and for the two operational
            //   4xx (408, 429) the policy deliberately keeps out of the benign set.
            // Success criteria: one WARN line per failure, each carrying THE failure as its cause, and none of
            //   them labelled an expected client error.
            // Why it matters: these are the lines an alert is built on. A 429 at DEBUG would hide backpressure;
            //   a WARN without the cause would leave nothing to diagnose it with.
            //
            // Given
            val log = callLog()
            val failures = listOf(TestResponseException(503), TestResponseException(408), TestResponseException(429), IllegalStateException("boom"))

            // When
            failures.forEach { log.onError(String::class.java, it) { 5 } }

            // Then
            assertThat(appender.list).hasSize(failures.size)
            appender.list.zip(failures).forEach { (line, failure) ->
                assertThat(line.level).`as`("level for %s", failure).isEqualTo(Level.WARN)
                assertThat((line.throwableProxy as ThrowableProxy).throwable).`as`("cause for %s", failure).isSameAs(failure)
                assertThat(line.formattedMessage).doesNotContain("expected client error")
            }
        }

        @Test
        fun `should follow a buffer-limit failure with the line naming the limit and the declared size`() {
            // What is tested: the buffer-limit line - after the generic WARN, a second WARN with the stack's
            //   wording, the limit, the declared Content-Length (or "unknown" without one), the key to raise and
            //   the meter that holds the sizes that fit; for a failure that is NOT a buffer overrun, no such line.
            // Success criteria: two lines for the overrun with a Content-Length, two (with "unknown") for one
            //   without, one line for an unrelated failure.
            // Why it matters: the size meter only sees bodies that fit, so this line is the only figure for the
            //   ones that do not - and the number an operator has to change is named nowhere else.
            //
            // Given
            val log = callLog(contentLength = { error -> if (error.message == "declared") 4096 else -1 })

            // When
            log.onError(String::class.java, TestBufferLimitException("declared")) { 3 }
            log.onError(String::class.java, TestBufferLimitException("chunked")) { 3 }
            log.onError(String::class.java, IllegalStateException("unrelated")) { 3 }

            // Then
            assertThat(appender.list).hasSize(5)
            val limitLines = appender.list.filter { it.formattedMessage.startsWith("Response body exceeded") }
            assertThat(limitLines).hasSize(2).allSatisfy { assertThat(it.level).isEqualTo(Level.WARN) }
            assertThat(limitLines[0].formattedMessage)
                .contains("exceeded the test buffer on backend '$BACKEND'", "limit=1024 bytes", "Content-Length=4096")
                .contains("Raise $PREFIX.max-in-memory-size", "see test.response.body.size")
            assertThat(limitLines[1].formattedMessage).contains("Content-Length=unknown")
        }
    }

    @Nested
    inner class `The other lines` {
        @Test
        fun `should log a cancellation at DEBUG without a cause`() {
            // What is tested: the cancellation line of an abandoned call.
            // Success criteria: one DEBUG line, no cause, with the backend and the attempt's numbers.
            // Why it matters: cancellations arrive all at once under a disturbance; at WARN they would bury the
            //   lines that say something about the backend's health.
            //
            // Given
            val log = callLog()

            // When
            log.onCancel(String::class.java) { 250 }

            // Then
            val line = appender.list.single()
            assertThat(line.level).isEqualTo(Level.DEBUG)
            assertThat(line.throwableProxy).isNull()
            assertThat(line.formattedMessage).contains("Cancelled request to backend '$BACKEND'", "duration so far: 250ms")
        }

        @Test
        fun `should write the opening line only when DEBUG is on, without resolving the budget otherwise`() {
            // What is tested: the guard of the per-call opening line, and the truncation of the request body.
            // Success criteria: at INFO nothing is written AND the budget lambda is never invoked; at DEBUG one
            //   line carries the budget and the body cut to its first 100 characters.
            // Why it matters: this runs on every outbound call, and the budget resolves through the Resilience4j
            //   registry - work the guard exists to keep off the hot path when nobody reads the line.
            //
            // Given
            val budgetReads = AtomicInteger()
            val log = callLog(timeLimiterTimeout = { Duration.ofMillis(1800).also { budgetReads.incrementAndGet() } })
            val body = "x".repeat(150)

            // When: once at INFO, once at DEBUG
            logger.level = Level.INFO
            log.logExecuting(String::class.java, body)
            val readsAtInfo = budgetReads.get()
            logger.level = Level.DEBUG
            log.logExecuting(String::class.java, body)

            // Then
            assertThat(readsAtInfo).isZero()
            val line = appender.list.single()
            assertThat(line.level).isEqualTo(Level.DEBUG)
            assertThat(line.formattedMessage).contains("timeLimiterTimeout: 1800ms", "requestBody=" + "x".repeat(100))
            assertThat(line.formattedMessage).doesNotContain("x".repeat(101))
        }

        @Test
        fun `should carry the backend name and the URI template as key-value pairs on every line`() {
            // What is tested: the two fields that join a transport line to legatium's exchange event, on each
            //   kind of line this class writes.
            // Success criteria: every line - error, buffer limit, cancel, opening, validation - carries
            //   adapter_name=<backend> and adapter_url_template=<template> as SLF4J key-value pairs.
            // Why it matters: a dashboard groups both loggers by the same field; a line without them is a line no
            //   query finds next to its exchange event.
            //
            // Given
            val log = callLog(validator = englishValidator())

            // When
            log.onError(String::class.java, TestBufferLimitException()) { 1 }
            log.onCancel(String::class.java) { 1 }
            log.logExecuting(String::class.java, null)
            log.validate(Dto("ok"))

            // Then
            assertThat(appender.list).hasSizeGreaterThanOrEqualTo(5)
            appender.list.forEach { line ->
                val pairs = line.keyValuePairs.orEmpty().associate { it.key to it.value }
                assertThat(pairs)
                    .`as`("key-values of '%s'", line.formattedMessage)
                    .containsOnly(
                        entry(ClientCallLog.ADAPTER_NAME_FIELD, BACKEND),
                        entry(ClientCallLog.URL_TEMPLATE_FIELD, TEMPLATE),
                    )
            }
        }

        @Test
        fun `should record the body size in UTF-8 bytes, and nothing without a summary`() {
            // What is tested: the size sample - bytes, not UTF-16 units - and the no-registry case.
            // Success criteria: "aä" records 3 (1 + 2 bytes); a log without a summary accepts the call silently.
            // Why it matters: the sample sizes max-in-memory-size, which is a byte limit; counting characters
            //   under-states every non-ASCII body.
            //
            // Given
            val summary = DistributionSummary.builder("test.response.body.size").register(SimpleMeterRegistry())

            // When
            callLog(responseBodySize = summary).recordBodySize("aä")
            callLog(responseBodySize = null).recordBodySize("aä")

            // Then
            assertThat(summary.count()).isEqualTo(1)
            assertThat(summary.totalAmount()).isEqualTo(3.0)
        }
    }

    @Nested
    inner class `Response validation` {
        @Test
        fun `should fail the call with every violation when enforcing`() {
            // What is tested: enforce mode (fail-on-violation=true) on a DTO that breaks its constraint.
            // Success criteria: a ConstraintViolationException naming the backend and the field, and a WARN line
            //   with the same text.
            // Why it matters: a backend answering outside its own published schema must surface as itself, with
            //   the field that broke - not as a bare "invalid" further down the caller's code.
            //
            // Given
            val log = callLog(validator = englishValidator(), failOnViolation = true)

            // When
            val thrown = catchThrowable { log.validate(Dto("too long")) }

            // Then
            assertThat(thrown)
                .isInstanceOf(ConstraintViolationException::class.java)
                .hasMessageContaining("Response from backend '$BACKEND' violates its own schema: value size must be between 0 and 3")
            assertThat((thrown as ConstraintViolationException).constraintViolations).hasSize(1)
            assertThat(
                appender.list
                    .filter { it.level == Level.WARN }
                    .single()
                    .formattedMessage,
            ).isEqualTo(thrown.message)
        }

        @Test
        fun `should report a violation and return the value unchanged when report-only`() {
            // What is tested: report-only mode (fail-on-violation=false).
            // Success criteria: the very same value comes back; one WARN line carries the violation and names the
            //   key that makes it fatal.
            // Why it matters: report-only is how a backend is observed against real traffic before enforcing; it
            //   must neither fail the call nor stay silent.
            //
            // Given
            val log = callLog(validator = englishValidator(), failOnViolation = false)
            val invalid = Dto("too long")

            // When
            val returned = log.validate(invalid)

            // Then
            assertThat(returned).isSameAs(invalid)
            val warning = appender.list.single { it.level == Level.WARN }.formattedMessage
            assertThat(warning)
                .contains("violates its own schema: value size must be between 0 and 3")
                .contains("reported only, not enforced ($PREFIX.validation.fail-on-violation=false)")
        }

        @Test
        fun `should pass a valid value and skip validation entirely without a validator`() {
            // What is tested: the two non-violating paths - a valid DTO, and validation switched off (null
            //   validator) for an invalid one.
            // Success criteria: both values come back unchanged, no WARN is written, and each path leaves its
            //   own trace line (no violations / disabled).
            // Why it matters: validation is off by default; an off switch that still rejected, or a valid value
            //   that warned, would make the feature unadoptable one backend at a time.
            //
            // Given
            val valid = Dto("ok")
            val invalid = Dto("too long")

            // When
            val validated = callLog(validator = englishValidator()).validate(valid)
            val unvalidated = callLog(validator = null).validate(invalid)

            // Then
            assertThat(validated).isSameAs(valid)
            assertThat(unvalidated).isSameAs(invalid)
            assertThat(appender.list.filter { it.level == Level.WARN }).isEmpty()
            assertThat(appender.list.map { it.formattedMessage })
                .anyMatch { it.contains("has no violations") }
                .anyMatch { it.contains("disabled") }
        }
    }

    @Nested
    inner class `Running with a snapshot MDC` {
        @Test
        fun `should install the snapshot for the block and restore the thread's own MDC afterwards`() {
            // What is tested: withMdc with a snapshot, on a thread that has an MDC of its own.
            // Success criteria: inside the block the MDC is exactly the snapshot (the thread's own key hidden);
            //   the block's value is returned; afterwards the thread's own MDC is back, without the snapshot's key.
            // Why it matters: the reactive stack writes its error and cancel lines through this on Netty event
            //   loops; a snapshot left behind would stamp the next request's lines on that thread with this
            //   request's correlation id.
            //
            // Given
            MDC.put("THREAD_OWN", "event-loop")

            // When
            val inside = ClientCallLog.withMdc(mapOf("CORRELATION_ID" to "Out:1")) { MDC.getCopyOfContextMap().orEmpty() }

            // Then
            assertThat(inside).containsExactly(entry("CORRELATION_ID", "Out:1"))
            assertThat(MDC.getCopyOfContextMap()).containsExactly(entry("THREAD_OWN", "event-loop"))
        }

        @Test
        fun `should clear the MDC for a null snapshot and restore it even when the block throws`() {
            // What is tested: the null snapshot (the caller had no MDC) and the failure path of the restoration.
            // Success criteria: a null snapshot runs the block on an EMPTY MDC; a block that throws propagates
            //   its exception and still leaves the thread's own MDC in place; a thread without an MDC is left
            //   without one.
            // Why it matters: a line must never borrow another request's keys from the thread it happens to run
            //   on, and an exception inside a log call must not be the way a snapshot leaks.
            //
            // Given
            MDC.put("THREAD_OWN", "event-loop")
            val failure = IllegalStateException("inside")

            // When
            val insideNull = ClientCallLog.withMdc(null) { MDC.getCopyOfContextMap().orEmpty() }
            val thrown = catchThrowable { ClientCallLog.withMdc(mapOf("CORRELATION_ID" to "Out:2")) { throw failure } }
            val afterThrow = MDC.getCopyOfContextMap()
            MDC.clear()
            ClientCallLog.withMdc(mapOf("CORRELATION_ID" to "Out:3")) { }
            val withoutOwn = MDC.getCopyOfContextMap().orEmpty()

            // Then
            assertThat(insideNull).isEmpty()
            assertThat(thrown).isSameAs(failure)
            assertThat(afterThrow).containsExactly(entry("THREAD_OWN", "event-loop"))
            assertThat(withoutOwn).isEmpty()
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** A DTO with one constraint: [value] may be at most three characters. */
    data class Dto(
        @field:Size(max = 3)
        val value: String,
    )

    /** The validator the clients use - the host's factory with messages pinned to English. */
    private fun englishValidator(): Validator = ResponseValidation(ResponseValidationProperties(enabled = true), validatorFactory).validator!!

    private fun callLog(
        validator: Validator? = null,
        failOnViolation: Boolean = true,
        responseBodySize: DistributionSummary? = null,
        timeLimiterTimeout: () -> Duration = { Duration.ofMillis(1800) },
        contentLength: (Throwable) -> Long = { -1 },
    ): ClientCallLog =
        ClientCallLog(
            logger = logger,
            backendName = BACKEND,
            pathTemplate = TEMPLATE,
            propertiesPrefix = PREFIX,
            responseTimeout = { Duration.ofMillis(1400) },
            timeLimiterTimeout = timeLimiterTimeout,
            verdicts = TestVerdicts.breakerVerdicts,
            validator = validator,
            failOnViolation = failOnViolation,
            responseBodySize = responseBodySize,
            bufferLimit =
                ClientCallLog.BufferLimitLine(
                    wording = "test buffer",
                    maxInMemoryBytes = 1024,
                    meterName = "test.response.body.size",
                    contentLengthOf = contentLength,
                ),
        )

    private companion object {
        const val BACKEND = "api-example-com-GET-accounts"
        const val TEMPLATE = "/accounts/{accountId}"
        const val PREFIX = "app.test"
    }
}
