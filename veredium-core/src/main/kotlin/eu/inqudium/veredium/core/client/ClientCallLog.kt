package eu.inqudium.veredium.core.client

import eu.inqudium.veredium.core.resilience.BreakerVerdicts
import io.micrometer.core.instrument.DistributionSummary
import jakarta.validation.ConstraintViolationException
import jakarta.validation.Validator
import org.slf4j.Logger
import org.slf4j.event.Level
import java.time.Duration

/**
 * What a resilient client of either stack does with a call besides the exchange: the transport lines it writes
 * under the caller's correlation keys, the size sample, the Bean Validation of the decoded response, and the
 * verdict-driven severity of the error line. One implementation, held once per client - the clients keep the
 * exchange, the (de)serialisation and their calling convention, and hand everything else here.
 *
 * ## The lines, and why they are the way they are
 *
 * Every line carries two fields that join it to legatium's exchange event: the backend name as `adapter_name`
 * and the URI template as `adapter_url_template` - the SAME wire names and values the library puts on the
 * `adapter-http-exchange` event ([ADAPTER_NAME_FIELD], [URL_TEMPLATE_FIELD]). One vocabulary for both loggers
 * is what lets a dashboard group either by the same field. The lines are written through the STACK's client
 * logger ([logger]), so their logger name - what a logging configuration and an operator's search address -
 * stays the client's.
 *
 * The error line's severity follows the SAME classification as the circuit breaker's record predicate
 * ([BreakerVerdicts.httpExchangeError]), so the two never disagree: a documented 4xx - which the adapter
 * contract declares as a normal outcome and the caller handles - is logged at DEBUG WITHOUT a stack trace,
 * everything else at WARN with one. Logging every non-2xx at ERROR would make routine "not found" answers
 * indistinguishable from real outages. (legatium's exchange event classifies on its own terms: a 4xx answer is
 * `rejected` with its status there, INFO except for 401/403/408/429.) The cause is attached ONLY to the
 * unexpected branch: a benign 4xx must not carry a stack trace that explains nothing the status and URL do not
 * - so the two branches must not be folded into a shared helper that takes the cause unconditionally.
 *
 * A cancellation - the caller going away, its budget expired or its scope cancelled - is DEBUG, deliberately:
 * normal, correlated with load, and under a disturbance arriving all at once, which is exactly when WARN must
 * not fill up with the one class of event that says nothing about the client's health. The operator-facing
 * signal lives in legatium's exchange event for the cancelled exchange.
 *
 * @property propertiesPrefix The stack's properties tree (`app.restclient` / `app.webclient`), named on the
 *   lines that tell an operator which key to change.
 * @property responseTimeout The transport's response timeout, read per line - a lambda, like the budget.
 * @property timeLimiterTimeout The TimeLimiter budget, read per line because it resolves through the
 *   Resilience4j registry and is not free - and deliberately not captured at construction.
 * @property validator Bean Validation for this backend's responses, or `null` when it is switched off for it.
 *   Null-as-off rather than a boolean flag: the per-backend decision is resolved ONCE by the stack's factory, so
 *   the hot path holds no toggle to consult and cannot disagree with the configuration mid-flight.
 * @property failOnViolation Whether a detected violation FAILS the call or is only REPORTED. Irrelevant when
 *   [validator] is null.
 * @property responseBodySize Records the size of each decoded response body, or `null` when the host wired no
 *   `MeterRegistry`. The number this exists to inform is the stack's `max-in-memory-size`: a ceiling that has to
 *   be picked, with nothing to pick it from. Recorded in UTF-8 BYTES ([ResponseBodyMetrics.utf8Size]) rather
 *   than `String.length`, so the sample and the limit it informs are the same unit.
 * @property bufferLimit What the line for a response that exceeded the buffer limit has to say in this stack's
 *   words.
 */
class ClientCallLog(
    private val logger: Logger,
    private val backendName: String,
    private val pathTemplate: String,
    private val propertiesPrefix: String,
    private val responseTimeout: () -> Duration,
    private val timeLimiterTimeout: () -> Duration,
    private val verdicts: BreakerVerdicts,
    private val validator: Validator?,
    private val failOnViolation: Boolean,
    private val responseBodySize: DistributionSummary?,
    private val bufferLimit: BufferLimitLine,
) {
    /**
     * The stack-specific half of the buffer-limit line.
     *
     * @property wording What the stack calls the limit (`buffer limit` for the blocking client's bounded read,
     *   `codec buffer` for the reactive codec).
     * @property maxInMemoryBytes The limit, in bytes, so the line carries the number an operator has to raise.
     * @property meterName The stack's response-size summary, named so the operator knows where the sizes that
     *   DO fit are.
     * @property contentLengthOf The `Content-Length` the response declared, read off the stack's exception, or
     *   `-1` when it declared none.
     */
    data class BufferLimitLine(
        val wording: String,
        val maxInMemoryBytes: Int,
        val meterName: String,
        val contentLengthOf: (Throwable) -> Long,
    )

    /**
     * The opening DEBUG line of a call. Guarded AND parameterised: this runs on every outbound call. A Kotlin
     * string template is built before the call regardless of the level, and two of the arguments are not free
     * - [timeLimiterTimeout] resolves through the R4j registry, and `take(100)` allocates - so the guard is what
     * keeps them off the hot path, not just the formatting.
     */
    fun logExecuting(
        responseType: Class<*>,
        requestBody: String?,
    ) {
        if (logger.isDebugEnabled) {
            log(
                Level.DEBUG,
                "Executing request to backend '{}' with responseType={} responseTimeout: {}ms," +
                    " timeLimiterTimeout: {}ms and requestBody={}",
                backendName,
                responseType.simpleName,
                responseTimeout().toMillis(),
                timeLimiterTimeout().toMillis(),
                requestBody?.take(100) ?: "null",
            )
        }
    }

    /** Samples the decoded body's size, when the host wired a registry. */
    fun recordBodySize(rawBody: String) {
        responseBodySize?.record(ResponseBodyMetrics.utf8Size(rawBody).toDouble())
    }

    /**
     * Validates [value] against its Bean Validation constraints, or returns it untouched when validation is
     * off for this backend ([validator] is then null - the decision is made once at construction rather than
     * per call).
     *
     * On a violation the behaviour depends on [failOnViolation]:
     * - `true` (enforce): throws [ConstraintViolationException] carrying every violation, so the caller sees
     *   which field of which type broke the contract instead of a bare "invalid". The failure is NOT an
     *   expected client error of [BreakerVerdicts.httpExchangeError], so it keeps full severity: a backend
     *   answering outside its own published schema is an anomaly worth a WARN with its cause, not a
     *   business-as-usual outcome.
     * - `false` (report-only): logs the same detail at WARN and returns [value] unchanged, so a backend can be
     *   observed against real traffic before its violations are made fatal.
     *
     * Both clients run this OUTSIDE the Resilience4j decoration: a response that violates its own schema is a
     * contract problem, not an outage, and must not open the circuit breaker.
     */
    fun <T : Any> validate(value: T): T {
        if (validator != null) {
            log(Level.TRACE, "Going to validate response value for backend '{}'", backendName)
            val violations = validator.validate(value).orEmpty()
            if (violations.isNotEmpty()) {
                val msg =
                    "Response from backend '$backendName' violates its own schema: " +
                        violations.joinToString("; ") { "${it.propertyPath} ${it.message}" }
                if (failOnViolation) {
                    log(Level.WARN, msg)
                    throw ConstraintViolationException(msg, violations)
                }
                log(
                    Level.WARN,
                    "{} - reported only, not enforced ($propertiesPrefix.validation.fail-on-violation=false)",
                    msg,
                )
            } else {
                log(Level.DEBUG, "Response from backend '{}' has no violations", backendName)
            }
        } else {
            log(Level.TRACE, "Validation of the response value for backend '{}' disabled", backendName)
        }
        return value
    }

    /**
     * The error line of a failed call (class KDoc for its severity), followed by the buffer-limit line when the
     * failure is one. [elapsedMillis] is the attempt's own clock, measured by the client regardless of any
     * logging configuration.
     */
    fun onError(
        responseType: Class<*>,
        error: Throwable,
        elapsedMillis: () -> Long,
    ) {
        val expected = verdicts.httpExchangeError.isExpectedClientError(error)
        val level = if (expected) Level.DEBUG else Level.WARN
        log(
            level = level,
            msg =
                "Error during request to backend '{}' with responseType={} responseTimeout: {}ms," +
                    " timeLimiterTimeout: {}ms duration so far: {}ms{}",
            cause = error.takeUnless { expected },
            backendName,
            responseType.simpleName,
            responseTimeout().toMillis(),
            timeLimiterTimeout().toMillis(),
            elapsedMillis(),
            if (expected) " - expected client error: $error" else "",
        )
        logIfBufferLimitExceeded(error)
    }

    /**
     * The one line that says how far the buffer limit was missed, for the calls the summary cannot sample.
     *
     * [responseBodySize] only ever sees bodies that FIT, so a response too large for the buffer leaves no
     * measurement at all - the read stops and no `String` is ever produced. `Content-Length`, read off the
     * response and carried on the exception, is the only figure available for it.
     *
     * **It is a LOWER bound, and the message says so.** The header is the WIRE size: with content compression
     * on the transport, a compressed body reports far less than the decompressed bytes the limit actually bounds.
     * It may also be absent entirely under chunked encoding, which is why the line still carries the limit on
     * its own - `limit=N, Content-Length=unknown` is a useful line, `Content-Length` alone is not.
     *
     * WARN and unconditional: this is a permanent, deterministic condition for the party whose response it is,
     * not a blip, and one line per occurrence is what makes it findable. The generic error line already fired
     * for the same call, but it names neither number.
     */
    private fun logIfBufferLimitExceeded(error: Throwable) {
        if (!verdicts.clientSideFailure.isBufferLimitFailure(error)) return
        val contentLength = bufferLimit.contentLengthOf(error)
        // Through log(), like every other line: it carries the two join fields (class KDoc) - written straight
        // to the logger, it would be the one transport line no dashboard can group next to its exchange event.
        log(
            Level.WARN,
            "Response body exceeded the {} on backend '{}': limit={} bytes, Content-Length={}" +
                " (wire size, a lower bound - the body may be compressed). Raise {}.max-in-memory-size;" +
                " see {} for the sizes that do fit.",
            bufferLimit.wording,
            backendName,
            bufferLimit.maxInMemoryBytes,
            if (contentLength >= 0) contentLength.toString() else "unknown",
            propertiesPrefix,
            bufferLimit.meterName,
        )
    }

    /** The cancellation line of an abandoned call - at DEBUG, deliberately (class KDoc). */
    fun onCancel(
        responseType: Class<*>,
        elapsedMillis: () -> Long,
    ) {
        log(
            level = Level.DEBUG,
            msg =
                "Cancelled request to backend '{}' with responseType={} " +
                    "responseTimeout: {}ms," +
                    " timeLimiterTimeout: {}ms duration so far: {}ms",
            backendName,
            responseType.simpleName,
            responseTimeout().toMillis(),
            timeLimiterTimeout().toMillis(),
            elapsedMillis(),
        )
    }

    /** Writes one transport line without a throwable - [log] with its `error` left out. */
    fun log(
        level: Level,
        msg: String,
        vararg arguments: Any?,
    ) {
        log(level, msg, null, *arguments)
    }

    /** Writes one transport line with the two fields that join it to the exchange event (class KDoc). */
    fun log(
        level: Level,
        msg: String,
        cause: Throwable?,
        vararg arguments: Any?,
    ) {
        val event = logger.atLevel(level)
        event.setMessage(msg)
        for (arg in arguments) {
            event.addArgument(arg?.toString())
        }
        event.addKeyValue(ADAPTER_NAME_FIELD, backendName)
        event.addKeyValue(URL_TEMPLATE_FIELD, pathTemplate)
        cause?.let { event.setCause(it) }
        event.log()
    }

    companion object {
        /**
         * The backend-name wire name the transport lines share with legatium's exchange event
         * (`ClientLogField.NAME` there). Spelled out because that enum is internal to the library; it is a
         * contract with the log index and with the operators' log queries, and each stack's integration test
         * pins it.
         */
        const val ADAPTER_NAME_FIELD: String = "adapter_name"

        /**
         * The URI-template wire name the transport lines share with legatium's exchange event
         * (`ClientLogField.URL_TEMPLATE` there); spelled out and pinned like [ADAPTER_NAME_FIELD].
         */
        const val URL_TEMPLATE_FIELD: String = "adapter_url_template"
    }
}
