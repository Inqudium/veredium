package eu.inqudium.veredium.webclient

import io.github.resilience4j.bulkhead.BulkheadFullException
import org.slf4j.spi.LoggingEventBuilder
import org.springframework.core.io.buffer.DataBufferLimitException
import org.springframework.web.reactive.function.client.WebClientResponseException

/**
 * The one decision "is this exchange failure the BACKEND's fault, or a benign client-side outcome?", owned by
 * neither the logging nor the resilience layer so that all consumers can depend on it without depending on each
 * other.
 *
 * Two consumers derive from it and therefore cannot disagree:
 *  - the synchronous error log's severity, in [ResilientWebClient];
 *  - the circuit breaker's ignore predicate, via [ExpectedClientErrorIgnoreRegistrar].
 *
 * The exchange event itself (legatium-webclient-logging, logger `adapter-http-exchange`) is NOT a consumer: the
 * library classifies on its own terms - a 4xx answer is `rejected` with its status there, at INFO except for
 * 401/403/408/429 (legatium ADR-0012) - and this module does not reach into it. The 404-means-business-case reading lives in the two
 * consumers above and in the runbook's reading of `adapter_response_status_code`.
 *
 * It lived in the module's former exchange diary while logging was its only consumer - a natural home then. Once
 * the breaker and the transport's error log came to share it, a resilience/transport policy hidden in a logging class was the
 * wrong owner, and the wrong dependency direction: a cross-cutting concern reaching into a logging utility for a
 * core decision. It is a neutral policy object here instead - the consumers reference a policy, not each other.
 */
object HttpExchangeErrorPolicy {
    /**
     * The two 4xx statuses that are operational signals rather than business cases: 408 Request Timeout and
     * 429 Too Many Requests. A 408 is a server-side timeout, a 429 is backpressure; a run of either is worth
     * alerting on and worth opening the circuit for. They are therefore held OUT of [isExpectedClientError],
     * which keeps them at full log severity and lets the circuit breaker record them.
     */
    private val OPERATIONAL_CLIENT_STATUSES = setOf(408, 429)

    /**
     * True for a failure the adapter contract documents as a NORMAL outcome: a benign 4xx the caller is
     * expected to handle, e.g. the 404 that says "this involved party does not exist" (see the `@throws` notes
     * on the adapter methods). Such an exchange did not succeed, but it is not an operational fault - logging
     * it at ERROR would fire alerting on a business case and bury the real failures underneath it.
     *
     * 408 and 429 are deliberately EXCLUDED (see [OPERATIONAL_CLIENT_STATUSES]): a request timeout and a
     * rate-limit rejection are operational signals, not business cases. They keep full severity AND are
     * recorded by the circuit breaker.
     *
     * Everything else is NOT expected and keeps full severity too: 5xx, a TimeLimiter timeout, a CircuitBreaker
     * rejection, a BulkheadFull, a transport error, or a deserialisation failure.
     *
     * Single source of truth for the distinction: the synchronous error log ([ResilientWebClient]) and the
     * circuit breaker's ignore predicate ([ExpectedClientErrorIgnoreRegistrar]) both derive from it, so they can
     * never disagree.
     */
    fun isExpectedClientError(error: Throwable?): Boolean =
        error is WebClientResponseException &&
            error.statusCode.is4xxClientError &&
            error.statusCode.value() !in OPERATIONAL_CLIENT_STATUSES

    /**
     * The startup line naming the backends this policy was applied to - the only evidence an operator has that
     * the amendment ran, and for WHICH aliases.
     *
     * The message carries ONE placeholder and therefore exactly one argument. It used to add two, the first
     * being [ClientSideFailurePolicy.CLIENT_SIDE_FAILURES] copied from the sibling policy: SLF4J filled the
     * single slot with that list and silently dropped [name], so the line read "for backend(s):
     * BulkheadFullException, DataBufferLimitException" - it named neither the right backends nor the right
     * policy, and a placeholder/argument mismatch neither fails nor warns. Pinned by
     * `ExpectedClientErrorIgnoreRegistrarTest`.
     */
    fun logPolicy(
        logBuilder: LoggingEventBuilder,
        name: String,
    ) {
        logBuilder
            .setMessage(
                "CircuitBreaker now ignores benign 4xx (every client error except 408/429) for backend(s): " +
                    "{} - a business-case rejection cannot open a breaker against a healthy backend",
            ).addArgument(name)
            .log()
    }
}

object ClientSideFailurePolicy {
    /**
     * The failures the backend is NOT at fault for, and which the CircuitBreaker must therefore not count.
     * Extend HERE, and only for a failure that meets the criterion in the class KDoc. Matched against the
     * thrown exception AND its causes - see [isClientSideFailure].
     *
     * - [BulkheadFullException]: this pipeline's own load shedding. The bulkhead is the innermost operator
     *   and the breaker the outermost (see [ReactiveResilienceDecorator]), so a rejection travels outwards
     *   straight through the breaker - yet the backend was never even called.
     * - [DataBufferLimitException]: the response exceeded `maxInMemorySize` (see [WebClientProperties]).
     *   The backend answered correctly, with a 200; this process declined to buffer the answer. Admitted
     *   only after the sizing question was settled - discounting it while the limit was suspected too low
     *   would have hidden a misconfiguration rather than excused a rare client-side refusal. The configured
     *   ceiling is therefore monitored and must be sized from observed payloads.
     */
    val CLIENT_SIDE_FAILURES: List<Class<out Throwable>> =
        listOf(BulkheadFullException::class.java, DataBufferLimitException::class.java)

    /** Bound on the cause walk, so a pathological cause chain cannot spin. */
    const val MAX_CAUSE_DEPTH = 10

    /**
     * Whether [error] is - or was CAUSED BY - a failure this client raised itself.
     *
     * The cause chain has to be walked, and that is not defensive coding. Spring wraps a codec failure on an
     * otherwise successful response into a `WebClientResponseException` reporting **200 OK**, with the real
     * cause underneath: `200 OK from GET ..., but response failed with cause: DataBufferLimitException`.
     * Matching on the thrown type alone - which is all `ignoreExceptions(Class...)` does - would therefore
     * never see it, and the entry would be a silent no-op.
     *
     * Walking the chain is safe here rather than merely convenient: a genuine backend failure does NOT carry
     * one of these as a cause. Verified for the case that would break it - a 5xx whose error body exceeds the
     * buffer limit yields a plain `WebClientResponseException` with no `DataBufferLimitException` underneath,
     * so it is still recorded. `ClientSideFailureIgnoreRegistrarTest` pins exactly that.
     */
    fun isClientSideFailure(error: Throwable): Boolean {
        var candidate: Throwable? = error
        var depth = 0
        while (candidate != null && depth++ < MAX_CAUSE_DEPTH) {
            if (CLIENT_SIDE_FAILURES.any { it.isInstance(candidate) }) return true
            candidate = candidate.cause.takeIf { it !== candidate }
        }
        return false
    }

    /**
     * Whether [error] is - or was caused by - specifically a [DataBufferLimitException].
     *
     * The narrow sibling of [isClientSideFailure], which folds this together with `BulkheadFullException`
     * because the BREAKER treats both the same way. The two differ in every other respect: a full bulkhead is
     * transient and says nothing about the response, while a buffer overrun is permanent for the party whose
     * response it is and is fixed by one configuration value. Only the second one warrants naming that value,
     * so only the second one is asked for here.
     */
    fun isBufferLimitFailure(error: Throwable): Boolean {
        var candidate: Throwable? = error
        var depth = 0
        while (candidate != null && depth++ < MAX_CAUSE_DEPTH) {
            if (candidate is DataBufferLimitException) return true
            candidate = candidate.cause.takeIf { it !== candidate }
        }
        return false
    }

    fun logPolicy(
        logBuilder: LoggingEventBuilder,
        name: String,
    ) {
        logBuilder
            .setMessage(
                "CircuitBreaker now ignores {} for backend(s): {} - " +
                    "a fault this client causes itself cannot open a breaker",
            ).addArgument(CLIENT_SIDE_FAILURES.joinToString(", ") { it.simpleName })
            .addArgument(name)
            .log()
    }
}
