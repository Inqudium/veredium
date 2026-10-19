package eu.inqudium.veredium.restclient

import io.github.resilience4j.bulkhead.BulkheadFullException
import org.slf4j.spi.LoggingEventBuilder
import org.springframework.web.client.RestClientResponseException
import java.util.concurrent.RejectedExecutionException

/**
 * The one decision "is this exchange failure the BACKEND's fault, or a benign client-side outcome?", owned by
 * neither the logging nor the resilience layer so that all consumers can depend on it without depending on each
 * other.
 *
 * Two consumers derive from it and therefore cannot disagree:
 *  - the synchronous error log's severity, in [ResilientRestClient];
 *  - the circuit breaker's record predicate, via the per-alias customizer in `resilience/MeshResilienceCustomizers.kt`.
 *
 * The exchange event itself (legatium-restclient-logging, logger `adapter-http-exchange`) is NOT a consumer: the
 * library classifies on its own terms - a 4xx answer is `rejected` with its status there, at INFO except for
 * 401/403/408/429 (legatium ADR-0012, `docs/decisions.md` D3) - and this module does not reach into it. The 404-means-business-case
 * reading lives in the two consumers above and in the runbook's reading of `adapter_response_status_code`.
 *
 * A neutral policy object rather than a member of either consumer, for the reason the WebClient twin learned the
 * hard way: a resilience/transport policy hidden in a logging class is the wrong owner and the wrong dependency
 * direction. The consumers reference a policy, not each other.
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
     * expected to handle, e.g. the 404 that says "this involved party does not exist". Such an exchange did not
     * succeed, but it is not an operational fault - logging it at ERROR would fire alerting on a business case
     * and bury the real failures underneath it.
     *
     * The type is [RestClientResponseException], the root of every status-carrying exception the blocking
     * client raises - `HttpClientErrorException.NotFound` and its siblings are subclasses - so the predicate
     * sees the exact objects [ResilientRestClient] throws for a 4xx answer.
     *
     * 408 and 429 are deliberately EXCLUDED (see [OPERATIONAL_CLIENT_STATUSES]): a request timeout and a
     * rate-limit rejection are operational signals, not business cases. They keep full severity AND are
     * recorded by the circuit breaker.
     *
     * Everything else is NOT expected and keeps full severity too: 5xx, a TimeLimiter timeout, a CircuitBreaker
     * rejection, a BulkheadFull, a transport error, or a deserialisation failure.
     */
    fun isExpectedClientError(error: Throwable?): Boolean =
        error is RestClientResponseException &&
            error.statusCode.is4xxClientError &&
            error.statusCode.value() !in OPERATIONAL_CLIENT_STATUSES

    /**
     * The startup line naming the backends this policy was applied to - the only evidence an operator has that
     * the amendment ran, and for WHICH aliases.
     *
     * The message carries ONE placeholder and therefore exactly one argument. The WebClient twin once added two,
     * the first being [ClientSideFailurePolicy.CLIENT_SIDE_FAILURES] copied from the sibling policy: SLF4J filled
     * the single slot with that list and silently dropped [name], so the line named neither the right backends
     * nor the right policy, and a placeholder/argument mismatch neither fails nor warns. Pinned by
     * `MeshResilienceStartupGuardTest`.
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
     * - [BulkheadFullException]: this pipeline's own load shedding. The bulkhead is the innermost decoration
     *   and the breaker the outermost (see [BlockingResilienceDecorator]), so a rejection travels outwards
     *   straight through the breaker - yet the backend was never even called.
     * - [ResponseBodyLimitExceededException]: the response exceeded `maxInMemorySize` (see
     *   [RestClientProperties]). The backend answered correctly, with a 200; this process declined to buffer
     *   the answer. The blocking counterpart of the reactive codec's `DataBufferLimitException`, raised by
     *   this module's own bounded reader rather than by a Spring codec - which is why it is a type of this
     *   module and not a Spring one.
     * - [InterruptedException]: the CALLER went away while waiting for the worker - its own deadline expired,
     *   its scope was cancelled, its executor is shutting down. The blocking counterpart of a subscriber
     *   cancelling, which the reactive twin's breaker never sees at all because a cancellation carries no
     *   error signal. Here it does: `BlockingResilienceDecorator` rethrows the `InterruptedException` out of the
     *   decorated call and `CircuitBreaker.decorateCallable` records every `Exception`, so without this entry
     *   a host that cancels callers routinely would open the circuit against a backend that answered nothing
     *   wrong - it was not even given the time to answer.
     * - [RejectedExecutionException]: the decorator's worker executor refused the submission because the
     *   decorator is already shut down - a call made during context teardown. `BlockingResilienceDecorator`
     *   submits the worker INSIDE the callable the breaker wraps (so that a short-circuited call submits
     *   nothing), which puts this refusal on the breaker's path like any other exception. The backend was
     *   never called.
     */
    val CLIENT_SIDE_FAILURES: List<Class<out Throwable>> =
        listOf(
            BulkheadFullException::class.java,
            ResponseBodyLimitExceededException::class.java,
            InterruptedException::class.java,
            RejectedExecutionException::class.java,
        )

    /** Bound on the cause walk, so a pathological cause chain cannot spin. */
    const val MAX_CAUSE_DEPTH = 10

    /**
     * Whether [error] is - or was CAUSED BY - a failure this client raised itself.
     *
     * The cause chain is walked on purpose, not defensively. The limit exception is thrown bare out of the
     * exchange today, but the same predicate must keep matching if a wrapper ever gets in between - Spring's
     * blocking client wraps every `IOException` of the exchange into a `ResourceAccessException`, and the
     * reactive twin saw exactly that kind of wrapping turn a class-based ignore into a silent no-op. Matching
     * on the thrown type alone - which is all `ignoreExceptions(Class...)` does - would not survive it.
     *
     * Walking the chain is safe here rather than merely convenient: a genuine backend failure does NOT carry
     * one of these as a cause. A 5xx whose error body exceeds the buffer limit yields a plain
     * `HttpServerErrorException` - the reader TRUNCATES an error body instead of rejecting it - so it is still
     * recorded. `ClientSideFailureCustomizerTest` pins exactly that.
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
     * Whether [error] is - or was caused by - specifically a [ResponseBodyLimitExceededException].
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
            if (candidate is ResponseBodyLimitExceededException) return true
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
