package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.resilience.BreakerVerdicts
import eu.inqudium.veredium.core.resilience.ClientSideFailurePolicy
import eu.inqudium.veredium.core.resilience.HttpExchangeErrorPolicy
import io.github.resilience4j.bulkhead.BulkheadFullException
import org.springframework.core.io.buffer.DataBufferLimitException
import org.springframework.http.HttpStatus
import org.springframework.web.reactive.function.client.WebClientResponseException

/**
 * The reactive stack's reading of the shared breaker verdicts (`veredium-core`): which exception carries a
 * response status here, and which failures this client raises itself.
 *
 * The RULES - benign 4xx except 408/429 is a success, a client-side failure is ignored, the cause chain is
 * walked - live in [HttpExchangeErrorPolicy] and [ClientSideFailurePolicy] and are the same for both stacks.
 * What this object adds is only the stack's exception vocabulary, which is why it is the one place in this
 * module that names those types.
 */
object WebClientVerdicts {
    /**
     * The status-carrying type is [WebClientResponseException], which `WebClient` raises for every non-2xx
     * answer, so the predicate sees the exact objects [ResilientWebClient] surfaces for a 4xx. A codec failure
     * on a 200 is ALSO a `WebClientResponseException` (reporting 200) - it is not a 4xx and so never benign.
     */
    val httpExchangeError: HttpExchangeErrorPolicy = HttpExchangeErrorPolicy { error -> (error as? WebClientResponseException)?.statusCode?.value() }

    /**
     * The failures the backend is NOT at fault for on this stack. Extend HERE, and only for a failure that meets
     * the criterion in [ClientSideFailurePolicy]'s KDoc.
     *
     * - [BulkheadFullException]: this pipeline's own load shedding. The bulkhead is the innermost operator
     *   and the breaker the outermost (see [ReactiveResilienceDecorator]), so a rejection travels outwards
     *   straight through the breaker - yet the backend was never even called.
     * - [DataBufferLimitException]: the response exceeded `maxInMemorySize` (see [WebClientProperties]).
     *   The backend answered correctly, with a 200; this process declined to buffer the answer. Admitted
     *   only after the sizing question was settled - discounting it while the limit was suspected too low
     *   would have hidden a misconfiguration rather than excused a rare client-side refusal. The configured
     *   ceiling is therefore monitored and must be sized from observed payloads.
     *
     * A subscriber that cancels is NOT on this list, because it never reaches the breaker: a cancellation
     * carries no error signal through the reactive operators. (The blocking twin lists its `InterruptedException`
     * for exactly the case a cancellation covers here.)
     *
     * The cause walk is what makes the second entry work at all: Spring wraps the codec failure on an otherwise
     * successful response into a `WebClientResponseException` reporting **200 OK**, with the real cause
     * underneath (`200 OK from GET ..., but response failed with cause: DataBufferLimitException`). A 5xx whose
     * error body exceeds the limit yields a plain `WebClientResponseException` with no limit exception
     * underneath, so it is still recorded; `WebClientVerdictsTest` pins both.
     */
    val clientSideFailure: ClientSideFailurePolicy =
        ClientSideFailurePolicy(
            clientSideFailures = listOf(BulkheadFullException::class.java, DataBufferLimitException::class.java),
            bufferLimitFailure = DataBufferLimitException::class.java,
        )

    /** The two verdicts and the guard's probe - a 404 in the shape `WebClient` throws it. */
    val breakerVerdicts: BreakerVerdicts =
        BreakerVerdicts(
            httpExchangeError = httpExchangeError,
            clientSideFailure = clientSideFailure,
            benignClientErrorProbe = WebClientResponseException(HttpStatus.NOT_FOUND.value(), "Not Found", null, null, null),
        )
}
