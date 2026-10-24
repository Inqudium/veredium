package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import eu.inqudium.veredium.core.resilience.BreakerVerdicts
import eu.inqudium.veredium.core.resilience.ClientSideFailurePolicy
import eu.inqudium.veredium.core.resilience.HttpExchangeErrorPolicy
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.RestClientResponseException

/**
 * The blocking stack's reading of the shared breaker verdicts (`veredium-core`): which exception carries a
 * response status here, and which failures this client raises itself.
 *
 * The RULES - benign 4xx except 408/429 is a success, a client-side failure is ignored, the cause chain is
 * walked - live in [HttpExchangeErrorPolicy] and [ClientSideFailurePolicy] and are the same for both stacks.
 * What this object adds is only the stack's exception vocabulary, which is why it is the one place in this
 * module that names those types.
 */
object RestClientVerdicts {
    /**
     * The status-carrying type is [RestClientResponseException], the root of every status-carrying exception
     * the blocking client raises - `HttpClientErrorException.NotFound` and its siblings are subclasses - so
     * the predicate sees the exact objects [ResilientRestClient] throws for a 4xx answer.
     */
    val httpExchangeError: HttpExchangeErrorPolicy = HttpExchangeErrorPolicy { error -> (error as? RestClientResponseException)?.statusCode?.value() }

    /**
     * The failures the backend is NOT at fault for on this stack. Extend HERE, and only for a failure that meets
     * the criterion in [ClientSideFailurePolicy]'s KDoc.
     *
     * - The three the pipeline raises itself, declared by the decorator that raises them
     *   ([BlockingResilienceDecorator.SELF_INFLICTED_FAILURES]): the bulkhead's rejection, the CALLER's
     *   `InterruptedException` (the blocking counterpart of a subscriber cancelling, which the reactive twin's
     *   breaker never sees because a cancellation carries no error signal - here it does, and without the entry
     *   a host that cancels callers routinely would open the circuit against a backend that was not even given
     *   the time to answer), and the `RejectedExecutionException` of a worker submitted after shutdown.
     * - [ResponseBodyLimitExceededException]: the response exceeded [RestClientProperties.maxInMemorySize]. The
     *   backend answered correctly, with a 200; this process declined to buffer the answer. The blocking
     *   counterpart of the reactive codec's `DataBufferLimitException`, raised by this module's own bounded reader
     *   rather than by a Spring codec - which is why it is a type of this module and not a Spring one.
     *
     * The cause walk matters on this stack. Assumption: Spring's blocking client wraps every `IOException` of the
     * exchange into a `ResourceAccessException` (spring-web 7.0); the limit exception is thrown bare today, but the
     * predicate must keep matching if a wrapper ever gets in between. A 5xx whose error body exceeds the buffer
     * limit yields a plain `HttpServerErrorException` - the reader TRUNCATES an error body instead of rejecting
     * it - so it is still recorded.
     */
    val clientSideFailure: ClientSideFailurePolicy =
        ClientSideFailurePolicy(
            clientSideFailures = BlockingResilienceDecorator.SELF_INFLICTED_FAILURES + ResponseBodyLimitExceededException::class.java,
            bufferLimitFailure = ResponseBodyLimitExceededException::class.java,
        )

    /** The two verdicts and the guard's probe - a 404 in the shape the client throws it, Spring's status-specific subclass. */
    val breakerVerdicts: BreakerVerdicts =
        BreakerVerdicts(
            httpExchangeError = httpExchangeError,
            clientSideFailure = clientSideFailure,
            benignClientErrorProbe = HttpClientErrorException.create(HttpStatus.NOT_FOUND, "Not Found", HttpHeaders(), ByteArray(0), null),
        )
}
