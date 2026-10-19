package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.mesh.MeshTimeoutConfig
import eu.inqudium.veredium.core.resilience.BreakerVerdicts
import eu.inqudium.veredium.core.resilience.ClientSideFailurePolicy
import eu.inqudium.veredium.core.resilience.HttpExchangeErrorPolicy
import io.github.resilience4j.bulkhead.BulkheadFullException
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.RestClientResponseException
import java.util.concurrent.RejectedExecutionException

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
     *   error signal. Here it does: [BlockingResilienceDecorator] rethrows the `InterruptedException` out of the
     *   decorated call and `CircuitBreaker.decorateCallable` records every `Exception`, so without this entry
     *   a host that cancels callers routinely would open the circuit against a backend that answered nothing
     *   wrong - it was not even given the time to answer.
     * - [RejectedExecutionException]: the decorator's worker executor refused the submission because the
     *   decorator is already shut down - a call made during context teardown. [BlockingResilienceDecorator]
     *   submits the worker INSIDE the callable the breaker wraps (so that a short-circuited call submits
     *   nothing), which puts this refusal on the breaker's path like any other exception. The backend was
     *   never called.
     *
     * The cause walk matters on this stack because Spring's blocking client wraps every `IOException` of the
     * exchange into a `ResourceAccessException`; the limit exception is thrown bare today, but the predicate
     * must keep matching if a wrapper ever gets in between. A 5xx whose error body exceeds the buffer limit
     * yields a plain `HttpServerErrorException` - the reader TRUNCATES an error body instead of rejecting it -
     * so it is still recorded; `ClientSideFailureCustomizerTest` pins exactly that.
     */
    val clientSideFailure: ClientSideFailurePolicy =
        ClientSideFailurePolicy(
            clientSideFailures =
                listOf(
                    BulkheadFullException::class.java,
                    ResponseBodyLimitExceededException::class.java,
                    InterruptedException::class.java,
                    RejectedExecutionException::class.java,
                ),
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

/**
 * Hands [RestClientVerdicts] to the shared mesh wiring and pulls that wiring into this module's context: the
 * `app.mesh` binding, the per-alias Resilience4j customizers, the startup guard and the ladder logger
 * ([MeshTimeoutConfig]). A host that scans this module's package therefore gets the core wiring too, without
 * knowing the core package exists.
 */
@Configuration(proxyBeanMethods = false)
@Import(MeshTimeoutConfig::class)
class RestClientVerdictsConfig {
    @Bean
    fun breakerVerdicts(): BreakerVerdicts = RestClientVerdicts.breakerVerdicts
}
