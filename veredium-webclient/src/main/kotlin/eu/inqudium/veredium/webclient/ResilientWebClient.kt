package eu.inqudium.veredium.webclient

import eu.inqudium.legatium.webclient.logging.ClientRequestLoggingFilter
import eu.inqudium.veredium.core.client.ClientCallLog
import eu.inqudium.veredium.core.client.EmptyResponseBodyException
import eu.inqudium.veredium.core.client.ResponseBodyMetrics
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import eu.inqudium.veredium.core.validation.ResponseValidationProperties
import io.micrometer.core.instrument.DistributionSummary
import jakarta.validation.ConstraintViolationException
import jakarta.validation.Validator
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.slf4j.event.Level
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * A resilience-wrapped HTTP caller for a single downstream backend, handed out by [ResilientWebClientFactory]
 * instead of a bare [WebClient]. It owns the per-call pipeline - error surfacing, body (de)serialisation,
 * the R4j decoration ([ReactiveResilienceDecorator]) and the reactive->coroutine bridge - so a caller only
 * describes the request. [backendName] doubles as the Resilience4j instance name.
 *
 * The exchange itself is NOT logged here. legatium-webclient-logging hangs its filter on Boot's
 * `WebClient.Builder` bean, which the factory clones per backend, and writes one structured event per exchange
 * on the `adapter-http-exchange` logger (`adapter_name` = [backendName], via the request attribute the factory
 * sets). What this class logs is the transport detail underneath: the per-call DEBUG lines, the error line
 * with the exception class, the cancellation line and the buffer-limit line - each carrying `adapter_name` and
 * `adapter_url_template` under the library's own field names ([ClientCallLog]), so it joins the exchange event on sight.
 *
 * ## The response contract
 *
 * The returned `Mono<T>` emits exactly one value or an error - never completes empty. A 2xx answer without a
 * body (a 204, or a 200 with an empty entity) fails with [EmptyResponseBodyException] naming the backend and the
 * expected type; a non-2xx answer fails with the `WebClientResponseException` `retrieve()` raises. Until
 * 2026-10-21 an empty body completed the `Mono` empty instead: no error line, a success for the breaker, and a
 * `NoSuchElementException` out of the caller's `awaitSingle()` that named nothing - the blocking twin failed
 * the same answer with a parse error, so the two stacks disagreed. The failure is raised AFTER the
 * Resilience4j decoration, like a deserialisation failure: the backend answered promptly with a success status,
 * so the breaker records a success, and the error line reports the anomaly at WARN.
 *
 * ## The caller's MDC on the transport lines
 *
 * The error, cancellation and closing lines are written in signal hooks that run on a Netty event loop, which
 * carries none of the caller's MDC. Rather than depend on the host having enabled Reactor's automatic context
 * propagation AND registered an MDC accessor, each subscription snapshots the MDC on the SUBSCRIBING thread -
 * where a coroutine's `MDCContext` or a servlet filter put the correlation keys - and writes those lines under
 * it ([ClientCallLog.withMdc]), leaving the event loop's own MDC as it found it.
 */
class ResilientWebClient(
    private val webClient: WebClient,
    private val backendName: String,
    private val endpoint: ResolvedTimeouts,
    private val resilience: ReactiveResilienceDecorator,
    private val objectMapper: ObjectMapper,
    /**
     * The codec's buffering limit for a response body, in bytes - `maxInMemorySize` as the factory set it on
     * this client's codecs. Held here for ONE purpose: naming the limit on the line that reports a response
     * exceeding it (the buffer-limit line of [ClientCallLog]), so that line carries the number an operator has to raise.
     */
    private val maxInMemoryBytes: Int,
    /**
     * Bean Validation for this backend's responses, or `null` when it is switched off for it.
     *
     * Null-as-off rather than a boolean flag: the per-backend decision is resolved ONCE, by
     * [ResilientWebClientFactory] when it builds this client (see [ResponseValidationProperties]), so the
     * hot path holds no toggle to consult and cannot disagree with the configuration mid-flight.
     */
    private val validator: Validator? = null,
    /**
     * Whether a detected violation FAILS the call or is only REPORTED. `true` (the default) throws a
     * [ConstraintViolationException]; `false` logs the violation at WARN and returns the response unchanged, so
     * a backend can be watched against real traffic before its violations are made fatal. Irrelevant when
     * [validator] is null (validation off for this backend). Shared, not per-backend - see
     * [ResponseValidationProperties.failOnViolation].
     */
    private val failOnViolation: Boolean = true,
    /**
     * Records the size of each decoded response body, or `null` when the host wired no `MeterRegistry`.
     *
     * The number this exists to inform is [WebClientProperties.maxInMemorySize]: a ceiling that has to be
     * picked, with nothing to pick it from. A `DataBufferLimitException` reports only that the limit was
     * passed, not by how much, and legatium's body capture is bounded by its own `max-body-bytes`, which says
     * nothing about the codec's.
     *
     * Sampled here rather than at the transport: Reactor-Netty's `data.received` counts bytes off the wire,
     * which with `compress(true)` is the COMPRESSED size, while the codec limit bounds the decompressed
     * buffer. Sizing the limit from that would under-set it by the compression ratio.
     *
     * Recorded in UTF-8 BYTES ([ResponseBodyMetrics.utf8Size]) rather than `String.length`, so the sample and the
     * limit it informs are the same unit.
     *
     * Null-as-off, like [validator]: resolved once per backend by [ResilientWebClientFactory], so the hot path
     * holds no registry lookup.
     */
    private val responseBodySize: DistributionSummary? = null,
    /**
     * Monotonic time source for the per-attempt duration, injected so a test can assert the elapsed time
     * exactly instead of observing it. Defaults to `System::nanoTime`, so production wiring is unchanged.
     * Monotonic on purpose - this measures a duration, never a timestamp.
     */
    private val nanoTime: () -> Long = System::nanoTime,
) {
    val pathTemplate: String get() = endpoint.pathTemplate
    val responseTimeout: Duration get() = endpoint.responseTimeout
    val timeLimiterTimeout: Duration get() = resilience.requestBudget(backendName)
    private val logger: Logger = LoggerFactory.getLogger(ResilientWebClient::class.java)

    /** The transport lines, the size sample and the validation of a call - shared with the blocking client. */
    private val calls =
        ClientCallLog(
            logger = logger,
            backendName = backendName,
            pathTemplate = pathTemplate,
            propertiesPrefix = "app.webclient",
            responseTimeout = { responseTimeout },
            timeLimiterTimeout = { timeLimiterTimeout },
            verdicts = WebClientVerdicts.breakerVerdicts,
            validator = validator,
            failOnViolation = failOnViolation,
            responseBodySize = responseBodySize,
            bufferLimit =
                ClientCallLog.BufferLimitLine(
                    wording = "codec buffer",
                    maxInMemoryBytes = maxInMemoryBytes,
                    meterName = WebClientMetrics.RESPONSE_BODY_SIZE,
                    // Read off the 200-reporting response exception Spring wraps the codec failure in.
                    contentLengthOf = { error -> (error as? WebClientResponseException)?.headers?.contentLength ?: -1L },
                ),
        )

    /**
     * Executes a GET. [block] receives a fresh request spec and must apply the URI (and any headers);
     * the response body is deserialised into [responseType].
     */
    fun <T : Any> get(
        responseType: Class<T>,
        block: (WebClient.RequestHeadersUriSpec<*>) -> WebClient.RequestHeadersSpec<*>,
    ): Mono<T> =
        execute(
            // Stamp the endpoint's low-cardinality path template onto the request so legatium's exchange
            // event (adapter_url_template) and Spring's client observation can record the template next to
            // the expanded URL. Done centrally so it holds regardless of which uri(...) overload the adapter
            // used - the builder-lambda form (POST adapters) does not set the attribute, and re-setting it for
            // the GET adapters is a no-op (same value they already passed via uri(template, vars)). The key
            // is the library's public constant, the same string Spring's own uri(template, vars) sets.
            block(webClient.get()).attribute(ClientRequestLoggingFilter.URI_TEMPLATE_ATTRIBUTE, pathTemplate),
            responseType,
            requestBody = null,
        )

    /**
     * Executes a POST of [body]. [block] applies the URI and content type and returns the body spec. The body
     * is serialised once per subscription and handed to the wire as a string.
     *
     * The serialisation runs INSIDE the returned Mono (`fromCallable`), not while assembling it. A method
     * returning `Mono<T>` promises to report failures as an error SIGNAL; serialising eagerly would instead
     * throw out of `post()` itself, before any Mono exists to carry the error - invisible to the coroutine
     * adapters, which subscribe immediately, but a broken contract for any caller that composes the Mono and
     * subscribes later.
     *
     * It sits OUTSIDE [execute] on purpose, so a serialisation failure never becomes an HTTP exchange: there
     * is no method, no path, no status, nothing on the wire - and therefore no exchange event either, because
     * legatium's filter only sees requests that reach the WebClient. A log whose consumers count real calls
     * gets no phantom exchange. (The response side is deliberately the other way round - a deserialisation
     * failure happens AFTER the exchange, which the library logs as the 200 it was.) Either way this is
     * separate from the Resilience4j decoration, which neither failure may reach - see
     * [ReactiveResilienceDecorator].
     */
    fun <T : Any> post(
        responseType: Class<T>,
        body: Any,
        block: (WebClient.RequestBodyUriSpec) -> WebClient.RequestBodySpec,
    ): Mono<T> =
        Mono
            .fromCallable { objectMapper.writeValueAsString(body) }
            .flatMap { serialized ->
                // Hand the JSON string straight to bodyValue(): WebClient encodes it as UTF-8, so an explicit
                // toByteArray() would only allocate a redundant array.
                // .attribute stamps the path template centrally (see get()): the POST adapters build the URI via
                // the uri { builder -> ... } overload, which does NOT set the template attribute, so without this
                // the exchange event and the observation would have no template for these calls.
                val spec =
                    block(webClient.post())
                        .bodyValue(serialized)
                        .attribute(ClientRequestLoggingFilter.URI_TEMPLATE_ATTRIBUTE, pathTemplate)
                execute(spec, responseType, requestBody = serialized)
            }

    private fun <T : Any> execute(
        spec: WebClient.RequestHeadersSpec<*>,
        responseType: Class<T>,
        requestBody: String?,
    ): Mono<T> {
        // Everything that owns per-request state - the attempt's startNanos clock - plus the HTTP exchange
        // itself lives INSIDE this defer, so it is materialised per SUBSCRIPTION, not per assembly. This method
        // only assembles and returns the cold Mono; subscription is the caller's job (the adapters do it via
        // .awaitSingle()). The defer is what keeps that correct: each subscription gets its OWN startNanos (so
        // the duration measures the attempt, not the time the Mono was held before a late subscription) and
        // its OWN exchange - so a caller that subscribes more than once (e.g. a .retry() on the returned Mono)
        // never shares state across subscriptions.
        val exchange =
            Mono.defer {
                // The caller's MDC, taken HERE because this is the subscribing thread - the last point at which
                // the caller's correlation keys are reliably on the thread. Every hook below fires on an event
                // loop and writes its line under this snapshot (class KDoc). Per subscription, like the clock.
                val callerMdc: Map<String, String>? = MDC.getCopyOfContextMap()

                // Per subscription, i.e. on every outbound call - guarded inside, see ClientCallLog. Runs on the
                // subscribing thread itself, so it carries the caller's MDC without help.
                calls.logExecuting(responseType, requestBody)

                // The attempt's own clock, started per SUBSCRIPTION like everything else in this defer. The
                // error and cancel lines below promise a number regardless of any logging configuration, so
                // it is measured here and nowhere else.
                val startNanos = nanoTime()
                val elapsedMillis = { TimeUnit.NANOSECONDS.toMillis(nanoTime() - startNanos) }

                // The source produces only the raw JSON string - all the CircuitBreaker, TimeLimiter and
                // Bulkhead observe. Deserialisation is deliberately kept off this inner mono (see below).
                val rawMono = spec.retrieve().bodyToMono(String::class.java)

                // Deserialisation runs AFTER the decoration: a parse failure is a client-side error, so the
                // CircuitBreaker must not see it as a failure and open the circuit while the backend is healthy.
                // Validation sits in the SAME map for the same reason - a response that violates its schema is a
                // contract problem, not an outage, and must not open the circuit either.
                resilience
                    .decorate(backendName, rawMono)
                    // An empty 2xx body (204, empty 200) completes bodyToMono EMPTY. Turned into a named error
                    // here - after the decoration, so the breaker has already recorded the success the backend's
                    // answer was, and before the hooks below, so the error line reports it (class KDoc).
                    .switchIfEmpty(Mono.error { EmptyResponseBodyException(backendName, responseType) })
                    .map { rawBody ->
                        // The report-only validation line is written here, on the event loop, too.
                        ClientCallLog.withMdc(callerMdc) {
                            calls.recordBodySize(rawBody)
                            calls.validate(objectMapper.readValue(rawBody, responseType))
                        }
                    }
                    // doOnError/doOnCancel/doFinally sit OUTSIDE the decoration so the transport lines also
                    // carry the resilience verdict: a TimeLimiter timeout, CircuitBreaker short-circuit,
                    // BulkheadFull, or a deserialisation failure from the map above all surface here, not just
                    // the raw HTTP outcome. (The exchange event legatium writes sees only what reached the
                    // WebClient: a breaker short-circuit or a full bulkhead produces no exchange and no event -
                    // these lines are where those verdicts are logged.)
                    .doOnCancel {
                        // A cancelled scope (caller timeout, client disconnect) tears the stream down without
                        // doOnError, so this is the only hook that can report it.
                        ClientCallLog.withMdc(callerMdc) { calls.onCancel(responseType, elapsedMillis) }
                    }.doOnError { error ->
                        ClientCallLog.withMdc(callerMdc) { calls.onError(responseType, error, elapsedMillis) }
                    }.doFinally { signalType ->
                        ClientCallLog.withMdc(callerMdc) {
                            calls.log(Level.DEBUG, "On signalType={} for backend '{}'", signalType, backendName)
                        }
                    }
            }

        return exchange.contextCapture()
    }
}
