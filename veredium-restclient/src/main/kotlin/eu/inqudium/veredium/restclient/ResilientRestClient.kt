package eu.inqudium.veredium.restclient

import eu.inqudium.legatium.restclient.logging.ClientRequestLoggingInterceptor
import eu.inqudium.veredium.core.client.ClientCallLog
import eu.inqudium.veredium.core.client.EmptyResponseBodyException
import eu.inqudium.veredium.core.client.ResponseBodyMetrics
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import eu.inqudium.veredium.core.resilience.ClientSideFailurePolicy
import eu.inqudium.veredium.core.validation.ResponseValidationProperties
import io.micrometer.core.instrument.DistributionSummary
import jakarta.validation.ConstraintViolationException
import jakarta.validation.Validator
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import org.springframework.core.log.LogFormatUtils
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatusCode
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.client.RestClient
import org.springframework.web.client.RestClientResponseException
import tools.jackson.databind.ObjectMapper
import java.nio.charset.Charset
import java.time.Duration
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * A resilience-wrapped HTTP caller for a single downstream backend, handed out by [ResilientRestClientFactory]
 * instead of a bare [RestClient]. It owns the per-call pipeline - the bounded body read, error surfacing, body
 * (de)serialisation and the R4j decoration ([BlockingResilienceDecorator]) - so a caller only describes the
 * request. [backendName] doubles as the Resilience4j instance name.
 *
 * [get] and [post] BLOCK the calling thread and return the decoded value or throw: there is no reactive type to
 * carry a signal, so a failure arrives the way a blocking contract promises it, as an exception out of the
 * call. The thread that waits is the caller's; the HTTP exchange itself runs on a worker the decorator hands it
 * to (see [BlockingResilienceDecorator]), which is invisible to a caller except in what cancellation means:
 * interrupting the caller abandons the call and is reported as a cancellation, not an error.
 *
 * ## The response contract
 *
 * A 2xx body is deserialised into the requested type; every other outcome is an exception, never `null`:
 * - a 4xx/5xx is Spring's status exception ([HttpClientErrorException] / [HttpServerErrorException], the
 *   status-specific subclass, with the truncated error body on it);
 * - a 1xx/3xx the engine did not resolve - none follows a redirect - is a plain [RestClientResponseException]
 *   carrying its status and body, which the breaker records as a failure;
 * - a 2xx WITHOUT a body - a 204, or a 200 with an empty entity - is an [EmptyResponseBodyException] naming the
 *   backend and the type that was expected, the same exception the reactive twin raises;
 * - a 2xx past the buffer limit is a [ResponseBodyLimitExceededException];
 * - a body that does not parse is Jackson's exception, a schema violation a [ConstraintViolationException]
 *   (when validation enforces it for this backend). A body that parses but lacks a property the DTO declares
 *   required and non-nullable, or carries it as an explicit `null`, fails too, never yields a partially populated
 *   object: Jackson's `DatabindException` names the property - nothing downstream guards a Kotlin non-null type;
 * - the resilience verdicts arrive as themselves: `TimeoutException`, `CallNotPermittedException`,
 *   `BulkheadFullException`; a transport fault as Spring's `ResourceAccessException`.
 *
 * The empty body, the parse failure and the violation are raised AFTER the Resilience4j decoration: the backend
 * answered, promptly and with a success status, so the circuit breaker records a success and never sees them.
 * Each still gets the client's error line at WARN with its stack trace, like any failure that is not a benign
 * 4xx.
 *
 * [getAsync] and [postAsync] are the same calls without the wait: they return a [CompletableFuture] at once and
 * run the same pipeline - the decorator's asynchronous form puts the wait on a second virtual thread, and the
 * deserialisation and validation run there too, once the exchange is done. Every outcome arrives on the
 * future, unwrapped (the exception the blocking form would have thrown, a serialisation failure included);
 * cancelling the future abandons the call exactly as interrupting a blocking caller does, and is reported the
 * same way. The two forms share the exchange, the verdicts and the log lines; they differ only in who waits.
 *
 * Only `cancel` reaches the call. `orTimeout` and `completeOnTimeout` on the returned future complete THAT future
 * (exceptionally, or with the fallback value) and nothing else, as on any `CompletableFuture`: the exchange runs
 * on until it ends or the TimeLimiter budget expires, holding its bulkhead permit and its pool connection all
 * the while. A caller that wants a deadline shorter than the budget cancels the future itself when it passes.
 *
 * The exchange itself is NOT logged here. legatium-restclient-logging hangs its interceptor on Boot's
 * `RestClient.Builder` bean, which the factory clones per backend, and writes one structured event per exchange
 * on the `adapter-http-exchange` logger (`adapter_name` = [backendName], via the request attribute the factory
 * sets). What this class logs is the transport detail underneath: the per-call DEBUG lines, the error line
 * with the exception class, the cancellation line and the buffer-limit line - each carrying `adapter_name` and
 * `adapter_url_template` under the library's own field names ([ClientCallLog]), so it joins the exchange event on sight.
 */
class ResilientRestClient(
    private val restClient: RestClient,
    private val backendName: String,
    private val endpoint: ResolvedTimeouts,
    private val resilience: BlockingResilienceDecorator,
    private val objectMapper: ObjectMapper,
    /**
     * The buffering limit for a response body, in bytes - `maxInMemorySize` as the factory resolved it for this
     * client. Enforced HERE, by the bounded read in [exchange], because the blocking client has no codec limit
     * of its own; and named on the line that reports a response exceeding it (the buffer-limit line of
     * [ClientCallLog]), so that line carries the number an operator has to raise.
     */
    private val maxInMemoryBytes: Int,
    /**
     * Bean Validation for this backend's responses, or `null` when it is switched off for it.
     *
     * Null-as-off rather than a boolean flag: the per-backend decision is resolved ONCE, by
     * [ResilientRestClientFactory] when it builds this client (see [ResponseValidationProperties]), so the
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
     * The number this exists to inform is [RestClientProperties.maxInMemorySize]: a ceiling that has to be
     * picked, with nothing to pick it from. A [ResponseBodyLimitExceededException] reports only that the limit
     * was passed and the wire size the peer declared, and legatium's body capture is bounded by its own
     * `max-body-bytes`, which says nothing about this limit.
     *
     * Sampled AFTER decompression, which is where the limit applies: every engine of this module inflates a
     * compressed body transparently, so what the bounded reader counts and what this meter records are the same
     * bytes.
     *
     * Recorded in UTF-8 BYTES ([ResponseBodyMetrics.utf8Size]) rather than `String.length`, so the sample and the
     * limit it informs are the same unit. For a UTF-8 body that is the byte count as read, taken without decoding
     * the body to a `String` first (see [decode]).
     *
     * Null-as-off, like [validator]: resolved once per backend by [ResilientRestClientFactory], so the hot path
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
    /** The endpoint's path template - the URI an adapter builds its calls on, and the low-cardinality tag of their metrics. */
    val pathTemplate: String get() = endpoint.pathTemplate

    /** The transport's rung of the ladder for this backend - what an adapter publishes as `BaseAdapter.responseTimeout`. */
    val responseTimeout: Duration get() = endpoint.responseTimeout

    /** The TimeLimiter budget of this backend - what an adapter publishes as `BaseAdapter.timeLimiterTimeout`. */
    val timeLimiterTimeout: Duration get() = resilience.requestBudget(backendName)
    private val logger: Logger = LoggerFactory.getLogger(ResilientRestClient::class.java)

    /** The transport lines, the size sample and the validation of a call - shared with the reactive client. */
    private val calls =
        ClientCallLog(
            logger = logger,
            backendName = backendName,
            pathTemplate = pathTemplate,
            propertiesPrefix = "app.restclient",
            responseTimeout = { responseTimeout },
            timeLimiterTimeout = { timeLimiterTimeout },
            verdicts = RestClientVerdicts.breakerVerdicts,
            validator = validator,
            failOnViolation = failOnViolation,
            responseBodySize = responseBodySize,
            bufferLimit =
                ClientCallLog.BufferLimitLine(
                    wording = "buffer limit",
                    maxInMemoryBytes = maxInMemoryBytes,
                    meterName = RestClientMetrics.RESPONSE_BODY_SIZE,
                    // Carried on this module's own exception, wherever in the cause chain it sits - within the
                    // core's bounded walk, as the verdicts read the same chain.
                    contentLengthOf = { error ->
                        ClientSideFailurePolicy
                            .causeChain(error)
                            .filterIsInstance<ResponseBodyLimitExceededException>()
                            .firstOrNull()
                            ?.contentLength ?: -1L
                    },
                ),
        )

    /**
     * Executes a GET and blocks for the result. [block] receives a fresh request spec and must apply the URI
     * (and any headers); the response body is deserialised into [responseType]. The failures are those of the
     * response contract (class KDoc).
     *
     * `@Throws` for a Java caller, which could otherwise not catch the two CHECKED exceptions by type.
     *
     * @throws TimeoutException when the TimeLimiter budget expired before the backend answered.
     * @throws InterruptedException when the calling thread was interrupted while waiting; the exchange is
     *   abandoned and the call is reported as a cancellation, not an error.
     */
    @Throws(TimeoutException::class, InterruptedException::class)
    fun <T : Any> get(
        responseType: Class<T>,
        block: (RestClient.RequestHeadersUriSpec<*>) -> RestClient.RequestHeadersSpec<*>,
    ): T =
        execute(
            // Stamp the endpoint's low-cardinality path template onto the request so legatium's exchange
            // event (adapter_url_template) and Spring's client observation can record the template next to
            // the expanded URL. Done centrally so it holds regardless of which uri(...) overload the caller
            // used - the builder-lambda form does not set the attribute, and for a caller that passed
            // uri(template, vars) re-setting it is a no-op (the same value). The key is the library's public
            // constant, the same string Spring's own uri(template, vars) sets.
            block(restClient.get()).attribute(ClientRequestLoggingInterceptor.URI_TEMPLATE_ATTRIBUTE, pathTemplate),
            responseType,
            requestBody = null,
        )

    /**
     * Executes a POST of [body] and blocks for the result. [block] applies the URI and content type and returns
     * the body spec. The body is serialised once and handed to the wire as a string.
     *
     * The serialisation runs BEFORE the exchange, deliberately outside [execute], so a serialisation failure
     * never becomes an HTTP exchange: there is no method, no path, no status, nothing on the wire - and
     * therefore no exchange event either, because legatium's interceptor only sees requests that reach the
     * RestClient. A log whose consumers count real calls gets no phantom exchange. (The response side is
     * deliberately the other way round - a deserialisation failure happens AFTER the exchange, which the
     * library logs as the 200 it was.) Either way this is separate from the Resilience4j decoration, which
     * neither failure may reach - see [BlockingResilienceDecorator].
     *
     * In a blocking contract the failure simply throws out of this method; the reactive twin has to route it
     * into the returned signal, and the difference is one of calling convention, not of behaviour.
     *
     * @throws TimeoutException when the TimeLimiter budget expired before the backend answered.
     * @throws InterruptedException when the calling thread was interrupted while waiting, as for [get].
     */
    @Throws(TimeoutException::class, InterruptedException::class)
    fun <T : Any> post(
        responseType: Class<T>,
        body: Any,
        block: (RestClient.RequestBodyUriSpec) -> RestClient.RequestBodySpec,
    ): T {
        val serialized = objectMapper.writeValueAsString(body)
        // Hand the JSON string straight to body(): the String converter writes it as UTF-8 under the content
        // type the caller set, so an explicit toByteArray() would only allocate a redundant array.
        // .attribute stamps the path template centrally (see get()): a caller that builds the URI with the
        // uri { builder -> ... } overload does NOT set the template attribute, so without this the exchange
        // event and the observation would have no template for its calls.
        val spec =
            block(restClient.post())
                .body(serialized)
                .attribute(ClientRequestLoggingInterceptor.URI_TEMPLATE_ATTRIBUTE, pathTemplate)
        return execute(spec, responseType, requestBody = serialized)
    }

    /**
     * Executes a GET without blocking: [get], with the result delivered on the returned future (class KDoc).
     */
    fun <T : Any> getAsync(
        responseType: Class<T>,
        block: (RestClient.RequestHeadersUriSpec<*>) -> RestClient.RequestHeadersSpec<*>,
    ): CompletableFuture<T> =
        executeAsync(
            block(restClient.get()).attribute(ClientRequestLoggingInterceptor.URI_TEMPLATE_ATTRIBUTE, pathTemplate),
            responseType,
            requestBody = null,
        )

    /**
     * Executes a POST of [body] without blocking: [post], with the result delivered on the returned future.
     *
     * The serialisation still runs BEFORE the exchange and outside the decoration, for the reasons [post] gives;
     * what differs is where its failure goes. A blocking caller gets it thrown; an asynchronous caller reads
     * every outcome off the future, so the failure is delivered there as a failed future - no exchange, no
     * event, no worker started.
     */
    fun <T : Any> postAsync(
        responseType: Class<T>,
        body: Any,
        block: (RestClient.RequestBodyUriSpec) -> RestClient.RequestBodySpec,
    ): CompletableFuture<T> {
        val serialized =
            try {
                objectMapper.writeValueAsString(body)
            } catch (e: Exception) {
                return CompletableFuture.failedFuture(e)
            }
        val spec =
            block(restClient.post())
                .body(serialized)
                .attribute(ClientRequestLoggingInterceptor.URI_TEMPLATE_ATTRIBUTE, pathTemplate)
        return executeAsync(spec, responseType, requestBody = serialized)
    }

    /**
     * [execute] without the wait. The decorator's asynchronous form runs the exchange and hands back a future
     * that completes on the waiter thread - under the caller's MDC, which the decorator carried there - and
     * that is where the rest of [execute] runs: the size sample, deserialisation, validation and the log lines,
     * in the same order and with the same verdicts as the blocking form. The future handed to the caller
     * cancels THROUGH to the decorator's, so a caller's `cancel` abandons the exchange (worker interrupted,
     * permit released) rather than merely detaching from its result.
     *
     * Every branch of the continuation completes the caller's future, whatever arrives. A cancellation the
     * CALLER did not ask for - the call itself ending in a `CancellationException` - would otherwise leave it
     * pending forever, since only the caller's own `cancel` completes it on the way in.
     */
    private fun <T : Any> executeAsync(
        spec: RestClient.RequestHeadersSpec<*>,
        responseType: Class<T>,
        requestBody: String?,
    ): CompletableFuture<T> {
        calls.logExecuting(responseType, requestBody)
        val startNanos = nanoTime()
        val elapsedMillis = { TimeUnit.NANOSECONDS.toMillis(nanoTime() - startNanos) }

        val exchange: CompletableFuture<RawBody> = resilience.decorateAsync(backendName) { exchange(spec) }
        val result = CancelThroughFuture<T>(exchange)
        exchange.whenComplete { rawBody, failure ->
            var outcome = "success"
            try {
                when {
                    // The caller cancelled the future handed out below: the decorator's future is cancelled
                    // through it, and that is the signal that arrives here. The blocking form's InterruptedException.
                    // `result` is cancelled already - super.cancel ran before the cancel reached upstream.
                    failure is CancellationException && result.isCancelled -> {
                        outcome = "cancelled"
                        calls.onCancel(responseType, elapsedMillis)
                    }

                    // Any other failure - a CancellationException the caller did not ask for included, which is
                    // an anomaly of the call, not the caller going away - is delivered as itself.
                    // completeExceptionally is a no-op on a future that is already done, so this cannot race a
                    // cancel arriving now.
                    failure != null -> {
                        outcome = "error"
                        calls.onError(responseType, failure, elapsedMillis)
                        result.completeExceptionally(failure)
                    }

                    else -> {
                        try {
                            result.complete(decode(rawBody, responseType))
                        } catch (e: Throwable) {
                            outcome = "error"
                            calls.onError(responseType, e, elapsedMillis)
                            result.completeExceptionally(e)
                        }
                    }
                }
            } finally {
                calls.log(Level.DEBUG, "Completed request to backend '{}' with outcome={}", backendName, outcome)
            }
        }
        return result
    }

    /**
     * The future [executeAsync] hands out: completed by the exchange's continuation, and cancelling it cancels
     * the decorator's future underneath - which interrupts the worker. Derived stages are plain futures, as
     * everywhere else; only the one the caller got from the client reaches the call.
     */
    private class CancelThroughFuture<T>(
        private val upstream: Future<*>,
    ) : CompletableFuture<T>() {
        override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
            val cancelled = super.cancel(mayInterruptIfRunning)
            upstream.cancel(true)
            return cancelled
        }

        override fun <U> newIncompleteFuture(): CompletableFuture<U> = CompletableFuture()
    }

    private fun <T : Any> execute(
        spec: RestClient.RequestHeadersSpec<*>,
        responseType: Class<T>,
        requestBody: String?,
    ): T {
        calls.logExecuting(responseType, requestBody)

        // The attempt's own clock. The error and cancel lines below promise a number regardless of any logging
        // configuration, so it is measured here and nowhere else.
        val startNanos = nanoTime()
        val elapsedMillis = { TimeUnit.NANOSECONDS.toMillis(nanoTime() - startNanos) }
        var outcome = "success"
        try {
            // The decorated call produces only the raw body - all the CircuitBreaker, TimeLimiter and Bulkhead
            // observe. Deserialisation is deliberately kept outside the decoration (see decode).
            val rawBody = resilience.decorate(backendName) { exchange(spec) }
            return decode(rawBody, responseType)
        } catch (e: InterruptedException) {
            // The caller was interrupted while waiting - its own budget expired, or its scope was cancelled.
            // The blocking counterpart of a subscriber cancelling, and reported the same way: not an error.
            outcome = "cancelled"
            calls.onCancel(responseType, elapsedMillis)
            throw e
        } catch (e: Throwable) {
            // Everything else carries the resilience verdict as well as the raw HTTP outcome: a TimeLimiter
            // timeout, CircuitBreaker short-circuit, BulkheadFull, or an empty body, a deserialisation failure or a
            // violation from decode all surface here. (The exchange event legatium writes sees only what reached
            // the RestClient: a breaker short-circuit or a full bulkhead produces no exchange and no event - this
            // line is where those verdicts are logged.)
            outcome = "error"
            calls.onError(responseType, e, elapsedMillis)
            throw e
        } finally {
            calls.log(Level.DEBUG, "Completed request to backend '{}' with outcome={}", backendName, outcome)
        }
    }

    /**
     * A success body as the exchange read it: the bytes, bounded by the buffer limit, and the charset the
     * response declared. Decoded only by [decode], outside the decoration.
     */
    private class RawBody(
        val bytes: ByteArray,
        val charset: Charset,
    )

    /**
     * The part of a call that runs AFTER the decoration, in both forms: the size sample, the empty-body check,
     * deserialisation and validation. Outside on purpose - an empty body, a parse failure and a schema violation
     * are each the backend ANSWERING, promptly and with a success status, so the CircuitBreaker must not see
     * them as failures and open the circuit while the backend is healthy (class KDoc, "The response contract").
     *
     * A UTF-8 body (JSON's default, and what a response without a charset is read as) is handed to Jackson as
     * the bytes it arrived in: no `String` copy of a body up to the buffer limit in size, and the size sample
     * is simply the byte count - the unit [ResponseBodyMetrics.utf8Size] measures, without the walk over a
     * decoded text. A body in any other declared charset is decoded with it first and sampled from the text, so
     * the meter still reports UTF-8 bytes for every body.
     */
    private fun <T : Any> decode(
        body: RawBody,
        responseType: Class<T>,
    ): T {
        val value =
            if (body.charset == Charsets.UTF_8) {
                responseBodySize?.record(body.bytes.size.toDouble())
                if (body.bytes.isEmpty()) throw EmptyResponseBodyException(backendName, responseType)
                objectMapper.readValue(body.bytes, responseType)
            } else {
                val text = String(body.bytes, body.charset)
                calls.recordBodySize(text)
                if (text.isEmpty()) throw EmptyResponseBodyException(backendName, responseType)
                objectMapper.readValue(text, responseType)
            }
        return calls.validate(value)
    }

    /**
     * The HTTP exchange proper, run by the decorator on its worker: sends the request, reads the body under the
     * buffer limit and surfaces a 4xx/5xx as the exception Spring's own default handler would raise, a 1xx/3xx
     * as a plain [RestClientResponseException].
     *
     * `exchange` rather than `retrieve()` because the bounded read needs the raw stream, and `exchange` is the one
     * API that hands it over. It also hands over the status decision - `exchange` applies no status handlers -
     * which is taken here the way Spring's `DefaultResponseErrorHandler` takes it: a 4xx becomes an
     * [HttpClientErrorException], a 5xx an [HttpServerErrorException], each carrying status, headers and the
     * (truncated) error body, so a caller or a log sees the same exception types a plain `retrieve()` throws.
     * Only a 2xx is a body to return. A 1xx or 3xx the transport did not resolve - no engine follows a redirect -
     * is never the answer the caller asked for, so it fails as a [RestClientResponseException] carrying its status,
     * which the breaker records like any status that is not a benign 4xx.
     *
     * The response is closed by `RestClient` when this function returns (`close = true`), which is also the
     * moment legatium emits its exchange event - on this worker thread, under the caller's MDC that the
     * decorator installed there.
     */
    private fun exchange(spec: RestClient.RequestHeadersSpec<*>): RawBody =
        spec.exchange<RawBody>({ _, response ->
            val status = response.statusCode
            val headers = response.headers
            // The declared length sizes the reader's buffer up front (BoundedBodyReader, "Memory"); it is never
            // trusted as the length of what follows.
            val contentLength = headers.contentLength
            if (!status.is2xxSuccessful) {
                // A non-2xx body is diagnostic only: cut it at the limit rather than reject it, so a 5xx stays a
                // 5xx for the breaker whatever size its error page has.
                val body = BoundedBodyReader.readTruncated(response.body, maxInMemoryBytes, contentLength)
                throw statusException(status, response.statusText, headers, body)
            }
            val body =
                BoundedBodyReader.readOrNull(response.body, maxInMemoryBytes, contentLength)
                    ?: throw ResponseBodyLimitExceededException(backendName, maxInMemoryBytes, contentLength)
            RawBody(body, charsetOf(headers))
        }, true)

    /**
     * The exception for a non-2xx answer, shaped like Spring's own: the concrete subclass for a 4xx/5xx status
     * (`HttpClientErrorException.NotFound`, `HttpServerErrorException.InternalServerError`, ...), the plain
     * [RestClientResponseException] for a 1xx/3xx, each with the full body attached and a message that previews
     * it. The preview is capped because this message ends up in the WARN line's stack trace; the whole body stays
     * on the exception for whoever asks for it.
     */
    private fun statusException(
        status: HttpStatusCode,
        statusText: String,
        headers: HttpHeaders,
        body: ByteArray,
    ): RestClientResponseException {
        val charset = charsetOf(headers)
        val preview =
            if (body.isEmpty()) "[no body]" else LogFormatUtils.formatValue(String(body, charset), ERROR_BODY_PREVIEW_CHARS, true)
        val message = "${status.value()} $statusText: $preview"
        return when {
            status.is4xxClientError -> HttpClientErrorException.create(message, status, statusText, headers, body, charset)
            status.is5xxServerError -> HttpServerErrorException.create(message, status, statusText, headers, body, charset)
            else -> RestClientResponseException(message, status, statusText, headers, body, charset)
        }
    }

    /** The response's declared charset, UTF-8 when it declares none or declares nonsense. */
    private fun charsetOf(headers: HttpHeaders): Charset = runCatching { headers.contentType?.charset }.getOrNull() ?: Charsets.UTF_8

    private companion object {
        /** How much of an error body the exception MESSAGE shows; the exception itself carries all of it. */
        const val ERROR_BODY_PREVIEW_CHARS = 200
    }
}
