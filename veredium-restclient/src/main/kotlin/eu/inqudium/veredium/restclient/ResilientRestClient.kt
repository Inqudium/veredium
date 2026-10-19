package eu.inqudium.veredium.restclient

import eu.inqudium.legatium.restclient.logging.ClientRequestLoggingInterceptor
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
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
import java.util.concurrent.TimeUnit

/**
 * The two effective per-call timeouts an adapter exposes to its callers, so upstream orchestration can
 * compose an end-to-end budget from the individual backend budgets (e.g. a resolver summing the timeouts
 * along its call chain) without reaching into the adapter's [ResilientRestClient]s.
 *
 * If an adapter fronts more than one mesh endpoint (one [ResilientRestClient] per operation), then an
 * implementation can aggregate the per-client values - typically the `maxOf` across its clients, the
 * pessimistic ceiling for a single logical call.
 *
 * The two values are layered, not alternatives: the TimeLimiter is the outer, hard deadline after which the
 * CALLER is released, while the transport response timeout is a slightly tighter bound underneath it on the
 * response phase alone (see the ordering in [BlockingResilienceDecorator]). Keeping the response timeout below
 * the TimeLimiter budget ensures the transport is what normally trips, so the worker thread is released too;
 * the TimeLimiter covers the phases before the response (connect, pending-acquire) that the response timeout
 * does not.
 */
interface BaseAdapter {
    /**
     * The Resilience4j TimeLimiter budget for the call - the outer, hard deadline enforced by the blocking
     * TimeLimiter, after which the caller is released with a `TimeoutException`.
     *
     * ## What an adapter call can fail with, beyond the documented non-2xx
     *
     * A 200 whose body does not fit the declared DTO fails the call rather than yielding a partially populated
     * object. A property declared required and non-nullable that is missing is rejected by Jackson; one that is
     * explicitly `null` is rejected by the null check the Kotlin compiler puts in the constructor. Both surface
     * as a `tools.jackson.databind.DatabindException` naming the property. This matters to a caller because
     * Kotlin's types promise the field cannot be null - nothing downstream guards against it, so a silent null
     * would only fail much later.
     *
     * Such a failure is CLIENT-side: the backend answered, and with a 200. Like a parse error it is therefore
     * kept out of the Resilience4j decoration and does not count towards the circuit breaker (see
     * [BlockingResilienceDecorator]). It is logged with full severity; the exchange line legatium writes for
     * the call shows the 200 the backend answered with, because the exchange itself succeeded.
     */
    val timeLimiterTimeout: Duration

    /**
     * The transport's response timeout - the bound on waiting for the HTTP response, sitting just below
     * [timeLimiterTimeout].
     */
    val responseTimeout: Duration
}

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
 * The exchange itself is NOT logged here. legatium-restclient-logging hangs its interceptor on Boot's
 * `RestClient.Builder` bean, which the factory clones per backend, and writes one structured event per exchange
 * on the `adapter-http-exchange` logger (`adapter_name` = [backendName], via the request attribute the factory
 * sets). What this class logs is the transport detail underneath: the per-call DEBUG lines, the error line
 * with the exception class, the cancellation line and the buffer-limit line - each carrying `adapter_name` and
 * `adapter_url_template` under the library's own field names ([log]), so it joins the exchange event on sight.
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
     * of its own; and named on the line that reports a response exceeding it ([logIfBufferLimitExceeded]), so
     * that line carries the number an operator has to raise.
     */
    private val maxInMemoryBytes: Int,
    /**
     * Bean Validation for this backend's responses, or `null` when it is switched off for it.
     *
     * Null-as-off rather than a boolean flag: the per-backend decision is resolved ONCE, by
     * [ResilientRestClientFactory] when it builds this client (see [RestClientProperties.Validation]), so the
     * hot path holds no toggle to consult and cannot disagree with the configuration mid-flight.
     */
    private val validator: Validator? = null,
    /**
     * Whether a detected violation FAILS the call or is only REPORTED. `true` (the default) throws a
     * [ConstraintViolationException]; `false` logs the violation at WARN and returns the response unchanged, so
     * a backend can be watched against real traffic before its violations are made fatal. Irrelevant when
     * [validator] is null (validation off for this backend). Shared, not per-backend - see
     * [RestClientProperties.Validation.failOnViolation].
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
     * Sampled AFTER decompression, which is where the limit applies: Apache HttpClient inflates a compressed
     * body transparently, so what the bounded reader counts and what this meter records are the same bytes.
     *
     * Recorded in UTF-8 BYTES ([RestClientMetrics.utf8Size]) rather than `String.length`, so the sample and the
     * limit it informs are the same unit.
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
    val pathTemplate: String get() = endpoint.pathTemplate
    val responseTimeout: Duration get() = endpoint.responseTimeout
    val timeLimiterTimeout: Duration get() = resilience.requestBudget(backendName)
    private val logger: Logger = LoggerFactory.getLogger(ResilientRestClient::class.java)

    /**
     * Executes a GET and blocks for the result. [block] receives a fresh request spec and must apply the URI
     * (and any headers); the response body is deserialised into [responseType].
     */
    fun <T : Any> get(
        responseType: Class<T>,
        block: (RestClient.RequestHeadersUriSpec<*>) -> RestClient.RequestHeadersSpec<*>,
    ): T =
        execute(
            // Stamp the endpoint's low-cardinality path template onto the request so legatium's exchange
            // event (adapter_url_template) and Spring's client observation can record the template next to
            // the expanded URL. Done centrally so it holds regardless of which uri(...) overload the adapter
            // used - the builder-lambda form (POST adapters) does not set the attribute, and re-setting it for
            // the GET adapters is a no-op (same value they already passed via uri(template, vars)). The key
            // is the library's public constant, the same string Spring's own uri(template, vars) sets.
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
     */
    fun <T : Any> post(
        responseType: Class<T>,
        body: Any,
        block: (RestClient.RequestBodyUriSpec) -> RestClient.RequestBodySpec,
    ): T {
        val serialized = objectMapper.writeValueAsString(body)
        // Hand the JSON string straight to body(): the String converter writes it as UTF-8 under the content
        // type the adapter set, so an explicit toByteArray() would only allocate a redundant array.
        // .attribute stamps the path template centrally (see get()): the POST adapters build the URI via
        // the uri { builder -> ... } overload, which does NOT set the template attribute, so without this
        // the exchange event and the observation would have no template for these calls.
        val spec =
            block(restClient.post())
                .body(serialized)
                .attribute(ClientRequestLoggingInterceptor.URI_TEMPLATE_ATTRIBUTE, pathTemplate)
        return execute(spec, responseType, requestBody = serialized)
    }

    private fun <T : Any> execute(
        spec: RestClient.RequestHeadersSpec<*>,
        responseType: Class<T>,
        requestBody: String?,
    ): T {
        // Guarded AND parameterised: this runs on every outbound call. A Kotlin string template is built before
        // the call regardless of the level, and two of the arguments are not free - timeLimiterTimeout resolves
        // through the R4j registry, and take(100) allocates - so the guard is what keeps them off the hot path,
        // not just the formatting.
        if (logger.isDebugEnabled) {
            log(
                Level.DEBUG,
                "Executing request to backend '{}' with responseType={} responseTimeout: {}ms," +
                    " timeLimiterTimeout: {}ms and requestBody={}",
                backendName,
                responseType.simpleName,
                responseTimeout.toMillis(),
                timeLimiterTimeout.toMillis(),
                requestBody?.take(100) ?: "null",
            )
        }

        // The attempt's own clock. The error and cancel lines below promise a number regardless of any logging
        // configuration, so it is measured here and nowhere else.
        val startNanos = nanoTime()
        val elapsedMillis = { TimeUnit.NANOSECONDS.toMillis(nanoTime() - startNanos) }
        var outcome = "success"
        try {
            // The decorated call produces only the raw body string - all the CircuitBreaker, TimeLimiter and
            // Bulkhead observe. Deserialisation is deliberately kept outside the decoration (see below).
            val rawBody = resilience.decorate(backendName) { exchange(spec) }

            // Deserialisation runs AFTER the decoration: a parse failure is a client-side error, so the
            // CircuitBreaker must not see it as a failure and open the circuit while the backend is healthy.
            // Validation sits in the SAME place for the same reason - a response that violates its schema is a
            // contract problem, not an outage, and must not open the circuit either.
            responseBodySize?.record(RestClientMetrics.utf8Size(rawBody).toDouble())
            return validate(objectMapper.readValue(rawBody, responseType))
        } catch (e: InterruptedException) {
            // The caller was interrupted while waiting - its own budget expired, or its scope was cancelled.
            // The blocking counterpart of a subscriber cancelling, and reported the same way: not an error.
            outcome = "cancelled"
            doOnCancel(responseType, elapsedMillis)
            throw e
        } catch (e: Throwable) {
            // Everything else carries the resilience verdict as well as the raw HTTP outcome: a TimeLimiter
            // timeout, CircuitBreaker short-circuit, BulkheadFull, or a deserialisation failure from above all
            // surface here. (The exchange event legatium writes sees only what reached the RestClient: a breaker
            // short-circuit or a full bulkhead produces no exchange and no event - this line is where those
            // verdicts are logged.)
            outcome = "error"
            doOnError(responseType, e, elapsedMillis)
            throw e
        } finally {
            log(Level.DEBUG, "Completed request to backend '{}' with outcome={}", backendName, outcome)
        }
    }

    /**
     * The HTTP exchange proper, run by the decorator on its worker: sends the request, reads the body under the
     * buffer limit and surfaces a non-2xx as the exception Spring's own default handler would raise.
     *
     * `exchange` rather than `retrieve()` because the bounded read needs the raw stream, and `exchange` is the one
     * API that hands it over. It also hands over the status decision - `exchange` applies no status handlers -
     * which is taken here the way Spring's `DefaultResponseErrorHandler` takes it: a 4xx becomes an
     * [HttpClientErrorException], a 5xx an [HttpServerErrorException], each carrying status, headers and the
     * (truncated) error body, so a caller or a log sees the same exception types a plain `retrieve()` throws.
     * Anything else - a 2xx, and a 1xx or 3xx the transport did not resolve - is a body to return.
     *
     * The response is closed by `RestClient` when this function returns (`close = true`), which is also the
     * moment legatium emits its exchange event - on this worker thread, under the caller's MDC that the
     * decorator installed there.
     */
    private fun exchange(spec: RestClient.RequestHeadersSpec<*>): String =
        spec.exchange<String>({ _, response ->
            val status = response.statusCode
            val headers = response.headers
            if (status.isError) {
                // An error body is diagnostic only: cut it at the limit rather than reject it, so a 5xx stays a
                // 5xx for the breaker whatever size its error page has.
                val body = BoundedBodyReader.readTruncated(response.body, maxInMemoryBytes)
                throw statusException(status, response.statusText, headers, body)
            }
            val body =
                BoundedBodyReader.readOrNull(response.body, maxInMemoryBytes)
                    ?: throw ResponseBodyLimitExceededException(backendName, maxInMemoryBytes, headers.contentLength)
            String(body, charsetOf(headers))
        }, true)

    /**
     * The exception for a 4xx/5xx answer, shaped like Spring's own: the concrete subclass for the status
     * (`HttpClientErrorException.NotFound`, `HttpServerErrorException.InternalServerError`, ...) with the full
     * error body attached and a message that previews it. The preview is capped because this message ends up in
     * the WARN line's stack trace; the whole body stays on the exception for whoever asks for it.
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
            else -> HttpServerErrorException.create(message, status, statusText, headers, body, charset)
        }
    }

    /** The response's declared charset, UTF-8 when it declares none or declares nonsense. */
    private fun charsetOf(headers: HttpHeaders): Charset = runCatching { headers.contentType?.charset }.getOrNull() ?: Charsets.UTF_8

    /**
     * Validates [value] against its Bean Validation constraints, or returns it untouched when validation is
     * off for this backend ([validator] is then null - the decision is made once at construction rather than
     * per call).
     *
     * On a violation the behaviour depends on [failOnViolation]:
     * - `true` (enforce): throws [ConstraintViolationException] carrying every violation, so the caller sees
     *   which field of which type broke the contract instead of a bare "invalid". The failure is NOT an
     *   `RestClientVerdicts.httpExchangeError.isExpectedClientError`, so it keeps full severity: a backend answering outside its
     *   own published schema is an anomaly worth a WARN with its cause, not a business-as-usual outcome.
     * - `false` (report-only): logs the same detail at WARN and returns [value] unchanged, so a backend can be
     *   observed against real traffic before its violations are made fatal.
     */
    private fun <T : Any> validate(value: T): T {
        validator?.let { validator ->
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
                    "{} - reported only, not enforced " +
                        "(app.restclient.validation.fail-on-violation=false)",
                    msg,
                )
            } else {
                log(Level.DEBUG, "Response from backend '{}' has no violations", backendName)
            }
        } ?: log(Level.TRACE, "Validation of the response value for backend '{}' disabled", backendName)
        return value
    }

    private fun <T : Any> doOnError(
        responseType: Class<T>,
        error: Throwable,
        elapsedMillis: () -> Long,
    ) {
        // Severity follows the SAME classification as the circuit breaker's record predicate
        // (RestClientVerdicts.httpExchangeError.isExpectedClientError), so the two never disagree: a documented 4xx - which the
        // adapter contract declares as a normal outcome and the caller handles - is logged at DEBUG WITHOUT a
        // stack trace, everything else at WARN with one. Logging every non-2xx at ERROR made routine "not found"
        // answers indistinguishable from real outages. (legatium's exchange event classifies on its own terms: a
        // 4xx answer is `rejected` with its status there, INFO except for 401/403/408/429 - see the runbook.)
        //
        // The cause is attached ONLY to the unexpected branch: a benign 4xx must not carry a stack trace that
        // explains nothing the status and URL do not. Pinned by
        // `ResilientRestClientIntegrationTest.should log a benign 4xx at DEBUG and without a stack trace`.
        val expected = RestClientVerdicts.httpExchangeError.isExpectedClientError(error)
        val level = if (expected) Level.DEBUG else Level.WARN
        log(
            level = level,
            msg =
                "Error during request to backend '{}' with responseType={} responseTimeout: {}ms," +
                    " timeLimiterTimeout: {}ms duration so far: {}ms{}",
            cause = error.takeUnless { expected },
            backendName,
            responseType.simpleName,
            responseTimeout.toMillis(),
            timeLimiterTimeout.toMillis(),
            elapsedMillis(),
            if (expected) " - expected client error: $error" else "",
        )
        logIfBufferLimitExceeded(error)
    }

    /**
     * The one line that says how far the buffer limit was missed, for the calls the summary cannot sample.
     *
     * [responseBodySize] only ever sees bodies that FIT, so a response too large for the buffer leaves no
     * measurement at all - the bounded reader stops and no `String` is ever produced. `Content-Length`, read off
     * the response and carried on the exception, is the only figure available for it.
     *
     * **It is a LOWER bound, and the message says so.** The header is the WIRE size: with content compression
     * on the transport, a compressed body reports far less than the decompressed bytes the limit actually bounds.
     * It may also be absent entirely under chunked encoding, which is why the line still carries the limit on
     * its own - `limit=N, Content-Length=unknown` is a useful line, `Content-Length` alone is not.
     *
     * WARN and unconditional: this is a permanent, deterministic condition for the party whose response it is,
     * not a blip, and one line per occurrence is what makes it findable. The generic error line above already
     * fired for the same call, but it names neither number.
     */
    private fun logIfBufferLimitExceeded(error: Throwable) {
        if (!RestClientVerdicts.clientSideFailure.isBufferLimitFailure(error)) return
        val contentLength =
            generateSequence(error) { it.cause.takeIf { cause -> cause !== it } }
                .filterIsInstance<ResponseBodyLimitExceededException>()
                .firstOrNull()
                ?.contentLength ?: -1L
        logger.warn(
            "Response body exceeded the buffer limit on backend '{}': limit={} bytes, Content-Length={}" +
                " (wire size, a lower bound - the body may be compressed). Raise app.restclient.max-in-memory-size;" +
                " see restclient.response.body.size for the sizes that do fit.",
            backendName,
            maxInMemoryBytes,
            if (contentLength >= 0) contentLength.toString() else "unknown",
        )
    }

    /**
     * Records an abandoned call - at DEBUG, deliberately.
     *
     * A cancellation is not a fault of this client: it means the CALLER went away, because its own budget
     * expired or because the request it serves was abandoned. Both are normal outcomes, both correlate with
     * load, and under a disturbance they arrive not one at a time but all at once - so WARN, the level
     * operators alert on, filled up with the one class of event that says nothing about this module's health,
     * exactly when the lines that do say something are needed.
     *
     * The operator-facing signal lives elsewhere: the decorator cancels the worker, whose socket read fails at
     * once with `Closed by interrupt`, and legatium's exchange event then reports that exchange's end with full
     * exchange context - as a `failure`, since the blocking stack has no `cancelled` outcome
     * (`LegatiumExchangeLoggingIntegrationTest` pins the vocabulary). This line is the per-call detail
     * underneath it, on the caller's side.
     */
    private fun <T : Any> doOnCancel(
        responseType: Class<T>,
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
            responseTimeout.toMillis(),
            timeLimiterTimeout.toMillis(),
            elapsedMillis(),
        )
    }

    fun log(
        level: Level,
        msg: String,
        vararg arguments: Any?,
    ) {
        log(level, msg, null, *arguments)
    }

    /**
     * Writes one transport line with the two fields that join it to the exchange event: the backend name as
     * `adapter_name` and the URI template as `adapter_url_template` - the SAME wire names and values legatium
     * puts on the `adapter-http-exchange` event ([ADAPTER_NAME_FIELD], [URL_TEMPLATE_FIELD]). One vocabulary
     * for both loggers is what lets a dashboard group either by the same field.
     */
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

    private companion object {
        /**
         * The two wire names the transport lines share with legatium's exchange event (`ClientLogField.NAME`
         * and `.URL_TEMPLATE` there). Spelled out because that enum is internal to the library; they are a
         * contract with the log index and with the runbook, and `ResilientRestClientIntegrationTest` pins them.
         */
        const val ADAPTER_NAME_FIELD = "adapter_name"
        const val URL_TEMPLATE_FIELD = "adapter_url_template"

        /** How much of an error body the exception MESSAGE shows; the exception itself carries all of it. */
        const val ERROR_BODY_PREVIEW_CHARS = 200
    }
}
