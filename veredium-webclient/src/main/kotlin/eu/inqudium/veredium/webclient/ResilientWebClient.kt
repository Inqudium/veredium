package eu.inqudium.veredium.webclient

import eu.inqudium.legatium.webclient.logging.ClientRequestLoggingFilter
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import eu.inqudium.veredium.core.validation.ResponseValidationProperties
import io.github.resilience4j.reactor.timelimiter.TimeLimiterOperator
import io.micrometer.core.instrument.DistributionSummary
import jakarta.validation.ConstraintViolationException
import jakarta.validation.Validator
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * The two effective per-call timeouts an adapter exposes to its callers, so upstream orchestration can
 * compose an end-to-end budget from the individual backend budgets (e.g. a resolver summing the timeouts
 * along its call chain) without reaching into the adapter's [ResilientWebClient]s.
 *
 * If an adapter fronts more than one mesh endpoint (one [ResilientWebClient] per operation), than an
 * implementation can aggregate the per-client values - typically the `maxOf` across its clients, the
 * pessimistic ceiling for a single logical call.
 *
 * The two values are layered, not alternatives: the TimeLimiter is the outer, hard deadline that cancels
 * the whole reactive chain, while the WebClient response timeout is a slightly looser transport-level
 * backstop underneath it (see the ordering in [ResilientWebClient]). Keeping the response timeout above
 * the TimeLimiter budget ensures the TimeLimiter is the one that trips first, yielding a clean timeout
 * rather than a raw transport error.
 */
interface BaseAdapter {
    /**
     * The Resilience4j TimeLimiter budget for the call - the outer, hard deadline enforced by the
     * [TimeLimiterOperator], after which the whole reactive chain is cancelled.
     *
     * ## What an adapter call can fail with, beyond the documented non-2xx
     *
     * A 200 whose body does not fit the declared DTO fails the call rather than yielding a partially populated
     * object. A property declared required and non-nullable that is missing is rejected by Jackson; one that is
     * explicitly `null` is rejected by the null check the Kotlin compiler puts in the constructor. Both surface
     * as a `tools.jackson.databind.DatabindException` naming the property, and both are pinned by
     * `ResponseDeserialisationContractTest`. This matters to a caller because Kotlin's types promise the field
     * cannot be null - nothing downstream guards against it, so a silent null would only fail much later.
     *
     * Such a failure is CLIENT-side: the backend answered, and with a 200. Like a parse error it is therefore
     * kept out of the Resilience4j decoration and does not count towards the circuit breaker (see
     * [ReactiveResilienceDecorator]). It is logged with full severity; the exchange line legatium writes for
     * the call shows the 200 the backend answered with, because the exchange itself succeeded.
     */
    val timeLimiterTimeout: Duration

    /**
     * The Reactor Netty response-receive timeout - the transport-level bound on waiting for the HTTP
     * response, sitting just below [timeLimiterTimeout] as a backstop. The ladder's `responseTimeout` rung
     * (`veredium-core`), the same number the blocking twin applies as Apache's response timeout.
     */
    val responseTimeout: Duration
}

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
 * `adapter_url_template` under the library's own field names ([log]), so it joins the exchange event on sight.
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
     * exceeding it ([logIfBufferLimitExceeded]), so that line carries the number an operator has to raise.
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
     * Recorded in UTF-8 BYTES ([WebClientMetrics.utf8Size]) rather than `String.length`, so the sample and the
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
                // Guarded AND parameterised: this runs per subscription, i.e. on every outbound call. A Kotlin
                // string template is built before the call regardless of the level, and two of the arguments are
                // not free - timeLimiterTimeout resolves through the R4j registry, and take(100) allocates - so
                // the guard is what keeps them off the hot path, not just the formatting.
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
                    .map { rawBody ->
                        responseBodySize?.record(WebClientMetrics.utf8Size(rawBody).toDouble())
                        validate(objectMapper.readValue(rawBody, responseType))
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
                        doOnCancel(responseType, elapsedMillis)
                    }.doOnError { error ->
                        doOnError(responseType, error, elapsedMillis)
                    }.doFinally { signalType ->
                        log(Level.DEBUG, "On signalType={} for backend '{}'", signalType, backendName)
                    }
            }

        return exchange.contextCapture()
    }

    /**
     * Validates [value] against its Bean Validation constraints, or returns it untouched when validation is
     * off for this backend ([validator] is then null - the decision is made once at construction rather than
     * per call).
     *
     * On a violation the behaviour depends on [failOnViolation]:
     * - `true` (enforce): throws [ConstraintViolationException] carrying every violation, so the caller sees
     *   which field of which type broke the contract instead of a bare "invalid". The failure is NOT an
     *   `WebClientVerdicts.httpExchangeError.isExpectedClientError`, so it keeps full severity: a backend answering outside its
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
                val event =
                    log(
                        Level.WARN,
                        "{} - reported only, not enforced " +
                            "(app.webclient.validation.fail-on-violation=false)",
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
        // Severity follows the SAME classification as the circuit breaker's ignore predicate
        // (WebClientVerdicts.httpExchangeError.isExpectedClientError), so the two never disagree: a documented 4xx - which the
        // adapter contract declares as a normal outcome and the caller handles - is logged at DEBUG WITHOUT a
        // stack trace, everything else at WARN with one. Logging every non-2xx at ERROR made routine "not found"
        // answers indistinguishable from real outages. (legatium's exchange event classifies on its own terms: a
        // 4xx answer is `rejected` with its status there, INFO except for 401/403/408/429 - see the runbook.)
        //
        // The cause is therefore attached ONLY to the unexpected branch. That condition lived in the call itself
        // (`if (expected) logger.atDebug() else logger.atWarn().setCause(error)`) and was lost when the two
        // branches were folded into the shared `log(level, msg, cause, ...)` helper, which took the cause
        // unconditionally - so every benign 4xx carried a reactive stack trace again, the second half of exactly
        // the noise this classification removes. Pinned by
        // `ResilientWebClientIntegrationTest.should log a benign 4xx at DEBUG and without a stack trace`.
        val expected = WebClientVerdicts.httpExchangeError.isExpectedClientError(error)
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
     * The one line that says how far the codec limit was missed, for the calls the summary cannot sample.
     *
     * [responseBodySize] only ever sees bodies that FIT, so a response too large for the buffer leaves no
     * measurement at all - the codec aborts and no `String` is ever produced. `Content-Length`, read off the
     * response the exception carries, is the only figure available for it.
     *
     * **It is a LOWER bound, and the message says so.** The header is the WIRE size: with `compress(true)` on
     * the transport, a compressed body reports far less than the decompressed bytes the limit actually bounds.
     * It may also be absent entirely under chunked encoding, which is why the line still carries the limit on
     * its own - `limit=N, Content-Length=unknown` is a useful line, `Content-Length` alone is not.
     *
     * WARN and unconditional: this is a permanent, deterministic condition for the party whose response it is,
     * not a blip, and one line per occurrence is what makes it findable. The generic error line above already
     * fired for the same call, but it names neither number.
     */
    private fun logIfBufferLimitExceeded(error: Throwable) {
        if (!WebClientVerdicts.clientSideFailure.isBufferLimitFailure(error)) return
        val contentLength = (error as? WebClientResponseException)?.headers?.contentLength ?: -1L
        logger.warn(
            "Response body exceeded the codec buffer on backend '{}': limit={} bytes, Content-Length={}" +
                " (wire size, a lower bound - the body may be compressed). Raise app.webclient.max-in-memory-size;" +
                " see webclient.response.body.size for the sizes that do fit.",
            backendName,
            maxInMemoryBytes,
            if (contentLength >= 0) contentLength.toString() else "unknown",
        )
    }

    /**
     * Marks the exchange as torn down and records it - at DEBUG, deliberately.
     *
     * A cancellation is not a fault of this client: it means the SUBSCRIBER went away, because its own budget
     * expired or because the client it serves disconnected. Both are normal outcomes, both correlate with load,
     * and under a disturbance they arrive not one at a time but all at once - so WARN, the level operators
     * alert on, filled up with the one class of event that says nothing about this module's health, exactly
     * when the lines that do say something are needed.
     *
     * The operator-facing signal is not lost, it lives elsewhere: legatium's exchange event reports the same
     * cancellation at WARN with `adapter_outcome=cancelled` and the full exchange context - for a cancel that
     * reached the WebClient, which is every cancel that had a request on the wire. This line is the per-call
     * detail underneath it. Note the dependency that creates - the `adapter-http-exchange` logger must run at
     * WARN or below for the signal to exist, which `application.yml` and `LoggingLevelContractTest` guarantee.
     *
     * The fluent builder keeps the arguments lazy: `timeLimiterTimeout` resolves through the Resilience4j
     * registry, so it is not free, and at DEBUG-off nothing is evaluated at all.
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
            elapsedMillis,
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
     * for both loggers is what lets a dashboard group either by the same field. (These lines carried
     * `adapter_target_name` until the module's own exchange diary was replaced; the data stream still maps
     * that name, nothing writes it any more.)
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
        cause.let { event.setCause(cause) }
        event.log()
    }

    private companion object {
        /**
         * The two wire names the transport lines share with legatium's exchange event (`ClientLogField.NAME`
         * and `.URL_TEMPLATE` there). Spelled out because that enum is internal to the library; they are a
         * contract with the log index and with the runbook, and `ResilientWebClientIntegrationTest` pins them.
         */
        const val ADAPTER_NAME_FIELD = "adapter_name"
        const val URL_TEMPLATE_FIELD = "adapter_url_template"
    }
}
