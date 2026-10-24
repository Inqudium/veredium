package eu.inqudium.veredium.core.resilience

import org.slf4j.spi.LoggingEventBuilder

/**
 * The one decision "is this exchange failure the BACKEND's fault, or a benign client-side outcome?" - the
 * benign-4xx verdict of [BreakerVerdicts], owned by neither client STACK, so the two stacks cannot drift apart on it.
 *
 * Two consumers per stack derive from it and therefore cannot disagree:
 *  - the synchronous error log's severity ([eu.inqudium.veredium.core.client.ClientCallLog.onError]);
 *  - the circuit breaker's record predicate, via the per-alias customizer ([circuitBreakerCustomizer]).
 *
 * The exchange event itself (legatium's `adapter-http-exchange` logger) is NOT a consumer. Assumption: legatium
 * classifies its exchange event itself (legatium ADR-0012), and this policy does not reach into it. The
 * 404-means-business-case reading lives in the two consumers above and in an operator's reading of
 * `adapter_response_status_code`.
 *
 * What is stack-neutral is the RULE: which statuses are benign, how the cause chain is walked, what the policy
 * line says. What is NOT is the SHAPE of the exceptions the rule is applied to - the blocking client throws
 * Spring's `RestClientResponseException` for a 4xx answer, the reactive one a `WebClientResponseException`; the
 * blocking client's buffer limit is its own exception, the reactive one's is Spring's codec exception. The rule
 * therefore lives here as a class that is handed the stack's reading of a throwable, and each stack holds its
 * one instance in its [BreakerVerdicts].
 *
 * Rationale: a neutral policy object, owned by neither the logging nor the resilience layer, rather than a member
 * of either consumer - a resilience/transport policy hidden in a logging class is the wrong owner and the wrong
 * dependency direction. The consumers reference a policy, not each other.
 *
 * @property responseStatus The HTTP status [Throwable] carries when it is a response exception of this stack -
 *   the status-carrying root type and its subclasses - or `null` for every other throwable. The ONE thing the
 *   stack has to say about the benign-4xx rule.
 */
class HttpExchangeErrorPolicy(
    private val responseStatus: (Throwable) -> Int?,
) {
    /**
     * True for a failure the adapter contract documents as a NORMAL outcome: a benign 4xx the caller is
     * expected to handle, e.g. the 404 that says "this involved party does not exist". Such an exchange did not
     * succeed, but it is not an operational fault - logging it at ERROR would fire alerting on a business case
     * and bury the real failures underneath it.
     *
     * 408 and 429 are deliberately EXCLUDED (see [OPERATIONAL_CLIENT_STATUSES]): a request timeout and a
     * rate-limit rejection are operational signals, not business cases. They keep full severity AND are
     * recorded by the circuit breaker.
     *
     * Everything else is NOT expected and keeps full severity too: 5xx, a TimeLimiter timeout, a CircuitBreaker
     * rejection, a BulkheadFull, a transport error, or a deserialisation failure.
     */
    fun isExpectedClientError(error: Throwable?): Boolean {
        val status = error?.let(responseStatus) ?: return false
        return status in CLIENT_ERROR_RANGE && status !in OPERATIONAL_CLIENT_STATUSES
    }

    /**
     * The startup line naming the backends this policy was applied to - the only evidence an operator has that
     * the verdict is in force, and for WHICH aliases.
     *
     * The message carries ONE placeholder and therefore exactly one argument: a second one placed before [name]
     * would fill the slot instead, and the line would name neither the right backends nor the right policy.
     * Assumption: SLF4J neither fails nor warns on a placeholder/argument mismatch.
     */
    fun logPolicy(
        logBuilder: LoggingEventBuilder,
        name: String,
    ) {
        logBuilder
            .setMessage(
                "CircuitBreaker now counts benign 4xx (every client error except 408/429) as a success for backend(s): " +
                    "{} - a business-case rejection cannot open a breaker against a healthy backend",
            ).addArgument(name)
            .log()
    }

    companion object {
        private val CLIENT_ERROR_RANGE = 400..499

        /**
         * The two 4xx statuses that are operational signals rather than business cases: 408 Request Timeout and
         * 429 Too Many Requests. A 408 is a server-side timeout, a 429 is backpressure; a run of either is worth
         * alerting on and worth opening the circuit for. They are therefore held OUT of [isExpectedClientError],
         * which keeps them at full log severity and lets the circuit breaker record them.
         */
        private val OPERATIONAL_CLIENT_STATUSES = setOf(408, 429)
    }
}

/**
 * The failures the backend is NOT at fault for, and which the CircuitBreaker must therefore not count.
 *
 * Membership has one criterion: **the backend is not at fault** - either it was never called, or it answered
 * and this process failed to handle the answer. The list is the stack's: the pipeline's own load shedding
 * (`BulkheadFullException`) is on it for both stacks, the buffer-limit exception is each stack's own type, and
 * the blocking stack adds the two faults its thread hand-off can raise (an interrupted caller, a shut-down
 * worker executor) that a reactive subscriber's cancellation never surfaces as an error at all.
 *
 * @property clientSideFailures The exception types to discount. Matched against the thrown exception AND its
 *   causes - see [isClientSideFailure].
 * @property bufferLimitFailure The one type on the list that is a buffer overrun: the response exceeded the
 *   stack's `maxInMemorySize`. Asked for separately by [isBufferLimitFailure], because it is the one failure
 *   whose report has to name a configuration value.
 */
class ClientSideFailurePolicy(
    val clientSideFailures: List<Class<out Throwable>>,
    private val bufferLimitFailure: Class<out Throwable>,
) {
    init {
        require(bufferLimitFailure in clientSideFailures) {
            "the buffer-limit failure ${bufferLimitFailure.simpleName} must be one of the client-side failures"
        }
    }

    /**
     * Whether [error] is - or was CAUSED BY - a failure this client raised itself.
     *
     * The cause chain is walked on purpose, not defensively. Assumption: Spring's blocking client (spring-web
     * 7.0.9) wraps every `IOException` of an exchange into a `ResourceAccessException`, and its reactive client wraps
     * a codec failure on an otherwise successful response into a `WebClientResponseException` reporting **200 OK**
     * with the real cause underneath. Matching on the thrown type alone - which is all `ignoreExceptions(Class...)`
     * does - would never see either, and the entry would be a silent no-op.
     *
     * Walking the chain is safe here rather than merely convenient: a genuine backend failure does NOT carry
     * one of these as a cause. A 5xx whose error body exceeds the buffer limit yields a plain status exception
     * with no limit exception underneath - on both stacks, each pinned by a test of the stack's verdicts - so it is
     * still recorded.
     */
    fun isClientSideFailure(error: Throwable): Boolean = causeChain(error).any { candidate -> clientSideFailures.any { it.isInstance(candidate) } }

    /**
     * Whether [error] is - or was caused by - specifically the [bufferLimitFailure].
     *
     * The narrow sibling of [isClientSideFailure], which folds this together with `BulkheadFullException`
     * because the BREAKER treats both the same way. The two differ in every other respect: a full bulkhead is
     * transient and says nothing about the response, while a buffer overrun is permanent for the party whose
     * response it is and is fixed by one configuration value. Only the second one warrants naming that value,
     * so only the second one is asked for here.
     */
    fun isBufferLimitFailure(error: Throwable): Boolean = causeChain(error).any(bufferLimitFailure::isInstance)

    /** The startup line naming the backends this policy was applied to, with the exception types it ignores; one argument per placeholder. */
    fun logPolicy(
        logBuilder: LoggingEventBuilder,
        name: String,
    ) {
        logBuilder
            .setMessage(
                "CircuitBreaker now ignores {} for backend(s): {} - " +
                    "a fault this client causes itself cannot open a breaker",
            ).addArgument(clientSideFailures.joinToString(", ") { it.simpleName })
            .addArgument(name)
            .log()
    }

    companion object {
        /** Bound on the cause walk, so a pathological cause chain cannot spin. */
        const val MAX_CAUSE_DEPTH: Int = 10

        /**
         * [error] and its causes, outermost first, at most [MAX_CAUSE_DEPTH] of them. The self-cause guard only
         * catches a throwable that is its OWN cause; a longer cycle is stopped by the depth bound alone. The one
         * cause walk of the module, offered to the client stacks so that a stack reading its own exception off a
         * chain walks it with the same bound.
         */
        fun causeChain(error: Throwable): Sequence<Throwable> =
            generateSequence(error) { it.cause.takeIf { cause -> cause !== it } }
                .take(MAX_CAUSE_DEPTH)
    }
}

/**
 * What a client stack tells the shared resilience wiring about itself: its two breaker verdicts and a probe
 * for the startup guard. One object per stack, handed to the shared wiring directly - to [MeshResilienceRegistrar]
 * by the stack's subclass of it, and to [eu.inqudium.veredium.core.client.ClientCallLog] by the stack's client -
 * and never declared as a bean, so nothing in a host's context can replace it.
 *
 * ## Outcomes and verdicts
 *
 * The two terms are kept apart throughout the module. A circuit breaker sorts every error into one of three
 * **outcomes**: *ignored* (no evidence about the backend in either direction), *success* (the backend answered
 * correctly) or *failure* (counted against the backend). A **verdict** is one of the two policies this object
 * carries, which decide those outcomes: [clientSideFailure] decides *ignored*, [httpExchangeError] decides
 * *success* for a benign 4xx, and whatever neither claims is a *failure*. [circuitBreakerCustomizer] installs
 * both verdicts and states the resulting table.
 *
 * @property httpExchangeError The benign-4xx rule, with this stack's reading of a response exception.
 * @property clientSideFailure The client-side-failure rule, with this stack's exception types.
 * @property benignClientErrorProbe A 404 in the shape this stack throws it. [MeshResilienceStartupGuard] applies
 *   each breaker's record predicate to it, so the check is made with the exact type the predicate will see at
 *   runtime rather than with a stand-in that the stack's [HttpExchangeErrorPolicy.isExpectedClientError] might
 *   not even recognise.
 */
class BreakerVerdicts(
    val httpExchangeError: HttpExchangeErrorPolicy,
    val clientSideFailure: ClientSideFailurePolicy,
    val benignClientErrorProbe: Throwable,
) {
    init {
        require(httpExchangeError.isExpectedClientError(benignClientErrorProbe)) {
            "the benign-client-error probe ${benignClientErrorProbe::class.simpleName} is not read as an expected client error " +
                "by the stack's own policy - the startup guard would then fail every start"
        }
    }
}
