package eu.inqudium.veredium.core.client

/**
 * A success response that carried no body although the caller asked for a [responseType] - a 204, or a 200 with
 * an empty entity.
 *
 * One contract for both client stacks. Without it they disagreed: the blocking client handed the empty string to
 * Jackson and failed with a parse error that named neither the backend nor the cause, while the reactive client
 * completed its `Mono` EMPTY - no error signal, no error line, and a `NoSuchElementException` out of the caller's
 * `awaitSingle()` that pointed nowhere near the backend. Both now fail with this exception.
 *
 * Raised AFTER the Resilience4j decoration on both stacks, like a deserialisation failure: the backend answered,
 * promptly and with a success status, so the circuit breaker records the exchange as a success and never sees
 * this. The client's error line reports it at full severity, because a backend whose contract promises a body and
 * sends none is an anomaly worth a WARN.
 */
class EmptyResponseBodyException(
    val backendName: String,
    val responseType: Class<*>,
) : RuntimeException(
        "Response from backend '$backendName' has an empty body, but a ${responseType.simpleName} was expected",
    )
