package eu.inqudium.veredium.examples.decorator

import eu.inqudium.veredium.core.CallerMdc
import eu.inqudium.veredium.examples.Customer
import org.slf4j.MDC
import org.springframework.stereotype.Component
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors

/**
 * The caller's side of the MDC hand-off: what a host does so that the lines logged on the decorator's worker carry its
 * correlation keys - and the one case where it has to carry them itself.
 *
 * - **One hop: nothing to do.** Put the keys in the MDC of the calling thread before calling the adapter, here with
 *   `MDC.putCloseable`; in a real host a servlet filter or a coroutine's `MDCContext` has usually done that. The
 *   decorator copies the MDC when the call is decorated and installs the copy on its worker, so the adapter's
 *   "Looking up customer" line carries [REQUEST_ID] ([customer]).
 * - **The copy is taken at the call.** [customerAsync] removes the key as soon as
 *   [DecoratorCustomerAdapter.customerAsync] has returned, while the lookup may still be running - the worker keeps
 *   the copy it got. Keys put after the call do not reach it, and keys the call puts on the worker do not come back.
 * - **A second hop: carry it yourself.** The decorator copies the MDC of the thread that calls it. When that thread is
 *   one of the host's own - a fan-out executor, as in [customers] - its MDC is empty unless the host hands the
 *   caller's copy on. [CallerMdc.with] (`veredium-core`) does that, and restores the thread's own MDC afterwards.
 */
@Component
class CorrelatedCustomerLookup(
    private val adapter: DecoratorCustomerAdapter,
) {
    /** The customer with [id], every line of the lookup tagged with [requestId]. */
    fun customer(
        requestId: String,
        id: String,
    ): Customer? = MDC.putCloseable(REQUEST_ID, requestId).use { adapter.customer(id) }

    /** [customer] without the wait: the key leaves the caller's MDC at once, the worker keeps its copy. */
    fun customerAsync(
        requestId: String,
        id: String,
    ): CompletableFuture<Customer> = MDC.putCloseable(REQUEST_ID, requestId).use { adapter.customerAsync(id) }

    /**
     * The customers with [ids], looked up in parallel, `null` for an unknown one, in the order of [ids]. Each lookup
     * calls the adapter from a fan-out thread, so the caller's MDC is copied by hand onto that thread first - without
     * [CallerMdc.with] the decorator would copy the fan-out thread's empty MDC, and the lines would lose [requestId].
     */
    fun customers(
        requestId: String,
        ids: List<String>,
    ): List<Customer?> =
        MDC.putCloseable(REQUEST_ID, requestId).use {
            val callerMdc: Map<String, String>? = MDC.getCopyOfContextMap()
            Executors.newVirtualThreadPerTaskExecutor().use { fanOut ->
                ids
                    .map { id -> fanOut.submit(Callable { CallerMdc.with(callerMdc) { adapter.customer(id) } }) }
                    .map { lookup ->
                        try {
                            lookup.get()
                        } catch (e: ExecutionException) {
                            throw e.cause ?: e
                        }
                    }
            }
        }

    companion object {
        /** The MDC key this example correlates by. */
        const val REQUEST_ID = "requestId"
    }
}
