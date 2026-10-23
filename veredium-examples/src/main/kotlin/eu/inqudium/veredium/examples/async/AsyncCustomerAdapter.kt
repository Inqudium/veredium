package eu.inqudium.veredium.examples.async

import eu.inqudium.veredium.core.BaseAdapter
import eu.inqudium.veredium.examples.Backends
import eu.inqudium.veredium.examples.Customer
import eu.inqudium.veredium.examples.CustomerOverview
import eu.inqudium.veredium.examples.OrderPage
import eu.inqudium.veredium.examples.OrderSearch
import eu.inqudium.veredium.restclient.ResilientRestClientFactory
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.client.HttpClientErrorException
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.Future

/**
 * The blocking example's adapter without the wait: the same clients, called through `getAsync` and `postAsync`.
 * Every method returns at once with a [CompletableFuture]; the exchange runs on a worker of the client, and every
 * outcome - the value or the exception the blocking form would have thrown - arrives on the future.
 *
 * `cancel` is the only thing that reaches a running call: it abandons the exchange and frees its bulkhead permit.
 * A future derived with `thenApply`, `handle` or `thenCombine` does not pass a cancel on to the one it came from,
 * so each method that returns a derived future hands it on itself ([cancelling]); [orders] returns the client's own
 * future, which needs no such step.
 */
@Component
class AsyncCustomerAdapter(
    factory: ResilientRestClientFactory,
) : BaseAdapter {
    private val customers = factory.create(Backends.CUSTOMERS)
    private val orderSearch = factory.create(Backends.ORDER_SEARCH)

    /** The pessimistic ceiling over both clients, as in the blocking adapter. */
    override val timeLimiterTimeout: Duration
        get() = maxOf(customers.timeLimiterTimeout, orderSearch.timeLimiterTimeout)

    override val responseTimeout: Duration
        get() = maxOf(customers.responseTimeout, orderSearch.responseTimeout)

    /** The customer with [id]; completes with `null` when the directory answers 404, fails on every other error. */
    fun customer(id: String): CompletableFuture<Customer?> {
        val call = customers.getAsync(Customer::class.java) { it.uri("/v1/customers/{id}", id) }
        return call
            .handle { customer, error ->
                when (val cause = error?.unwrapped()) {
                    null -> customer
                    is HttpClientErrorException.NotFound -> null
                    else -> throw CompletionException(cause)
                }
            }.cancelling(call)
    }

    /** The orders of [customerId]. */
    fun orders(customerId: String): CompletableFuture<OrderPage> =
        orderSearch.postAsync(OrderPage::class.java, OrderSearch(customerId)) {
            it.uri("/v1/orders/search").contentType(MediaType.APPLICATION_JSON)
        }

    /**
     * The customer with [id] and their orders, both calls in flight at once; `null` for an unknown customer. The
     * future fails with whatever the first failing call failed with.
     */
    fun overview(id: String): CompletableFuture<CustomerOverview?> {
        val customer = customer(id)
        val orders = orders(id)
        return customer
            .thenCombine(orders) { found, page -> found?.let { CustomerOverview(it, page.orders) } }
            .cancelling(customer, orders)
    }

    private companion object {
        /** A derived stage fails with a [CompletionException] around the cause; the client's own future does not. */
        fun Throwable.unwrapped(): Throwable = if (this is CompletionException) cause ?: this else this

        /**
         * Invariant: once this future is done - completed, failed, or cancelled by the caller - none of [calls] is
         * left running. Cancelling a call that already ended is a no-op.
         */
        fun <T> CompletableFuture<T>.cancelling(vararg calls: Future<*>): CompletableFuture<T> = also { it.whenComplete { _, _ -> calls.forEach { call -> call.cancel(true) } } }
    }
}
