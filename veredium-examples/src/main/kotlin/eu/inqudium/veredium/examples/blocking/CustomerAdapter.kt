package eu.inqudium.veredium.examples.blocking

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

/**
 * An adapter over two backends with the blocking client: one [eu.inqudium.veredium.restclient.ResilientRestClient]
 * per mesh alias, created once from the factory. A call only describes the request - the URI, a body, a content
 * type; the client runs it through the bulkhead, the time limiter and the circuit breaker of its alias and
 * deserialises the answer.
 *
 * [customer] and [orders] block the calling thread. [overview] shows the asynchronous form: both calls start at
 * once and the caller gets one future for the combined result.
 */
@Component
class CustomerAdapter(
    factory: ResilientRestClientFactory,
) : BaseAdapter {
    private val customers = factory.create(Backends.CUSTOMERS)
    private val orderSearch = factory.create(Backends.ORDER_SEARCH)

    /** The pessimistic ceiling over both clients: what one call of this adapter may take at most. */
    override val timeLimiterTimeout: Duration
        get() = maxOf(customers.timeLimiterTimeout, orderSearch.timeLimiterTimeout)

    override val responseTimeout: Duration
        get() = maxOf(customers.responseTimeout, orderSearch.responseTimeout)

    /**
     * The customer with [id], or `null` when the directory does not know it. A 404 is Spring's
     * [HttpClientErrorException.NotFound]; every other failure - another status, a timeout, an open breaker - is
     * left to the caller.
     */
    fun customer(id: String): Customer? =
        try {
            customers.get(Customer::class.java) { it.uri("/v1/customers/{id}", id) }
        } catch (_: HttpClientErrorException.NotFound) {
            null
        }

    /** The orders of [customerId]. The body is serialised to JSON before the exchange starts. */
    fun orders(customerId: String): OrderPage =
        orderSearch.post(OrderPage::class.java, OrderSearch(customerId)) {
            it.uri("/v1/orders/search").contentType(MediaType.APPLICATION_JSON)
        }

    /**
     * The customer with [id] and their orders, both calls in flight at once. The future fails with whatever the
     * first failing call failed with; cancelling it abandons both exchanges.
     */
    fun overview(id: String): CompletableFuture<CustomerOverview> {
        val customer = customers.getAsync(Customer::class.java) { it.uri("/v1/customers/{id}", id) }
        val orders =
            orderSearch.postAsync(OrderPage::class.java, OrderSearch(id)) {
                it.uri("/v1/orders/search").contentType(MediaType.APPLICATION_JSON)
            }
        val overview = customer.thenCombine(orders) { found, page -> CustomerOverview(found, page.orders) }
        // Invariant: once the combined future is done - completed, failed or cancelled by the caller - no call
        // it was waiting for is left running. Cancelling a call that already ended is a no-op.
        overview.whenComplete { _, _ ->
            customer.cancel(true)
            orders.cancel(true)
        }
        return overview
    }
}
